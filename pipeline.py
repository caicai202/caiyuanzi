#!/usr/bin/env python3
"""
火山引擎 ARK → Seedream 定妆照 → Seedance 1.5 Pro 口播成片
==========================================================
阶段1: Seedream 5.0 → 定妆照
阶段2: Seedance (1.5 Pro/2.0) ×N段 (尾帧接力, 4~12或15s/段)
阶段3: ffmpeg concat → 最终成片

用法:
  python3 pipeline.py --script 口播剧本.txt --character "30岁知性女主播..."
  python3 pipeline.py --interactive  # 交互式输入

  # 使用 Seedance 2.0 (需先在控制台开通)
  python3 pipeline.py --model 2.0 --script 剧本.txt --character "..."

环境变量:
  ARK_API_KEY  火山引擎 API Key (默认使用内置 key)
"""

import os
import sys
import json
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

from ark_client import (
    api_session,
    SEEDREAM_MODEL, SEEDANCE_15_PRO, SEEDANCE_2,
    OUTPUT_DIR,
    generate_portrait, create_video_task, wait_for_task,
    download_with_retry,
)

# 默认配置
SEEDANCE_MODEL = SEEDANCE_15_PRO  # 默认 1.5 Pro

SESSION_ID = datetime.now().strftime("%Y%m%d_%H%M%S")


# ============================================================
# 阶段1: Seedream 5.0 生成定妆照（封装 ark_client）
# ============================================================
def generate_portrait_local(prompt: str, size: str = "1080x1920", n: int = 1):
    """调用 ark_client 并保存到带 session_id 的路径。"""
    print(f"\n{'='*60}")
    print("📸 阶段1: Seedream 5.0 生成定妆照")
    print(f"   Prompt: {prompt[:80]}...")
    print(f"   Size: {size}")

    urls, _ = generate_portrait(prompt, size=size, n=n)

    saved = []
    for i, url in enumerate(urls):
        img_data = api_session.get(url, timeout=30).content
        path = OUTPUT_DIR / f"{SESSION_ID}_portrait_{i}.png"
        path.write_bytes(img_data)
        saved.append(str(path))
        print(f"   ✅ 定妆照 {i+1}: {path} ({len(img_data)//1024}KB)")

    return urls, saved


# ============================================================
# 阶段2: Seedance 分段生成视频 (尾帧接力)
# ============================================================
# 使用 ark_client.create_video_task / query_task / wait_for_task


def generate_segments(
    portrait_url: str,
    segments: list[dict],
    duration: int = 15,
    resolution: str = "720p",
    ratio: str = "9:16",
    generate_audio: bool = True,
) -> list[dict]:
    """
    分段生成视频，尾帧接力。

    segments: [{"prompt": "...", "duration": 15}, ...]
    generate_audio: False 时 Seedance 生成无声视频 (配合外部 TTS)
    返回: [{"video_path": ..., "video_url": ..., "last_frame_path": ...}, ...]
    """
    audio_mode = "有声" if generate_audio else "无声(外部TTS)"
    print(f"\n{'='*60}")
    print(f"🎬 阶段2: Seedance 2.0 分段生成 ({len(segments)}段, {audio_mode})")
    print(f"   总目标时长: {len(segments) * duration}s ≈ {len(segments) * duration / 60:.1f}分钟")

    results = []
    ref_image_url = portrait_url

    for idx, seg in enumerate(segments):
        seg_num = idx + 1
        seg_duration = seg.get("duration", duration)
        seg_prompt = seg["prompt"]

        print(f"\n   --- 段 {seg_num}/{len(segments)} ({seg_duration}s) ---")
        print(f"   参考图: {ref_image_url[:60]}...")
        print(f"   台词: {seg_prompt[:100]}...")

        # 构建 content
        content = [
            {
                "type": "text",
                "text": seg_prompt,
            },
            {
                "type": "image_url",
                "image_url": {"url": ref_image_url},
                "role": "first_frame",
            },
        ]

        # 创建任务
        task = create_video_task(
            model=SEEDANCE_MODEL,
            content=content,
            duration=seg_duration,
            resolution=resolution,
            ratio=ratio,
            generate_audio=generate_audio,
            return_last_frame=True,
        )

        task_id = task.get("id", "")
        if not task_id:
            raise RuntimeError(f"创建任务未返回 ID: {task}")

        # 等待完成
        result = wait_for_task(task_id)
        video_url = result.get("content", {}).get("video_url", "")
        last_frame_url = result.get("content", {}).get("last_frame_url", "")

        if not video_url:
            raise RuntimeError(f"任务成功但未返回 video_url: {json.dumps(result, ensure_ascii=False)[:500]}")

        print(f"   ✅ 视频 URL: {video_url[:80]}...")
        if last_frame_url:
            print(f"   ✅ 尾帧 URL: {last_frame_url[:80]}...")

        # 下载视频
        video_path = str(OUTPUT_DIR / f"{SESSION_ID}_seg{seg_num:02d}.mp4")
        video_data = download_with_retry(video_url, f"seg{seg_num}")
        Path(video_path).write_bytes(video_data)
        print(f"   已保存: {video_path} ({len(video_data) // 1024}KB)")

        # 下载尾帧
        last_frame_path = None
        if last_frame_url:
            last_frame_path = str(OUTPUT_DIR / f"{SESSION_ID}_lastframe{seg_num:02d}.png")
            last_frame_data = download_with_retry(last_frame_url, f"lastframe_seg{seg_num}")
            Path(last_frame_path).write_bytes(last_frame_data)

        results.append({
            "seg_num": seg_num,
            "video_path": video_path,
            "video_url": video_url,
            "last_frame_url": last_frame_url,
            "last_frame_path": last_frame_path,
        })

        # 下一段用尾帧做参考图
        if last_frame_url:
            ref_image_url = last_frame_url
        else:
            print("   ⚠️ 未返回尾帧，下一段继续用上一段参考图(可能穿帮)")

    return results


# ============================================================
# 阶段3: ffmpeg 拼接
# ============================================================
def concat_videos(video_paths: list[str], output_path: str = None) -> str:
    """
    使用 ffmpeg concat demuxer 拼接视频。
    无损拼接，不需要重新编码。
    """
    if output_path is None:
        output_path = str(OUTPUT_DIR / f"{SESSION_ID}_final.mp4")

    print(f"\n{'='*60}")
    print(f"🔗 阶段3: ffmpeg 拼接 ({len(video_paths)}段)")

    # 写 concat 文件列表
    concat_list_path = str(OUTPUT_DIR / f"{SESSION_ID}_concat.txt")
    with open(concat_list_path, "w") as f:
        for p in video_paths:
            f.write(f"file '{p}'\n")

    # ffmpeg concat
    cmd = [
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", concat_list_path,
        "-c", "copy",
        output_path,
    ]

    print(f"   执行: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"   ❌ 拼接失败:\n{result.stderr[:1000]}")
        # 尝试重编码拼接 (fallback)
        print("   尝试重编码拼接...")
        filter_parts = []
        for i, p in enumerate(video_paths):
            filter_parts.append(f"[{i}:v][{i}:a]")
        filter_str = " ".join(filter_parts) + f" concat=n={len(video_paths)}:v=1:a=1 [v][a]"

        cmd2 = ["ffmpeg", "-y"]
        for p in video_paths:
            cmd2.extend(["-i", p])
        cmd2.extend([
            "-filter_complex", filter_str,
            "-map", "[v]", "-map", "[a]",
            "-c:v", "libx264", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            output_path,
        ])
        result2 = subprocess.run(cmd2, capture_output=True, text=True)
        if result2.returncode != 0:
            raise RuntimeError(f"重编码拼接也失败:\n{result2.stderr[:1000]}")

    size_mb = Path(output_path).stat().st_size / (1024 * 1024)
    print(f"   ✅ 成片: {output_path} ({size_mb:.1f}MB)")

    # 清理 concat 列表
    os.remove(concat_list_path)

    return output_path


# ============================================================
# 工具: 自动分割口播剧本
# ============================================================
def split_script(script_text: str, segment_duration: int = 15, speech_rate: float = 4.0) -> list[dict]:
    """
    将口播剧本按语速拆分为多段。

    speech_rate: 每秒中文字数 (默认 4 字/秒，中等语速)
    """
    chars_per_seg = int(segment_duration * speech_rate)

    # 按句号、问号、感叹号、换行分段
    sentences = []
    current = ""
    for char in script_text:
        current += char
        if char in "。！？\n" and len(current) > 10:
            sentences.append(current.strip())
            current = ""
    if current.strip():
        sentences.append(current.strip())

    segments = []
    current_seg = ""
    for sent in sentences:
        if len(current_seg) + len(sent) > chars_per_seg and current_seg:
            segments.append(current_seg.strip())
            current_seg = sent
        else:
            current_seg += sent if not current_seg else sent

    if current_seg.strip():
        segments.append(current_seg.strip())

    # 添加风格提示词模板
    style_template = (
        "特写镜头，正面拍摄。人物保持自然微笑，眼神看向镜头。"
        "柔和摄影棚灯光，纯色背景。"
        "口播文案："
    )

    return [{"prompt": f"{style_template}{seg}", "duration": segment_duration} for seg in segments]


# ============================================================
# 主流程
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="火山引擎 ARK → Seedream 定妆照 → Seedance 2.0 口播成片",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 从剧本文件生成
  python3 pipeline.py \\
    --character "30岁知性女主播，黑色长发，淡妆，白色衬衫，温暖微笑" \\
    --script 口播剧本.txt \\
    --duration 15 \\
    --resolution 720p

  # 交互式输入
  python3 pipeline.py --interactive

  # 只需拼接已有片段
  python3 pipeline.py --concat-only seg01.mp4 seg02.mp4 seg03.mp4
        """,
    )

    parser.add_argument("--character", "-c", help="人物形象描述 (用于 Seedream 定妆照)")
    parser.add_argument("--script", "-s", help="口播剧本文件路径")
    parser.add_argument("--script-text", help="直接输入口播剧本文本")
    parser.add_argument("--duration", "-d", type=int, default=12, help="每段视频时长(秒), 1.5Pro默认12, 2.0默认15")
    parser.add_argument("--model", "-m", default="1.5pro", choices=["1.5pro", "2.0"], help="Seedance 模型版本, 默认1.5pro")
    parser.add_argument("--resolution", "-r", default="720p", choices=["480p", "720p", "1080p"])
    parser.add_argument("--ratio", default="9:16", choices=["16:9", "9:16", "1:1", "4:3", "3:4", "21:9"])
    parser.add_argument("--portrait-size", default="1440x2560", help="定妆照尺寸 (最小 3686400px), 默认 1440x2560")
    parser.add_argument("--speech-rate", type=float, default=4.0, help="语速(字/秒), 默认4")
    parser.add_argument("--skip-portrait", action="store_true", help="跳过定妆照生成, 使用已有图片")
    parser.add_argument("--portrait-url", help="已有定妆照 URL (配合 --skip-portrait)")
    parser.add_argument("--concat-only", nargs="*", help="仅拼接已有视频文件")
    parser.add_argument("--interactive", "-i", action="store_true", help="交互式输入")
    parser.add_argument("--external-tts", action="store_true", help="使用外部 TTS (edge-tts) 统一音色，Seedance 无声生成")
    parser.add_argument("--tts-voice", default="zh-CN-YunxiNeural",
                        help="TTS 语音 (默认 zh-CN-YunxiNeural)，可用: YunxiNeural/YunjianNeural/YunyangNeural")

    args = parser.parse_args()

    # 仅拼接模式
    if args.concat_only is not None:
        if len(args.concat_only) == 0:
            # 自动找 output 目录下的 seg*.mp4
            segs = sorted(OUTPUT_DIR.glob("*_seg*.mp4"))
            args.concat_only = [str(s) for s in segs]
            print(f"自动找到 {len(segs)} 个片段")
        if len(args.concat_only) < 2:
            print("❌ 至少需要 2 个视频文件才能拼接")
            sys.exit(1)
        concat_videos(args.concat_only)
        return

    # 交互式模式
    if args.interactive:
        print("🎬 火山引擎 ARK → 口播成片 完整管道\n")
        args.character = input("人物形象描述 (Seedream prompt): ").strip()
        print("\n口播剧本 (输入 END 结束):")
        lines = []
        while True:
            line = input()
            if line.strip().upper() == "END":
                break
            lines.append(line)
        args.script_text = "\n".join(lines)
        args.duration = int(input("\n每段时长(秒) [15]: ") or "15")
        args.resolution = input("分辨率 [720p]: ") or "720p"
        args.ratio = input("宽高比 [9:16]: ") or "9:16"

    # 检查必需参数
    if not args.character and not args.skip_portrait:
        print("❌ 需要 --character 或 --skip-portrait")
        sys.exit(1)

    # 读取剧本
    if args.script:
        script_text = Path(args.script).read_text(encoding="utf-8")
    elif args.script_text:
        script_text = args.script_text
    else:
        print("❌ 需要 --script 或 --script-text")
        sys.exit(1)

    print(f"\n{'#'*60}")
    print("# 火山引擎 ARK → 口播成片管道")
    print(f"# Session: {SESSION_ID}")
    print(f"# 剧本长度: {len(script_text)} 字")
    print(f"# 每段时长: {args.duration}s")
    print(f"# 模型: Seedance {args.model}")
    print(f"# 分辨率: {args.resolution}  {args.ratio}")
    print(f"{'#'*60}")

    # 分割剧本
    segments = split_script(script_text, args.duration, args.speech_rate)
    print(f"\n📝 剧本已分为 {len(segments)} 段")
    for i, seg in enumerate(segments):
        print(f"   段{i+1}: {len(seg['prompt'])}字, {seg['duration']}s")

    # ============================================================
    # 🎙️ 外部 TTS 模式: TTS 先行 → 用实际时长覆盖 segment duration
    # ============================================================
    tts_mode = args.external_tts
    if tts_mode:
        from tts_engine import tts_pipeline, merge_video_audio

        # 提取纯文本 (去除 style template)
        style_preview = (
            "特写镜头，正面拍摄。人物保持自然微笑，眼神看向镜头。"
            "柔和摄影棚灯光，纯色背景。"
            "口播文案："
        )
        text_segments = []
        for seg in segments:
            raw = seg["prompt"]
            text = raw.replace(style_preview, "").strip()
            text_segments.append({"text": text})

        # TTS 合成 → 更新 segments 的 duration
        session_dir = str(OUTPUT_DIR / SESSION_ID)
        os.makedirs(session_dir, exist_ok=True)
        segments = tts_pipeline(
            text_segments, session_dir, voice=args.tts_voice
        )
        # 把 text 还原回 prompt
        for seg, tseg in zip(segments, text_segments):
            seg["prompt"] = f"{style_preview}{tseg['text']}"

        print("\n   ⚡ 切换 Seedance 为无声模式 (generate_audio=False)")

    est_total = sum(seg.get("duration", args.duration) for seg in segments)
    print(f"\n   预估总时长: {est_total}s = {est_total/60:.1f}分钟")
    print(f"   预估耗时: {len(segments) * 5}~{len(segments) * 10} 分钟")

    # 确认
    if not args.interactive:
        confirm = input("\n继续? [Y/n]: ").strip().lower()
        if confirm and confirm != "y":
            print("已取消")
            return

    # 切换模型
    global SEEDANCE_MODEL
    if args.model == "2.0":
        SEEDANCE_MODEL = SEEDANCE_2
        print("\n⚠️ 使用 Seedance 2.0 (需确保控制台已开通!)")
    else:
        SEEDANCE_MODEL = SEEDANCE_15_PRO
        # 如果用户没手动指定 duration, 提醒 1.5 Pro 上限
        if args.duration == 15:  # parser default changed to 12 now
            pass  # already 12

    # 阶段1: 定妆照
    if args.skip_portrait:
        if not args.portrait_url:
            print("❌ --skip-portrait 需要 --portrait-url")
            sys.exit(1)
        portrait_urls = [args.portrait_url]
        portrait_paths = [args.portrait_url]
        print(f"\n📸 使用已有定妆照: {args.portrait_url[:80]}...")
    else:
        full_prompt = (
            f"专业摄影棚人像照，正面特写，竖构图。"
            f"{args.character}。"
            f"柔和自然光，干净纯色背景。"
            f"高分辨率，皮肤细节清晰，眼神自然看向镜头。"
        )
        portrait_urls, portrait_paths = generate_portrait_local(full_prompt, args.portrait_size)

    # 阶段2: 分段生成视频
    video_results = generate_segments(
        portrait_url=portrait_urls[0],
        segments=segments,
        duration=args.duration,
        resolution=args.resolution,
        ratio=args.ratio,
        generate_audio=not tts_mode,
    )

    # TTS 模式: 每段视频生成后合并外挂音频
    if tts_mode:
        print(f"\n{'='*60}")
        print(f"🔊 TTS 音视频合并 ({len(video_results)}段)")
        for i, (vr, seg) in enumerate(zip(video_results, segments)):
            tts_path = seg.get("tts_path", "")
            if tts_path and os.path.exists(tts_path):
                merged_path = vr["video_path"].replace(".mp4", "_merged.mp4")
                print(f"   段{i+1}: 合并 {tts_path} → {merged_path}")
                merge_video_audio(vr["video_path"], tts_path, merged_path)
                vr["video_path"] = merged_path  # 替换为合并后的
            else:
                print(f"   段{i+1}: ⚠️ 无 TTS 音频，跳过")

    # 阶段3: 拼接
    video_paths = [r["video_path"] for r in video_results]
    final_path = concat_videos(video_paths)

    # 总结
    print(f"\n{'='*60}")
    print("🎉 管道完成!")
    print(f"   定妆照: {portrait_paths[0] if portrait_paths else 'N/A'}")
    print(f"   视频段: {len(video_paths)} 段")
    print(f"   成片:   {final_path}")
    total_duration = est_total
    print(f"   时长:   {total_duration}s")
    print(f"{'='*60}")

    # 生成报告
    report = {
        "session": SESSION_ID,
        "timestamp": datetime.now().isoformat(),
        "portrait": portrait_paths,
        "segments": [
            {
                "num": r["seg_num"],
                "path": r["video_path"],
                "url": r["video_url"],
                "last_frame_url": r["last_frame_url"],
            }
            for r in video_results
        ],
        "final_video": final_path,
        "total_duration_s": total_duration,
        "config": {
            "seedream_model": SEEDREAM_MODEL,
            "seedance_model": SEEDANCE_MODEL,
            "duration_per_seg": args.duration,
            "resolution": args.resolution,
            "ratio": args.ratio,
        },
    }
    report_path = OUTPUT_DIR / f"{SESSION_ID}_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\n📋 报告: {report_path}")


if __name__ == "__main__":
    main()

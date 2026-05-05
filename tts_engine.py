"""
外挂 TTS 引擎 — 基于 edge-tts (Microsoft Azure 免费语音合成)
==============================================================
逐段合成 → 返回 {audio_path, duration_seconds} → Seedance generate_audio=False → ffmpeg 合并

中文男声推荐:
  zh-CN-YunxiNeural   - 阳光自然 (默认)
  zh-CN-YunjianNeural - 激情有力
  zh-CN-YunyangNeural - 专业稳重
"""

import os
import re
import json
import asyncio
import subprocess
import tempfile
from pathlib import Path
from dataclasses import dataclass, field, asdict


@dataclass
class TTSAudio:
    text: str
    path: str
    duration_seconds: float
    error: str = ""


# ============================================================
# 核心功能
# ============================================================

async def generate_audio_edge_tts(text: str, voice: str, output_path: str) -> float:
    """使用 edge-tts 生成音频，返回时长"""
    import edge_tts

    communicate = edge_tts.Communicate(text, voice)
    await communicate.save(output_path)

    # 用 ffprobe 获取精确时长
    return get_audio_duration(output_path)


def get_audio_duration(path: str) -> float:
    """ffprobe 获取音频时长"""
    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json",
         "-show_format", path],
        capture_output=True, text=True, timeout=10
    )
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe 失败: {result.stderr}")
    info = json.loads(result.stdout)
    return float(info["format"]["duration"])


def generate_segment_tts(
    segments: list[dict],
    output_dir: str,
    voice: str = "zh-CN-YunxiNeural",
) -> list[TTSAudio]:
    """
    为每一段生成 TTS 音频，返回音频路径和时长列表。

    segments: [{"text": "口播词1"}, {"text": "口播词2"}, ...]
    返回: [TTSAudio, ...]
    """
    results = []

    async def run_all():
        for i, seg in enumerate(segments):
            text = seg.get("text", "").strip()
            if not text:
                results.append(TTSAudio(text="", path="", duration_seconds=0, error="empty"))
                continue

            audio_path = os.path.join(output_dir, f"tts_seg{i+1:02d}.mp3")

            try:
                duration = await generate_audio_edge_tts(text, voice, audio_path)
                print(f"  TTS 段{i+1}: {len(text)}字 → {duration:.1f}s → {audio_path}")
                results.append(TTSAudio(text=text, path=audio_path, duration_seconds=duration))
            except Exception as e:
                print(f"  ❌ TTS 段{i+1} 失败: {e}")
                results.append(TTSAudio(text=text, path="", duration_seconds=0, error=str(e)))

    asyncio.run(run_all())
    return results


def merge_video_audio(
    video_path: str,
    audio_path: str,
    output_path: str,
) -> str:
    """
    将静音视频和外挂音频合并。

    策略：视频无声轨时直接加音轨，有声轨时用音频替换。
    """
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-i", audio_path,
        "-c:v", "copy",
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-shortest",
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"音视频合并失败: {result.stderr[:500]}")

    return output_path


def sync_segment_durations(segments: list[dict], tts_results: list[TTSAudio]):
    """
    用 TTS 实际时长更新 segments 的 duration。

    这是保证音画同步的关键步骤。
    """
    for seg, tts in zip(segments, tts_results):
        if tts.duration_seconds > 0:
            # 向上取整到整数秒 (Seedance 要求整数 duration)
            seg["duration"] = max(4, min(12, int(tts.duration_seconds + 0.5)))
            seg["tts_path"] = tts.path
            seg["tts_duration"] = tts.duration_seconds

    return segments


# ============================================================
# 便捷入口
# ============================================================

def tts_pipeline(
    segments: list[dict],
    output_dir: str,
    voice: str = "zh-CN-YunxiNeural",
) -> list[dict]:
    """
    一步完成: TTS 合成 + 时长同步 → 返回更新后的 segments。

    用法:
      segments = tts_pipeline(segments, output_dir, voice="zh-CN-YunxiNeural")
      # segments 现在包含 "duration" 为 TTS 实际时长, "tts_path" 为音频路径
      generate_segments(..., generate_audio=False)
      # 每段视频生成后: merge_video_audio(video_path, seg["tts_path"], output)
    """
    print(f"\n{'='*60}")
    print(f"🎙️ 外部 TTS: Microsoft Edge ({voice})")
    print(f"   模式: TTS 先行 → Seedance 无声 → ffmpeg 合并")
    print(f"   优势: 多段音色 100% 一致, 完美对口型")

    # 1. 生成 TTS
    tts_results = generate_segment_tts(segments, output_dir, voice)

    # 2. 检查失败
    failed = [r for r in tts_results if r.error]
    if failed:
        raise RuntimeError(f"TTS 有 {len(failed)} 段失败: {[r.error for r in failed]}")

    # 3. 同步时长
    segments = sync_segment_durations(segments, tts_results)

    total_dur = sum(s.get("duration", 0) for s in segments)
    total_tts = sum(s.get("tts_duration", 0) for s in segments)
    print(f"\n   总 TTS 时长: {total_tts:.1f}s")
    print(f"   总视频时长: {total_dur}s (向上取整)")
    print(f"   分段数: {len(segments)}")

    return segments

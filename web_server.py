#!/usr/bin/env python3
"""
koubo Web 控制台 — Flask 后端
提供 REST API + SSE 实时进度推送
"""
import os
import sys
import json
import re
import time
import threading
import queue
import subprocess
from pathlib import Path
from datetime import datetime
from flask import Flask, request, jsonify, Response, send_file, render_template_string
from flask_cors import CORS
import urllib3

from ark_client import (
    ARK_API_KEY, ARK_BASE_URL, get_headers, api_session,
    SEEDREAM_MODEL, SEEDANCE_15_PRO, OUTPUT_DIR,
    download_with_retry,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 所有对外 API 调用使用此 session（已在 ark_client 中配置 bypass 代理）
HEADERS = get_headers()
ARK_BASE = ARK_BASE_URL
SEEDANCE_MODEL = SEEDANCE_15_PRO

app = Flask(__name__)
CORS(app)

# 会话存储
sessions: dict[str, dict] = {}
sessions_lock = threading.Lock()


# ============================================================
# SSE 辅助
# ============================================================
def sse_event(event: str, data: dict) -> str:
    """格式化为 SSE 事件字符串"""
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


# ============================================================
# 管道核心（带进度回调）
# ============================================================
class KouboPipeline:
    def __init__(self, session_id: str, params: dict, progress_queue: queue.Queue):
        self.sid = session_id
        self.params = params
        self.q = progress_queue
        self.session_dir = OUTPUT_DIR / self.sid
        self.session_dir.mkdir(exist_ok=True)
        self._aborted = False

    def abort(self):
        self._aborted = True

    def emit(self, event: str, data: dict):
        self.q.put(sse_event(event, data))

    def log(self, msg: str):
        self.emit("log", {"message": msg, "time": datetime.now().strftime("%H:%M:%S")})

    def run(self):
        try:
            self._do_run()
            if self._aborted:
                self.emit("aborted", {"message": "用户取消了生成"})
                return
            self.emit("complete", {
                "final_video": str(self.final_path),
                "final_url": f"/api/download/{self.sid}/final.mp4",
                "portrait_url": f"/api/download/{self.sid}/portrait.png",
            })
        except Exception as e:
            err_msg = str(e)
            self.emit("error", {"message": err_msg})
            with sessions_lock:
                if self.sid in sessions:
                    sessions[self.sid]["status"] = "failed"
                    sessions[self.sid]["error"] = err_msg

    def _do_run(self):
        # ======== 阶段1: 定妆照 ========
        portrait_url = self.params.get("portrait_url")
        
        if portrait_url and not portrait_url.startswith("/api/download/"):
            # 外部公网 URL → 直接下载使用
            self.emit("stage", {"stage": "portrait", "step": 1, "total": 3, "label": "🎨 使用已有定妆照"})
            self.log("使用外部定妆照 URL")
            portrait_data = download_with_retry(portrait_url, "定妆照", max_retries=3, timeout=60)
            portrait_path = self.session_dir / "portrait.png"
            portrait_path.write_bytes(portrait_data)
            self.log(f"✅ 下载照片完成 ({len(portrait_data)//1024}KB)")
            self.emit("portrait_ready", {
                "url": f"/api/download/{self.sid}/portrait.png",
                "size_kb": len(portrait_data) // 1024,
            })
        elif portrait_url and portrait_url.startswith("/api/download/"):
            # 本地上传的照片 → 尝试通过公网隧道访问
            local_rel = portrait_url.replace("/api/download/", "")
            local_path = OUTPUT_DIR / local_rel
            if local_path.exists():
                portrait_data = local_path.read_bytes()
                local_portrait = self.session_dir / "portrait_local.png"
                local_portrait.write_bytes(portrait_data)
                self.log(f"📸 已保存上传照片 ({len(portrait_data)//1024}KB)")
                
                # 尝试构建公网 URL (通过隧道)
                public_url = os.environ.get("KOUBO_PUBLIC_URL", "")
                tunnel_file = OUTPUT_DIR.parent / ".tunnel_url"
                if not public_url and tunnel_file.exists():
                    public_url = tunnel_file.read_text().strip()
                if public_url:
                    # 验证隧道是否真的可达
                    tunnel_ok = False
                    try:
                        test_resp = api_session.get(f"{public_url.rstrip('/')}/api/sessions", timeout=5, verify=False)
                        tunnel_ok = (test_resp.status_code == 200)
                    except:
                        pass
                    
                    if not tunnel_ok:
                        self.log("⚠️ 公网隧道不可达，本地照片无法被 Seedance 访问")
                        self.emit("error", {
                            "message": "本地照片无法被云端访问（公网隧道断开）。\n\n"
                                       "请选择：\n"
                                       "① 重启隧道后重试\n"
                                       "② 切换到「🎨 AI生成」模式，用 Seedream 生成定妆照\n"
                                       "③ 将照片上传到图床，粘贴公网 URL",
                            "action_required": True,
                        })
                        with sessions_lock:
                            if self.sid in sessions:
                                sessions[self.sid]["status"] = "failed"
                                sessions[self.sid]["error"] = "公网隧道断开，本地照片无法访问"
                        return
                
                if public_url:
                    public_portrait = f"{public_url.rstrip('/')}/api/download/{local_rel}"
                    self.log(f"🔗 公网隧道 URL: {public_portrait[:80]}...")
                    # 直接使用公网 URL，跳过 Seedream
                    portrait_url = public_portrait
                    self.emit("stage", {"stage": "portrait", "step": 1, "total": 3, "label": "🎨 使用上传照片(公网隧道)"})
                    self.emit("portrait_ready", {
                        "url": f"/api/download/{self.sid}/portrait.png",
                        "size_kb": len(portrait_data) // 1024,
                    })
                else:
                    self.log("⚠️ 未配置公网隧道，回退到 Seedream 生成定妆照")
                    portrait_url = None  # 触发 Seedream 流程
            else:
                self.log(f"⚠️ 本地照片未找到: {local_path}")
                portrait_url = None
            
        if not portrait_url:
            # Seedream 生成定妆照
            self.emit("stage", {"stage": "portrait", "step": 1, "total": 3, "label": "🎨 定妆照生成"})
            self.log("开始生成定妆照...")

            environment = self.params.get("environment", "干净纯色灰色背景，柔和摄影棚灯光")
            portrait_prompt = (
                f"专业摄影棚人像照，正面特写，竖构图。"
                f"{self.params['character']}。"
                f"背景：{environment}。"
                f"柔和自然光，高分辨率，皮肤细节清晰。"
            )
            size = self.params.get("portrait_size", "1440x2560")
            custom_key = self.params.get("api_key", "")
            custom_model = self.params.get("portrait_model", "")
            use_key = custom_key or ARK_API_KEY
            use_model = custom_model or SEEDREAM_MODEL

            self.log(f"调用 Seedream → {size} (model={use_model})")
            payload = {"model": use_model, "prompt": portrait_prompt, "size": size, "n": 1}
            if "lite" in use_model:
                payload["output_format"] = "png"
            resp = api_session.post(
                f"{ARK_BASE}/images/generations",
                headers={"Authorization": f"Bearer {use_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=120,
            )
            if resp.status_code != 200:
                raise RuntimeError(f"定妆照失败 HTTP {resp.status_code}: {resp.text[:300]}")

            portrait_url = resp.json()["data"][0]["url"]
            portrait_data = download_with_retry(portrait_url, "定妆照", max_retries=3, timeout=60)
            portrait_path = self.session_dir / "portrait.png"
            portrait_path.write_bytes(portrait_data)
            self.log(f"✅ 定妆照完成 ({len(portrait_data)//1024}KB)")

            self.emit("portrait_ready", {
                "url": f"/api/download/{self.sid}/portrait.png",
                "size_kb": len(portrait_data) // 1024,
            })

        # ======== 阶段2: 分段视频 ========
        if self._aborted: return
        script_text = self.params["script"]
        duration = self.params.get("duration", 12)
        ratio = self.params.get("ratio", "9:16")
        resolution = self.params.get("resolution", "720p")
        speech_rate = self.params.get("speech_rate", 4.0)
        custom_segments = self.params.get("custom_segments")

        if custom_segments:
            # 用户手动编辑过分段，直接使用
            segments = [{"text": s["text"], "duration": s["duration"]} for s in custom_segments]
        else:
            segments = KouboPipeline._split_script(script_text, duration, speech_rate)
        total_segs = len(segments)

        # ======== 🎙️ 外部 TTS: TTS 先行，覆盖 duration ========
        external_tts = self.params.get("external_tts", False)
        if external_tts and not custom_segments:
            from tts_engine import tts_pipeline, merge_video_audio
            self.log("🎙️ 外部 TTS 模式: 使用 edge-tts 统一音色")
            segments = tts_pipeline(
                segments, str(self.session_dir),
                voice=self.params.get("tts_voice", "zh-CN-YunxiNeural"),
            )
        elif external_tts and custom_segments:
            self.log("⚠️ 已手动编辑分段，跳过 TTS 时长覆盖")

        # 公网隧道使用完毕 → 可以关闭（Seedance 已下载 first_frame）
        # 延迟 30s 等待 Seedance 下载完图片后自动关闭隧道
        def close_tunnel_delayed():
            time.sleep(30)
            tunnel_pid_file = OUTPUT_DIR.parent / ".tunnel_pid"
            if tunnel_pid_file.exists():
                try:
                    pid = int(tunnel_pid_file.read_text().strip())
                    os.kill(pid, 0)  # check if alive
                    os.kill(pid, 15)  # SIGTERM
                    self.log("🔒 公网隧道已自动关闭（图片已下载完毕）")
                except:
                    pass
        if os.environ.get("KOUBO_PUBLIC_URL") or (OUTPUT_DIR.parent / ".tunnel_url").exists():
            threading.Thread(target=close_tunnel_delayed, daemon=True).start()
            self.log("⏳ 30秒后将自动关闭公网隧道")

        duration_actual = sum(s.get("duration", duration) for s in segments)
        self.log(f"剧本分为 {total_segs} 段，预估总时长 {duration_actual}s")

        self.emit("stage", {"stage": "video", "step": 2, "total": 3, "label": f"🎬 分段视频生成 (1/{total_segs})"})

        ref_url = portrait_url
        video_paths = []

        for i, seg in enumerate(segments):
            seg_num = i + 1
            seg_dur = seg["duration"]
            self.emit("stage", {"stage": "video", "step": 2, "total": 3, "label": f"🎬 分段视频生成 ({seg_num}/{total_segs})"})
            self.emit("progress", {"segment": seg_num, "total_segments": total_segs, "percent": 0})
            self.log(f"段{seg_num}/{total_segs} 提交中... ({seg_dur}s)")

            # 构建 prompt: 风格基座从人物描述+环境动态生成
            character = self.params.get("character", "").strip()
            environment = self.params.get("environment", "干净纯色灰色背景，柔和摄影棚灯光").strip()
            seg_camera = seg.get("camera", "镜头固定，正面拍摄，无变焦，无镜头移动")
            if character:
                style_anchor = f"{character}，{environment}，{seg_camera}，高清画质。"
            else:
                style_anchor = f"人物，干净背景，柔和自然光，{seg_camera}，高清画质。"
            seg_action = seg.get("action", "人物保持自然微笑，眼神看向镜头。")
            full_prompt = f"{style_anchor} {seg_action} 口播：\"{seg['text']}\""

            # 创建任务
            payload = {
                "model": SEEDANCE_MODEL,
                "content": [
                    {"type": "text", "text": full_prompt},
                    {"type": "image_url", "image_url": {"url": ref_url}, "role": "first_frame"},
                ],
                "duration": seg_dur,
                "resolution": resolution,
                "ratio": ratio,
                "generate_audio": not external_tts,  # TTS模式=无声, 默认=口型同步
                "return_last_frame": True,
            }
            task_resp = api_session.post(
                f"{ARK_BASE}/contents/generations/tasks",
                headers=HEADERS, json=payload, timeout=60,
            )
            if task_resp.status_code != 200:
                raise RuntimeError(f"段{seg_num} 创建失败: {task_resp.text[:300]}")

            task_id = task_resp.json()["id"]
            self.log(f"段{seg_num} 任务: {task_id[:30]}...")

            # 轮询
            start = time.time()
            while time.time() - start < 900:
                q = api_session.get(f"{ARK_BASE}/contents/generations/tasks/{task_id}", headers=HEADERS, timeout=30)
                qd = q.json()
                st = qd.get("status", "?")
                elapsed = int(time.time() - start)
                # 推送进度（0-90% 是等待，最后 10% 是下载）
                wait_pct = min(90, int(elapsed / 300 * 90)) if elapsed < 300 else 90
                self.emit("progress", {"segment": seg_num, "total_segments": total_segs, "percent": wait_pct})

                if st == "succeeded":
                    vu = qd.get("content", {}).get("video_url", "")
                    lf = qd.get("content", {}).get("last_frame_url", "")
                    if not vu:
                        raise RuntimeError(f"段{seg_num} 无 video_url")

                    # 下载（带重试，防止瞬时网络故障）
                    vpath = self.session_dir / f"seg{seg_num:02d}.mp4"
                    video_data = download_with_retry(vu, seg_num)
                    vpath.write_bytes(video_data)
                    seg_size = vpath.stat().st_size // 1024
                    self.log(f"✅ 段{seg_num} 完成 ({elapsed}s, {seg_size}KB)")

                    self.emit("segment_done", {
                        "segment": seg_num,
                        "total_segments": total_segs,
                        "file": vpath.name,
                        "size_kb": seg_size,
                        "time_s": elapsed,
                    })
                    video_paths.append(str(vpath))

                    idx = len(video_paths) - 1

                    # TTS 模式: 合并外挂音频
                    if external_tts:
                        tts_path = seg.get("tts_path", "")
                        if tts_path and os.path.exists(tts_path):
                            merged = vpath.with_suffix(".merged.mp4")
                            merge_video_audio(str(vpath), tts_path, str(merged))
                            self.log("   🔊 合并 TTS 音频完成")
                            vpath = merged
                            video_paths[idx] = str(vpath)

                    ref_url = lf if lf else ref_url
                    break
                elif st in ("failed", "expired"):
                    raise RuntimeError(f"段{seg_num} {st}: {qd.get('error', '')}")
                time.sleep(10)  # 每 10s 轮询

        # ======== 阶段3: 拼接 ========
        if self._aborted: return
        self.emit("stage", {"stage": "concat", "step": 3, "total": 3, "label": "🔗 拼接成片"})
        self.log(f"ffmpeg 拼接 {len(video_paths)} 段...")

        concat_list = self.session_dir / "concat.txt"
        with open(concat_list, "w") as f:
            for p in video_paths:
                f.write(f"file '{p}'\n")

        self.final_path = self.session_dir / "final.mp4"
        result = subprocess.run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", str(concat_list), "-c", "copy", str(self.final_path),
        ], capture_output=True, text=True)
        if result.returncode != 0:
            # fallback 重编码
            self.log("concat -c copy 失败，尝试重编码...")
            filter_parts = " ".join([f"[{i}:v][{i}:a]" for i in range(len(video_paths))])
            filter_str = f"{filter_parts} concat=n={len(video_paths)}:v=1:a=1 [v][a]"
            cmd2 = ["ffmpeg", "-y"]
            for p in video_paths:
                cmd2.extend(["-i", p])
            cmd2.extend(["-filter_complex", filter_str, "-map", "[v]", "-map", "[a]",
                         "-c:v", "libx264", "-crf", "23", "-c:a", "aac", "-b:a", "128k",
                         str(self.final_path)])
            r2 = subprocess.run(cmd2, capture_output=True, text=True)
            if r2.returncode != 0:
                raise RuntimeError(f"拼接失败: {r2.stderr[:500]}")

        size_mb = self.final_path.stat().st_size / (1024 * 1024)
        self.log(f"✅ 成片完成 ({size_mb:.1f}MB)")
        self.emit("concat_done", {"size_mb": round(size_mb, 1)})

    # ---- 口语软断点词（通常为句尾语气词）----
    _SPOKEN_BREAKS = re.compile(r'(呢|吧|啊|呀|嘛|哈|啦|哦|哟|呗|咯|噢|喔|诶|嘿|哇|呵)')

    @staticmethod
    def _has_minimal_punctuation(text: str) -> bool:
        """检测标点覆盖率是否足够（>=5% 且至少 2 个标点）"""
        punct = sum(1 for c in text if c in '。！？，；：、\n')
        return punct >= 2 and punct / max(len(text), 1) >= 0.05

    @staticmethod
    def _split_script(text: str, seg_dur: int, rate: float = 4.0, max_duration: int = None) -> list[dict]:
        """
        智能拆分口播剧本。自动检测标点覆盖率，无标点时启用口语词软切。
        
        规则（优先级从高到低）：
        1. 优先在句号/问号/叹号处切 —— 绝不打断完整句子
        2. 无标点时，在口语语气词处软切（呢/吧/啊/嘛/哈/啦）
        3. 每段不超过 seg_dur×rate 字（默认 4字/秒）
        4. 如果一个句子超长，尝试在逗号/分号/顿号处切
        5. 实在切不开就等分（不丢内容）
        
        Returns:
            [{"text": "段文本", "duration": 秒}, ...], 
            以及一个标识 has_punctuation (通过 output 额外字段传递)
        """
        if max_duration is None:
            max_duration = seg_dur
        
        chars_per_seg = int(seg_dur * rate)
        chars_absolute_max = int(max_duration * rate)
        
        has_punct = KouboPipeline._has_minimal_punctuation(text)
        
        # ---- 步骤1: 拆成句子 ----
        sentences = []
        
        if has_punct:
            # 标准路径：按标点符号断句
            cur = ""
            for ch in text:
                cur += ch
                if ch in "。！？\n" and len(cur) >= 6:
                    s = cur.strip()
                    if s:
                        sentences.append(s)
                    cur = ""
            if cur.strip():
                sentences.append(cur.strip())
        else:
            # 无标点路径：用口语语气词做软断点
            cur = ""
            for ch in text:
                cur += ch
                # 遇到语气词 + 至少12个字 → 在语气词后切
                if len(cur) >= 12 and KouboPipeline._SPOKEN_BREAKS.match(ch):
                    s = cur.strip()
                    if s:
                        sentences.append(s)
                    cur = ""
            if cur.strip():
                sentences.append(cur.strip())
            
            # 如果连口语词都没有，按固定字数强制断句
            if len(sentences) <= 1 and len(text) > chars_per_seg:
                sentences = []
                for i in range(0, len(text), chars_per_seg):
                    chunk = text[i:i+chars_per_seg]
                    if chunk.strip():
                        sentences.append(chunk.strip())
                # 尾部如果太短（<10字），合并到前一段
                if len(sentences) >= 2 and len(sentences[-1]) < 10:
                    sentences[-2] += sentences[-1]
                    sentences.pop()
        
        # ---- 步骤2: 处理超长句 ----
        def split_long_sentence(s: str, limit: int) -> list[str]:
            """在逗号/分号/冒号/顿号处切开长句，至少保留一半 limit"""
            if len(s) <= limit:
                return [s]
            soft_breaks = [m.start() for m in re.finditer(r'[，；：、]', s)]
            # 无标点时，用口语词做软断
            if not soft_breaks:
                spoken = [m.start() + 1 for m in KouboPipeline._SPOKEN_BREAKS.finditer(s)]
                if spoken:
                    soft_breaks = [p for p in spoken if limit * 0.5 <= p <= limit * 1.5]
            if not soft_breaks:
                return [s]  # 宁长勿丢
            parts = []
            start = 0
            min_len = int(limit * 0.5)
            for bp in soft_breaks:
                chunk = s[start:bp]
                if len(chunk) >= min_len:
                    parts.append(chunk)
                    start = bp
            if start < len(s):
                parts.append(s[start:])
            if len(parts) >= 2 and len(parts[-1]) < min_len:
                parts[-2] += parts[-1]
                parts.pop()
            return parts if parts else [s]
        
        flat_sentences = []
        for s in sentences:
            if len(s) > chars_absolute_max:
                parts = split_long_sentence(s, chars_absolute_max)
                flat_sentences.extend(parts)
            else:
                flat_sentences.append(s)
        
        # ---- 步骤3: 按容量打包 ----
        segments = []
        cur_seg = ""
        for s in flat_sentences:
            new_len = len(cur_seg) + len(s)
            if new_len > chars_per_seg and cur_seg:
                segments.append(cur_seg.strip())
                cur_seg = s
            else:
                cur_seg += s
        
        if cur_seg.strip():
            segments.append(cur_seg.strip())
        
        # ---- 步骤4: 计算每段实际时长（字数÷语速 + 等比缓冲）----
        result = []
        for text in segments:
            actual_chars = len(text)
            speech_dur = actual_chars / rate
            buffer = max(1.0, speech_dur * 0.2)  # 20%缓冲，最少1s
            total_dur = speech_dur + buffer
            actual_dur = max(4, min(max_duration, round(total_dur)))
            result.append({"text": text, "duration": actual_dur, "char_count": actual_chars})
        
        return result


# ============================================================
# 路由
# ============================================================

@app.route("/api/preview_split", methods=["POST"])
def api_preview_split():
    """预览剧本切割结果（不执行生成）"""
    data = request.json
    script = data.get("script", "")
    duration = int(data.get("duration", 12))
    rate = float(data.get("speech_rate", 4.0))
    
    segments = KouboPipeline._split_script(script, duration, rate)
    has_punct = KouboPipeline._has_minimal_punctuation(script)
    
    # 切割质量评估
    max_chars = duration * rate
    oversized = [s for s in segments if len(s["text"]) > max_chars * 1.2]
    quality = "good" if not oversized and has_punct else "warning" if oversized else "poor"
    
    return jsonify({
        "total_segments": len(segments),
        "total_duration_est": sum(s["duration"] for s in segments),
        "speech_rate": rate,
        "has_punctuation": has_punct,
        "quality": quality,
        "quality_note": (
            "✅ 切割质量良好" if quality == "good" else
            "⚠️ 部分段落超长，建议调低语速或启用 AI 智能分段" if quality == "warning" else
            "❌ 文案缺少标点符号，建议点击「AI 智能分段」自动加标点" if not has_punct else
            "⚠️ 切割质量不佳"
        ),
        "segments": [
            {
                "index": i + 1,
                "text": s["text"],
                "char_count": len(s["text"]),
                "duration": s["duration"],
                "action": "人物保持自然微笑，眼神看向镜头。",
                "preview": s["text"][:50] + ("..." if len(s["text"]) > 50 else ""),
            }
            for i, s in enumerate(segments)
        ],
    })


# 在 index 路由之前插入
@app.route("/")
def index():
    template_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as f:
            return f.read()
    return render_template_string(HTML_TEMPLATE)



@app.route("/api/smart_segment", methods=["POST"])
def api_smart_segment():
    """
    AI 智能分段 — 调用 LLM 为无标点文案自动添加标点符号。
    使用 OpenClaw clip-director agent 或直接调用 DeepSeek。
    """
    data = request.json
    script = data.get("script", "")
    
    if not script.strip():
        return jsonify({"error": "请提供文案"}), 400
    
    # 优先用 OpenClaw agent，失败则用 DeepSeek API
    import subprocess
    
    prompt = f"""请为以下口播文案添加合适的标点符号（句号、逗号、感叹号等）。
只添加标点，不改变任何文字内容，不添加任何解释。
标点风格：口播自然节奏，每句话15-40字为宜。

原文案：
{script}

加标点后："""
    
    punctuated = None
    
    # 方式1: OpenClaw clip-director
    try:
        result = subprocess.run(
            ["openclaw", "agent", "--agent", "clip-director", "-m", prompt, "--json", "--local"],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            raw = result.stdout + result.stderr
            start = raw.find('{"payloads"')
            if start >= 0:
                d = json.loads(raw[start:])
                text = d["payloads"][0].get("text", "")
                if text and len(text) > len(script) * 0.5:
                    punctuated = text.strip()
    except Exception as e:
        print(f"OpenClaw分段失败: {e}")
    
    # 方式2: DeepSeek API 直调
    if not punctuated:
        try:
            ds_key = "sk-62a850eeede747dea0512a4186570581"
            resp = api_session.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {ds_key}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "你是一个标点符号修复工具。只输出带标点的原文，不添加任何解释或额外文字。"},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.1,
                    "max_tokens": len(script) * 3,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                punctuated = resp.json()["choices"][0]["message"]["content"].strip()
        except Exception as e:
            print(f"DeepSeek分段失败: {e}")
    
    if not punctuated:
        return jsonify({"error": "AI 分段失败，请手动添加标点后重试"}), 500
    
    # 用加了标点的文本重新切割
    duration = int(data.get("duration", 12))
    rate = float(data.get("speech_rate", 4.0))
    segments = KouboPipeline._split_script(punctuated, duration, rate)
    
    return jsonify({
        "success": True,
        "punctuated_text": punctuated,
        "total_segments": len(segments),
        "total_duration_est": sum(s["duration"] for s in segments),
        "segments": [
            {
                "index": i + 1,
                "text": s["text"],
                "char_count": len(s["text"]),
                "duration": s["duration"],
                "action": "人物保持自然微笑，眼神看向镜头。",
                "preview": s["text"][:50] + ("..." if len(s["text"]) > 50 else ""),
            }
            for i, s in enumerate(segments)
        ],
    })



@app.route("/api/preview_portrait", methods=["POST"])
def api_preview_portrait():
    """生成肖像预览（仅生成定妆照，不进入管道）"""
    data = request.json
    character = data.get("character", "")
    if not character.strip():
        return jsonify({"error": "请提供人物描述"}), 400
    
    size = data.get("size", "1440x2560")
    environment = data.get("environment", "干净纯色灰色背景，柔和摄影棚灯光")
    
    # 支持用户自定义 API Key 和模型
    custom_key = data.get("api_key", "")
    custom_model = data.get("model", "")
    use_key = custom_key or ARK_API_KEY
    use_model = custom_model or SEEDREAM_MODEL
    
    portrait_prompt = (
        f"专业摄影棚人像照，正面特写，竖构图。"
        f"{character}。"
        f"背景：{environment}。"
        f"柔和自然光，高分辨率，皮肤细节清晰。"
    )
    
    try:
        resp = api_session.post(
            f"{ARK_BASE}/images/generations",
            headers={"Authorization": f"Bearer {use_key}", "Content-Type": "application/json"},
            json={"model": use_model, "prompt": portrait_prompt, "size": size, "n": 1, "output_format": "png"},
            timeout=120,
        )
        if resp.status_code != 200:
            return jsonify({"error": f"Seedream 失败 HTTP {resp.status_code}: {resp.text[:200]}"}), 500
        
        portrait_url = resp.json()["data"][0]["url"]
        portrait_data = download_with_retry(portrait_url, "定妆照", max_retries=3, timeout=60)
        
        # 保存到 output/preview/ 目录
        preview_dir = OUTPUT_DIR / "previews"
        preview_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        preview_path = preview_dir / f"portrait_{timestamp}.png"
        preview_path.write_bytes(portrait_data)
        
        return jsonify({
            "url": f"/api/download/previews/portrait_{timestamp}.png",
            "direct_url": portrait_url,
            "size_kb": len(portrait_data) // 1024,
            "prompt": portrait_prompt,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/test_connection", methods=["POST"])
def api_test_connection():
    """测试 API 连接是否正常"""
    import time as _time
    data = request.json
    api_key = data.get("api_key", "").strip() or ARK_API_KEY
    api_base = data.get("api_base", "").strip() or ARK_BASE
    model = data.get("model", "").strip() or SEEDREAM_MODEL

    if not api_key:
        return jsonify({"ok": False, "error": "未提供 API Key"})

    results = []

    # 测试1：API 可达性
    t0 = _time.time()
    try:
        r = api_session.get(f"{api_base}/models", headers={"Authorization": f"Bearer {api_key}"}, timeout=15)
        t1 = _time.time()
        if r.status_code == 200:
            results.append({"step": "API 连接", "ok": True, "latency_ms": int((t1-t0)*1000)})
        elif r.status_code == 401:
            return jsonify({"ok": False, "error": "API Key 无效（401 认证失败），请检查 Key 是否正确或已过期"})
        elif r.status_code == 403:
            return jsonify({"ok": False, "error": "API 拒绝访问（403），请检查权限"})
        elif r.status_code == 404:
            return jsonify({"ok": False, "error": f"API 地址错误（404），请确认地址为 https://ark.cn-beijing.volces.com/api/v3，当前填的是: {api_base}"})
        else:
            return jsonify({"ok": False, "error": f"API 返回异常状态 {r.status_code}: {r.text[:200]}"})
    except requests.exceptions.Timeout:
        return jsonify({"ok": False, "error": f"连接超时，无法访问 {api_base}，请检查 API 地址和网络"})
    except requests.exceptions.ConnectionError as e:
        return jsonify({"ok": False, "error": f"连接失败: {str(e)[:200]}"})
    except Exception as e:
        return jsonify({"ok": False, "error": f"未知错误: {str(e)[:200]}"})

    # 测试2：模型是否可用
    try:
        r2 = api_session.post(
            f"{api_base}/images/generations",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": model, "prompt": "a simple blue circle", "size": "1440x2560", "n": 1},
            timeout=30,
        )
        if r2.status_code == 200:
            results.append({"step": f"模型 {model}", "ok": True})
        elif r2.status_code == 404:
            results.append({"step": f"模型 {model}", "ok": False, "error": "模型不存在（404），请检查模型名称"})
        elif "insufficient" in r2.text.lower() or "quota" in r2.text.lower():
            results.append({"step": f"模型 {model}", "ok": False, "error": "配额不足，请检查账户余额"})
        else:
            results.append({"step": f"模型 {model}", "ok": False, "error": f"HTTP {r2.status_code}: {r2.text[:150]}"})
    except Exception as e:
        results.append({"step": f"模型 {model}", "ok": False, "error": str(e)[:200]})

    all_ok = all(r["ok"] for r in results)
    return jsonify({"ok": all_ok, "results": results})


@app.route("/api/upload_portrait", methods=["POST"])
def api_upload_portrait():
    """上传本地肖像照片"""
    if "portrait" not in request.files:
        return jsonify({"error": "请选择图片文件"}), 400
    
    file = request.files["portrait"]
    if file.filename == "":
        return jsonify({"error": "文件名为空"}), 400
    
    # 只接受图片格式
    allowed_ext = {".png", ".jpg", ".jpeg", ".webp", ".bmp"}
    ext = Path(file.filename).suffix.lower()
    if ext not in allowed_ext:
        return jsonify({"error": f"不支持的格式 {ext}，请上传 {', '.join(allowed_ext)}"}), 400
    
    preview_dir = OUTPUT_DIR / "previews"
    preview_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"upload_{timestamp}{ext}"
    save_path = preview_dir / filename
    file.save(str(save_path))
    
    import subprocess as sp
    # 如果是非 PNG 格式，转为 PNG 供 Seedance 使用
    if ext != ".png":
        png_path = preview_dir / f"upload_{timestamp}.png"
        sp.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(save_path), str(png_path)], check=True)
        save_path = png_path
    
    return jsonify({
        "url": f"/api/download/previews/{save_path.name}",
        "size_kb": save_path.stat().st_size // 1024,
        "filename": file.filename,
    })


@app.route("/api/describe_portrait", methods=["POST"])
def api_describe_portrait():
    """用 AI 识别上传照片，自动生成人物描述"""
    data = request.json
    image_url = data.get("url", "")
    if not image_url:
        return jsonify({"error": "缺少图片 URL"}), 400
    
    # 构建本地完整路径
    # url 格式: /api/download/previews/xxx.png
    filename = image_url.rsplit("/", 1)[-1]
    image_path = OUTPUT_DIR / "previews" / filename
    if not image_path.exists():
        return jsonify({"error": "图片文件不存在"}), 404
    
    import base64
    image_b64 = base64.b64encode(image_path.read_bytes()).decode()
    ext = image_path.suffix.lower().replace(".", "")
    mime_map = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "webp": "image/webp"}
    mime = mime_map.get(ext, "image/png")
    
    prompt = """请仔细观察这张照片中的人物，生成一段超写实人像描述，直接用于 Seedance 口播视频的人物一致性控制。

要求按以下结构，一段流畅中文，200-400字：

1. 摄影基调：超写实人像摄影，竖屏 9:16，抖音短视频截图质感
2. 人物外貌：年龄区间、发型发色（具体长度/卷直/分缝）、面部表情（眉头/眼神/嘴巴具体状态）、皮肤质感
3. 服装：颜色+材质+款式+细节（领口/袖口/图案/纹理全部描述）
4. 配饰：项链/耳环/眼镜/麦克风等全部列出
5. 姿态动作：坐/站姿，手臂手势，手持物品
6. 环境背景：家具/墙面/装饰物/植物，左右上下方位具体描述
7. 光影质感：光源方向+类型，阴影特点，面料光泽，景深，画质关键词（8K 高清，细节拉满，真实光影，电影级质感）

只输出纯描述文本，不要列表、不要前缀、不要解释。"""
    
    try:
        resp = api_session.post(
            f"{ARK_BASE}/chat/completions",
            headers=HEADERS,
            json={"model": "doubao-seed-1-6-vision-250815", "messages": [
                {"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{image_b64}"}},
                    {"type": "text", "text": prompt}
                ]}
            ], "max_tokens": 800, "temperature": 0.7},
            timeout=30,
        )
        if resp.status_code != 200:
            return jsonify({"error": f"AI 调用失败: {resp.text[:200]}"}), 500
        
        desc = resp.json()["choices"][0]["message"]["content"].strip()
        # 保存描述到同名 .txt 文件，供形象库展示
        desc_path = image_path.with_suffix(".txt")
        desc_path.write_text(desc, encoding="utf-8")
        return jsonify({"description": desc})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route("/api/history")
def api_history():
    """获取生成历史列表"""
    session_dirs = sorted(OUTPUT_DIR.glob("20*"), reverse=True)
    results = []
    for d in session_dirs[:50]:  # 最多50条
        if not d.is_dir():
            continue
        final = d / "final.mp4"
        portrait = d / "portrait.png"
        if not final.exists():
            continue
        segs = sorted(d.glob("seg*.mp4"))
        results.append({
            "session_id": d.name,
            "created": d.name.replace("_", " "),
            "final_url": f"/api/download/{d.name}/final.mp4",
            "portrait_url": f"/api/download/{d.name}/portrait.png" if portrait.exists() else None,
            "size_mb": round(final.stat().st_size / (1024 * 1024), 1),
            "segments": len(segs),
        })
    return jsonify(results)


@app.route("/api/history/<session_id>", methods=["DELETE"])
def api_delete_history(session_id):
    """删除历史记录"""
    import shutil
    d = OUTPUT_DIR / session_id
    if d.exists():
        shutil.rmtree(d)
        return jsonify({"success": True})
    return jsonify({"error": "not found"}), 404


@app.route("/api/colloquialize", methods=["POST"])
def api_colloquialize():
    """一键口语化 — 用 LLM 将书面文案转为口播风格"""
    data = request.json
    script = data.get("script", "")
    if not script.strip():
        return jsonify({"error": "请提供文案"}), 400
    
    prompt = f"""请将以下书面文案改写为适合口播的口语化表达。
要求：
- 保持原意和关键信息
- 添加自然的语气词（呢、吧、啊、嘛、就是、对吧）
- 句式更短、更口语化
- 不要改变专业术语
- 只输出改写后文本，不添加解释

原文案：
{script}

口语化版本："""
    
    result_text = None
    # DeepSeek API
    try:
        resp = api_session.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers={"Authorization": "Bearer sk-62a850eeede747dea0512a4186570581", "Content-Type": "application/json"},
            json={"model": "deepseek-chat", "messages": [
                {"role": "system", "content": "你是口播文案改写专家。只输出改写后的文案。"},
                {"role": "user", "content": prompt}
            ], "temperature": 0.7, "max_tokens": len(script) * 4},
            timeout=30,
        )
        if resp.status_code == 200:
            result_text = resp.json()["choices"][0]["message"]["content"].strip()
    except:
        pass
    
    if not result_text:
        return jsonify({"error": "口语化失败，请稍后重试"}), 500

    return jsonify({"original": script, "colloquial": result_text})


@app.route("/api/design_actions", methods=["POST"])
def api_design_actions():
    """根据完整口播剧本，用 LLM 为每段设计自然生动的人物动作"""
    data = request.json
    script = data.get("script", "")
    segments = data.get("segments", [])
    character = data.get("character", "人物")
    custom_prompt = data.get("custom_prompt", "").strip()

    if not script or not segments:
        return jsonify({"error": "缺少参数: script 或 segments"}), 400

    seg_preview = "\n".join(
        f"段{i+1}（{s.get('duration', 8)}秒）：{s.get('text', '')}"
        for i, s in enumerate(segments)
    )

    # 默认要求（专业动作导演框架）
    default_rules = """你是一位拥有10年以上影视表演指导经验的资深动作设计师，擅长将文字内容转化为自然、有感染力的肢体语言和面部表情。

【设计原则】
1. 情感优先：动作必须准确传达文案的情感基调（严肃、轻松、惊讶、自信、亲切等）
2. 自然真实：避免机械、夸张的动作，幅度适中，符合日常交流场景
3. 节奏同步：动作与切片时长精确匹配，关键动作落在文案重音和停顿处
4. 重点突出：在核心信息、关键词和结论处设计强调性动作
5. 连贯性：相邻切片之间动作自然过渡，避免突兀切换
6. 多样性：避免重复相同动作，保持肢体语言丰富性
7. 避免AI感：不用"人物保持"、"自然微笑"等模板化表达；用具体动作替代抽象描述

【输出格式】
对每个切片，输出一句完整的动作描述，融合以下要素（选取合适的，不必全部覆盖）：
- 面部表情变化 + 眼神方向 + 手部动作 + 身体姿态 + 关键时间点的动作节奏

每段只用 1-2 句话，像真人导演给演员说戏一样简洁，不要啰嗦。"""

    action_rules = custom_prompt if custom_prompt else default_rules

    # 提取角色关键特征，供动作设计参考
    char_traits = character
    # 尝试从角色描述中提取关键特征词（年龄、性别、风格等）
    import re as _re2
    trait_patterns = [
        r'(年轻|中年|老年|少年|少女|男生|女生|男性|女性)[^，。；]*',
        r'(商务|休闲|正式|运动|古装|职业|学生|职场|日常|优雅|活泼|严肃|温柔|干练|稳重|随性|酷帅|可爱|知性|时尚)',
        r'(扮演|角色|身份|风格)[：:]*[^，。；]{2,20}',
    ]
    trait_keywords = []
    for pat in trait_patterns:
        for m in _re2.findall(pat, character):
            trait_keywords.append(m)
    traits_summary = '、'.join(trait_keywords[:5]) if trait_keywords else character[:80]

    prompt = f"""你是口播视频动作导演。你的设计流程分两步：先通读完整剧本把握整体情绪走向和节奏，再为每一段口播设计具体的表演动作。

【第一步：通读完整剧本，把握整体】
先理解整个视频从开头到结尾的情绪弧线、节奏变化、核心信息点：
{script}

【角色设定及关键特征】
身份：{character}
关键特征：{traits_summary}

【第二步：为每一段口播切片设计动作】
根据你刚才对完整剧本的理解，为下面每一段台词设计自然、贴合内容的表演动作：
{seg_preview}

【动作设计要求】
- 每段的动作要服务于该段台词的具体内容和情感，不能脱离文本
- 相邻段之间动作要自然过渡，形成连贯的表演弧线
- 关键信息、转折点、金句处必须设计强调性动作
- 开头段和结尾段的动作要有仪式感（出场/收尾）
- 情绪激动的段落动作可以大一些，平缓叙述的段落动作收敛
{action_rules}

⚠️ 动作贴合三原则：
1. 先贴合角色身份（年龄/气质/职业决定动作风格）
2. 再贴合该段台词内容（说的话决定手势和表情）
3. 最后贴合整体情绪弧线（前段→高潮→收尾的动作力度变化）
示例：剧本讲创业艰辛→开篇可以低头沉思→中段讲到突破时抬手握拳→结尾微笑收束

按 JSON 格式输出，只输出纯 JSON，不要 markdown 代码块，不要加任何解释。actions 数组内的动作描述字符串中不要包含未转义的双引号：
{{"actions": ["动作1", "动作2", ...]}}"""

    try:
        resp = api_session.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers={"Authorization": "Bearer sk-62a850eeede747dea0512a4186570581", "Content-Type": "application/json"},
            json={"model": "deepseek-chat", "messages": [
                {"role": "system", "content": "你是专业口播动作导演。只输出 JSON，不解释。"},
                {"role": "user", "content": prompt}
            ], "temperature": 0.8, "max_tokens": 2000},
            timeout=30,
        )
        if resp.status_code != 200:
            return jsonify({"error": f"LLM 调用失败: {resp.text[:200]}"}), 500

        content = resp.json()["choices"][0]["message"]["content"].strip()

        import re as _re
        # 优先提取 markdown 代码块中的 JSON
        json_match = _re.search(r'```(?:json)?\s*\n?([\s\S]*?)\n?```', content)
        if json_match:
            content = json_match.group(1).strip()
        else:
            # 尝试匹配带花括号的完整 JSON
            json_match = _re.search(r'\{[\s\S]*?\}\s*$', content)
            if json_match:
                content = json_match.group()
        
        # 清理可能的 BOM 和不可见字符
        content = content.strip().lstrip('\ufeff')
        
        # 多层 JSON 解析回退
        result = None
        parse_errors = []
        
        # 1. 直接解析
        try:
            result = json.loads(content)
        except json.JSONDecodeError as e1:
            parse_errors.append(f"直接解析失败: {e1}")
            
            # 2. 尝试替换中文引号为英文引号后解析
            try:
                fixed = content.replace('\u201c', '"').replace('\u201d', '"').replace('\u2018', "'").replace('\u2019', "'")
                result = json.loads(fixed)
            except json.JSONDecodeError as e2:
                parse_errors.append(f"替换引号后仍失败: {e2}")
                
                # 3. 尝试用 ast.literal_eval 解析（更宽容）
                try:
                    import ast
                    fixed2 = content
                    # 确保是有效的 Python 字面量
                    if fixed2.startswith('{') and fixed2.endswith('}'):
                        result = ast.literal_eval(fixed2)
                except Exception as e3:
                    parse_errors.append(f"ast 解析失败: {e3}")
                
                # 4. 最后手段：正则提取 actions 数组
                if result is None:
                    arr_match = _re.search(r'"actions"\s*:\s*\[([\s\S]*?)\]', content)
                    if arr_match:
                        try:
                            actions_raw = arr_match.group(1)
                            # 匹配所有引号包裹的字符串
                            str_matches = _re.findall(r'"([^"\\]*(?:\\.[^"\\]*)*)"', actions_raw)
                            if str_matches:
                                result = {"actions": str_matches}
                        except Exception:
                            pass
        
        if result is None:
            return jsonify({
                "error": "JSON 解析失败（已尝试多种回退策略）",
                "raw": content[:800],
                "parse_errors": parse_errors[-3:],
            }), 500
        actions = result.get("actions", [])
        
        if not actions:
            return jsonify({"error": "LLM 返回的 actions 为空", "raw": content[:500]}), 500

        while len(actions) < len(segments):
            actions.append("人物自然注视镜头，微微点头。")
        actions = actions[:len(segments)]

        return jsonify({"actions": actions})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/start", methods=["POST"])
def api_start():
    data = request.json
    if not data.get("script"):
        return jsonify({"error": "缺少参数: script"}), 400
    if not data.get("character") and not data.get("portrait_url"):
        return jsonify({"error": "需要人物描述(character)或上传照片(portrait_url)"}), 400

    sid = datetime.now().strftime("%Y%m%d_%H%M%S")
    q = queue.Queue()

    params = {
        "character": data.get("character", ""),
        "script": data["script"],
        "duration": int(data.get("duration", 12)),
        "ratio": data.get("ratio", "9:16"),
        "resolution": data.get("resolution", "720p"),
        "portrait_size": data.get("portrait_size", "1440x2560"),
        "speech_rate": float(data.get("speech_rate", 4.0)),
        "portrait_url": data.get("portrait_url"),
        "environment": data.get("environment", "干净纯色灰色背景，柔和摄影棚灯光"),
        "external_tts": data.get("external_tts", False),
        "tts_voice": data.get("tts_voice", "zh-CN-YunxiNeural"),
        "api_key": data.get("api_key", ""),
        "portrait_model": data.get("portrait_model", ""),
    }

    with sessions_lock:
        sessions[sid] = {"status": "running", "params": params, "queue": q, "created": time.time()}

    pipeline = KouboPipeline(sid, params, q)
    sessions[sid]["_pipeline"] = pipeline
    t = threading.Thread(target=pipeline.run, daemon=True)
    t.start()

    return jsonify({"session_id": sid})


@app.route("/api/events/<session_id>")
def api_events(session_id):
    """SSE 实时进度推送"""
    with sessions_lock:
        session = sessions.get(session_id)
        if not session:
            return jsonify({"error": "会话不存在"}), 404

    q = session["queue"]

    def generate():
        yield sse_event("connected", {"session_id": session_id})
        while True:
            try:
                msg = q.get(timeout=30)
                yield msg
                if "event: complete" in msg or "event: error" in msg:
                    break
            except queue.Empty:
                yield sse_event("heartbeat", {"time": time.time()})
                # 检查是否已完成
                with sessions_lock:
                    if session_id in sessions and sessions[session_id].get("status") in ("completed", "failed"):
                        break

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/status/<session_id>")
def api_status(session_id):
    with sessions_lock:
        s = sessions.get(session_id)
    if not s:
        return jsonify({"error": "not found"}), 404
    return jsonify({"session_id": session_id, "status": s["status"], "error": s.get("error", ""), "params": s.get("params")})


@app.route("/api/download/<session_id>/<filename>")
def api_download(session_id, filename):
    # session_id 可能是 "previews" 特殊目录
    if session_id == "previews":
        path = OUTPUT_DIR / "previews" / filename
    else:
        path = OUTPUT_DIR / session_id / filename
    if not path.exists():
        return jsonify({"error": "文件不存在"}), 404
    return send_file(str(path), as_attachment=True, download_name=filename)


@app.route("/api/segments/<session_id>")
def api_segments(session_id):
    """返回某 session 的分段视频列表（如 seg01.mp4, seg02.mp4...）"""
    session_dir = OUTPUT_DIR / session_id
    if not session_dir.exists():
        return jsonify({"error": "not found"}), 404
    seg_files = sorted([f for f in session_dir.glob("seg*.mp4") if ".merged." not in f.name])
    segments = [{
        "name": f.name,
        "url": f"/api/download/{session_id}/{f.name}",
        "size_kb": f.stat().st_size // 1024,
    } for f in seg_files]
    return jsonify({"segments": segments, "total": len(segments)})


@app.route("/api/portraits")
def api_portraits():
    """返回所有已保存的定妆照/上传照片列表"""
    previews_dir = OUTPUT_DIR / "previews"
    if not previews_dir.exists():
        return jsonify({"portraits": []})
    
    # 收集项目中的 portrait_url 和对应的 character
    project_characters = {}
    projects_dir = OUTPUT_DIR / "projects"
    if projects_dir.exists():
        for pf in sorted(projects_dir.glob("*.json"), reverse=True):
            try:
                pdata = json.loads(pf.read_text())
                pu = pdata.get("portraitUrl", "")
                ch = pdata.get("character", "")
                if pu and ch and pu not in project_characters:
                    project_characters[pu] = ch
            except: pass
    
    portraits = []
    for f in sorted(previews_dir.glob("*"), reverse=True):
        if f.suffix.lower() not in ('.png', '.jpg', '.jpeg', '.webp'):
            continue
        url = f"/api/download/previews/{f.name}"
        # 读取 AI 描述文件
        desc_file = f.with_suffix(".txt")
        ai_desc = desc_file.read_text(encoding="utf-8").strip() if desc_file.exists() else ""
        portraits.append({
            "filename": f.name,
            "url": url,
            "size_kb": f.stat().st_size // 1024,
            "character": project_characters.get(url, "") or ai_desc,
            "ai_desc": ai_desc,
        })
    
    return jsonify({"portraits": portraits[:20]})  # 最多20张


@app.route("/api/portraits/<filename>", methods=["DELETE"])
def api_portrait_delete(filename):
    """删除服务器形象库中的某张照片"""
    # 安全检查：只允许在 previews 目录内
    previews_dir = OUTPUT_DIR / "previews"
    filepath = (previews_dir / filename).resolve()
    if not str(filepath).startswith(str(previews_dir.resolve())):
        return jsonify({"error": "非法路径"}), 403
    if not filepath.exists():
        return jsonify({"error": "文件不存在"}), 404
    try:
        filepath.unlink()
        return jsonify({"ok": True, "deleted": filename})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/tunnel/restart", methods=["POST"])
def api_tunnel_restart():
    """重启 serveo.net 公网隧道"""
    tunnel_url_file = OUTPUT_DIR.parent / ".tunnel_url"
    tunnel_pid_file = OUTPUT_DIR.parent / ".tunnel_pid"
    
    # 1. 杀掉旧隧道进程
    if tunnel_pid_file.exists():
        try:
            old_pid = int(tunnel_pid_file.read_text().strip())
            os.kill(old_pid, 9)
        except: pass
        tunnel_pid_file.unlink(missing_ok=True)
    
    # 2. 清旧 URL
    tunnel_url_file.unlink(missing_ok=True)
    
    # 3. 启动新隧道
    tunnel_script = OUTPUT_DIR.parent / "tunnel.py"
    if not tunnel_script.exists():
        return jsonify({"error": "tunnel.py 不存在"}), 500
    
    proc = subprocess.Popen(
        [sys.executable, str(tunnel_script)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        cwd=str(OUTPUT_DIR.parent)
    )
    
    # 4. 等待 URL 写入（最多等 20 秒）
    deadline = time.time() + 20
    url = None
    buf = ""
    while time.time() < deadline:
        if tunnel_url_file.exists():
            url = tunnel_url_file.read_text().strip()
            if url.startswith("http"):
                break
        line = proc.stdout.readline() if proc.stdout else ""
        if line:
            buf += line
            # 从输出中也尝试提取 URL
            if not url:
                m = re.search(r'https://[^\s]+', line)
                if m:
                    url = m.group(0)
                    tunnel_url_file.write_text(url)
                    break
        time.sleep(0.5)
    
    if not url:
        proc.kill()
        return jsonify({"error": f"隧道启动超时: {buf[-200:]}"}), 500
    
    return jsonify({"ok": True, "url": url, "message": "隧道已重启"})


@app.route("/api/tunnel/status")
def api_tunnel_status():
    """检查公网隧道状态"""
    tunnel_url_file = OUTPUT_DIR.parent / ".tunnel_url"
    tunnel_pid_file = OUTPUT_DIR.parent / ".tunnel_pid"
    
    url = None
    alive = False
    pid = None
    
    if tunnel_url_file.exists():
        url = tunnel_url_file.read_text().strip()
    if tunnel_pid_file.exists():
        try:
            pid = int(tunnel_pid_file.read_text().strip())
            # 检查进程是否存在
            os.kill(pid, 0)
            alive = True
        except: pass
    
    # 如果 URL 存在且有进程，进一步验证隧道是否可达
    tunnel_ok = False
    if alive and url:
        try:
            test_resp = api_session.get(f"{url.rstrip('/')}/api/sessions", timeout=5, verify=False)
            tunnel_ok = (test_resp.status_code == 200)
        except: pass
    
    return jsonify({
        "url": url,
        "pid": pid,
        "process_alive": alive,
        "tunnel_ok": tunnel_ok,
        "status": "connected" if tunnel_ok else ("process_only" if alive else "disconnected")
    })


@app.route("/api/sessions")
def api_sessions():
    with sessions_lock:
        result = []
        for sid, s in sessions.items():
            result.append({"session_id": sid, "status": s["status"],
                           "character": s.get("params", {}).get("character", "")[:40],
                           "created": s.get("created", 0)})
    return jsonify(sorted(result, key=lambda x: x["created"], reverse=True))


@app.route("/api/abort/<session_id>", methods=["POST"])
def api_abort(session_id):
    with sessions_lock:
        s = sessions.get(session_id)
        if not s:
            return jsonify({"error": "not found"}), 404
        pipeline = s.get("_pipeline")
        if pipeline:
            pipeline.abort()
        s["status"] = "aborted"
    return jsonify({"ok": True})


# ============================================================
# 项目持久化 API（服务端存储，不怕清缓存）
# ============================================================
PROJECTS_DIR = OUTPUT_DIR / "projects"
PROJECTS_DIR.mkdir(exist_ok=True)


@app.route("/api/projects", methods=["GET"])
def api_list_projects():
    """列出所有项目"""
    projects = []
    for f in sorted(PROJECTS_DIR.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            data["_filename"] = f.stem
            projects.append(data)
        except Exception:
            pass
    return jsonify(projects)


@app.route("/api/projects", methods=["POST"])
def api_save_project():
    """保存/更新项目"""
    data = request.json
    pid = data.get("projectId", "")
    if not pid:
        return jsonify({"error": "缺少 projectId"}), 400
    fpath = PROJECTS_DIR / f"{pid}.json"
    data["_saved_at"] = datetime.now().isoformat()
    fpath.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return jsonify({"ok": True, "projectId": pid})


@app.route("/api/projects/<pid>", methods=["GET"])
def api_load_project(pid):
    """加载单个项目"""
    fpath = PROJECTS_DIR / f"{pid}.json"
    if not fpath.exists():
        return jsonify({"error": "项目不存在"}), 404
    data = json.loads(fpath.read_text(encoding="utf-8"))
    return jsonify(data)


@app.route("/api/projects/<pid>", methods=["DELETE"])
def api_delete_project(pid):
    """删除项目（移到回收站）"""
    fpath = PROJECTS_DIR / f"{pid}.json"
    trash_dir = PROJECTS_DIR / ".trash"
    trash_dir.mkdir(exist_ok=True)
    if fpath.exists():
        fpath.rename(trash_dir / f"{pid}.json")
    return jsonify({"ok": True})


# ============================================================
# HTML 模板
# ============================================================
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>koubo · 口播成片控制台</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#0f1117;color:#e1e4e8;min-height:100vh}
.container{max-width:1100px;margin:0 auto;padding:20px}
header{text-align:center;padding:30px 0 20px}
header h1{font-size:28px;font-weight:700;background:linear-gradient(135deg,#6366f1,#8b5cf6,#a855f7);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
header p{color:#8b949e;margin-top:6px;font-size:14px}
.grid{display:grid;grid-template-columns:460px 1fr;gap:20px}
@media(max-width:860px){.grid{grid-template-columns:1fr}}
.card{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:24px}
.card h2{font-size:16px;color:#e1e4e8;margin-bottom:16px;display:flex;align-items:center;gap:8px}
label{display:block;font-size:13px;color:#8b949e;margin-bottom:4px;margin-top:14px}
label:first-of-type{margin-top:0}
label.required::after{content:' *';color:#f85149}
input,textarea,select{width:100%;padding:10px 12px;border:1px solid #30363d;border-radius:8px;background:#0d1117;color:#e1e4e8;font-size:14px;transition:border-color .2s}
input:focus,textarea:focus,select:focus{outline:none;border-color:#6366f1}
textarea{resize:vertical;min-height:160px;font-family:inherit;line-height:1.6}
.row{display:flex;gap:12px}
.row>*{flex:1}
button{padding:12px 24px;border:none;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;transition:all .2s}
.btn-primary{background:linear-gradient(135deg,#6366f1,#8b5cf6);color:#fff;width:100%;margin-top:16px}
.btn-primary:hover{transform:translateY(-1px);box-shadow:0 4px 20px rgba(99,102,241,.35)}
.btn-primary:disabled{opacity:.5;cursor:not-allowed;transform:none;box-shadow:none}
.btn-danger{background:#da3633;color:#fff;width:100%;margin-top:8px;padding:12px;border:none;border-radius:8px;font-size:15px;cursor:pointer;transition:all .2s}
.btn-danger:hover{background:#f85149;transform:translateY(-1px)}
/* 分段视频卡片 */
.seg-card{background:#0d1117;border:1px solid #30363d;border-radius:8px;overflow:hidden;transition:border-color .2s}
.seg-card:hover{border-color:#6366f1}
.seg-card video{width:100%;display:block;max-height:200px;background:#000}
.seg-card .seg-label{padding:6px 10px;font-size:12px;color:#8b949e;display:flex;justify-content:space-between;align-items:center}
.seg-card .seg-label a{color:#58a6ff;text-decoration:none;font-size:11px}
.seg-card .seg-label a:hover{text-decoration:underline}
/* 我的视频 - 卡片画廊 */
.history-card{background:#161b22;border:1px solid #30363d;border-radius:6px;overflow:hidden;transition:transform .15s,box-shadow .15s;cursor:pointer;position:relative}
.history-card:hover{transform:translateY(-1px);box-shadow:0 4px 12px rgba(0,0,0,.4);border-color:#6366f1}
.history-card .thumb-wrap{position:relative;aspect-ratio:9/16;background:#0d1117;overflow:hidden}
.history-card video{width:100%;height:100%;object-fit:contain;display:block}
.history-card .card-overlay{position:absolute;bottom:0;left:0;right:0;padding:12px 6px 4px;background:linear-gradient(transparent,rgba(0,0,0,.75));pointer-events:none}
.history-card .card-info{display:flex;align-items:center;justify-content:space-between;padding:4px 6px;gap:4px}
.history-card .card-meta{font-size:9px;color:#8b949e;flex:1;min-width:0}
.history-card .card-meta .vid{font-size:9px;color:#e1e4e8;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.history-card .card-actions{display:flex;gap:2px;flex-shrink:0}
.btn-download{display:inline-flex;align-items:center;gap:6px;background:#238636;color:#fff;text-decoration:none;padding:10px 20px;border-radius:8px;font-size:14px;font-weight:600;margin-top:12px}
.btn-download:hover{background:#2ea043}
/* 步骤条 */
.steps{display:flex;justify-content:space-between;margin-bottom:24px;position:relative}
.steps::before{content:'';position:absolute;top:14px;left:30px;right:30px;height:2px;background:#30363d;z-index:0}
.step{display:flex;flex-direction:column;align-items:center;position:relative;z-index:1;flex:1}
.step-circle{width:30px;height:30px;border-radius:50%;background:#21262d;border:2px solid #30363d;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;transition:all .3s}
.step.active .step-circle{background:#6366f1;border-color:#6366f1;box-shadow:0 0 12px rgba(99,102,241,.4)}
.step.done .step-circle{background:#238636;border-color:#238636}
.step-label{font-size:11px;color:#8b949e;margin-top:6px;text-align:center}
.step.active .step-label,.step.done .step-label{color:#e1e4e8}
.step.pending .step-circle{opacity:.4}
/* 日志区 */
.log-area{background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:12px;height:300px;overflow-y:auto;font-family:"SF Mono",Menlo,monospace;font-size:12px;line-height:1.7}
.log-entry{color:#8b949e;margin-bottom:2px}
.log-entry .time{color:#484f58;margin-right:8px}
.log-entry.error{color:#f85149}
.log-entry.success{color:#3fb950}
/* 预览区 */
.preview-area{margin-top:16px;text-align:center}
.preview-area img,.preview-area video{max-width:100%;max-height:280px;border-radius:8px;border:1px solid #30363d}
/* 进度条 */
.progress-bar{height:4px;background:#21262d;border-radius:2px;margin-top:12px;overflow:hidden}
.progress-fill{height:100%;background:linear-gradient(90deg,#6366f1,#8b5cf6);border-radius:2px;transition:width .3s;width:0%}
.progress-label{font-size:12px;color:#8b949e;margin-top:4px;text-align:right}
/* Toast */
.toast{position:fixed;top:20px;right:20px;padding:12px 20px;border-radius:8px;font-size:13px;animation:slideIn .3s;z-index:1000}
.toast.error{background:#490202;color:#f85149;border:1px solid #f85149}
.toast.success{background:#04260f;color:#3fb950;border:1px solid #3fb950}
@keyframes slideIn{from{transform:translateX(100%);opacity:0}to{transform:translateX(0);opacity:1}}
/* 隐藏默认 */
.hidden{display:none!important}
.spinner{display:inline-block;width:14px;height:14px;border:2px solid #30363d;border-top-color:#6366f1;border-radius:50%;animation:spin .6s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
@keyframes fadeIn{from{opacity:0;transform:scale(0.95)}to{opacity:1;transform:scale(1)}}
@keyframes pulse{0%,100%{opacity:0.4}50%{opacity:0.8}}
.portrait-loading{
  width:100%;height:160px;border-radius:8px;border:2px dashed #30363d;
  display:flex;align-items:center;justify-content:center;
  background:linear-gradient(135deg,#0d1117,#161b22);
  animation:pulse 1.5s ease-in-out infinite;
}
.portrait-loading-inner{
  text-align:center;color:#8b949e;font-size:13px
}
.portrait-loading-inner .spinner{margin-bottom:8px;width:24px;height:24px}
</style>
</head>
<body>
<div class="container">
<header>
  <h1>🎬 koubo 口播成片控制台</h1>
  <p>AI 虚拟人口播视频 · 三步生成 · 火山引擎 ARK</p>
  <nav style="margin-top:12px;display:flex;gap:12px;justify-content:center">
    <button onclick="showTab('create')" id="nav-create" class="nav-btn active" style="padding:6px 16px;background:#6366f1;color:#fff;border:none;border-radius:6px;font-size:13px;cursor:pointer">📝 创作</button>
    <button onclick="showTab('history')" id="nav-history" class="nav-btn" style="padding:6px 16px;background:#21262d;color:#8b949e;border:1px solid #30363d;border-radius:6px;font-size:13px;cursor:pointer">📼 我的视频</button>
  </nav>
  <div id="tunnel-status-bar" style="margin-top:10px;display:flex;align-items:center;justify-content:center;gap:8px;font-size:12px;cursor:pointer" onclick="refreshTunnelStatus()">
    <span id="tunnel-status-dot" style="width:8px;height:8px;border-radius:50%;background:#484f58;display:inline-block"></span>
    <span id="tunnel-status-text" style="color:#484f58">隧道: 检测中...</span>
    <button onclick="event.stopPropagation();tunnelReconnect()" style="background:transparent;border:1px solid #30363d;color:#8b949e;border-radius:4px;padding:2px 8px;font-size:11px;cursor:pointer;display:none" id="tunnel-reconnect-btn">🔄 重连</button>
  </div>
</header>

<div class="grid">
  <!-- 左侧：表单 -->
  <div>
    <div id="tab-create">
    <div class="card" id="form-card">
      <h2>📝 新建口播视频</h2>
      <label>🤖 人物形象描述 <span style="font-size:11px;color:#8b949e">（描述或用下方上传本地照片）</span></label>
      <input id="character" placeholder="例：40岁中国男性，深蓝衬衫，亲和力强，温暖微笑...">
      
      <div style="display:flex;gap:8px;margin-top:8px">
        <div style="flex:1">
          <label style="font-size:11px;color:#8b949e;margin:0 0 4px">🌄 环境/背景</label>
          <input type="text" id="environment" list="env-presets" placeholder="干净纯色灰色背景，柔和摄影棚灯光" value="干净纯色灰色背景，柔和摄影棚灯光"
            oninput="saveDraft()" onchange="saveDraft()" 
            style="width:100%;padding:8px 10px;border:1px solid #30363d;border-radius:6px;background:#0d1117;color:#e1e4e8;font-size:12px">
          <datalist id="env-presets">
            <option value="干净纯色灰色背景，柔和摄影棚灯光">🎬 纯灰背景</option>
            <option value="现代简约办公室，落地窗自然光，书柜背景，专业氛围">🏢 现代办公室</option>
            <option value="虚拟演播室，科技感LED背景墙，蓝紫色调灯光，专业麦克风">📺 科技演播室</option>
            <option value="温暖家居环境，书架和绿植，柔和的暖色调灯光，舒适自然">🏠 温暖家居</option>
            <option value="明亮简洁的白色空间，极简风格，柔光，干净高级感">⬜ 极简白</option>
            <option value="户外城市天台，黄昏日落余晖，微风轻拂">🌇 城市天台</option>
            <option value="咖啡厅角落，暖黄灯光，绿植点缀，慵懒氛围">☕ 咖啡厅</option>
            <option value="开放式联合办公空间，年轻活力，明亮落地窗">💼 联合办公</option>
          </datalist>
        </div>
      </div>
      
      <div style="display:flex;gap:6px;margin-top:8px;flex-wrap:wrap">
        <span style="font-size:11px;color:#8b949e;line-height:24px">预设：</span>
        <button onclick="fillPreset('专业男讲师')" class="preset-btn" style="padding:3px 10px;background:#1f2937;color:#8b949e;border:1px solid #30363d;border-radius:12px;font-size:11px;cursor:pointer">👨‍🏫 专业讲师</button>
        <button onclick="fillPreset('年轻女主播')" class="preset-btn" style="padding:3px 10px;background:#1f2937;color:#8b949e;border:1px solid #30363d;border-radius:12px;font-size:11px;cursor:pointer">👩‍💼 亲切主播</button>
        <button onclick="fillPreset('中年商务男士')" class="preset-btn" style="padding:3px 10px;background:#1f2937;color:#8b949e;border:1px solid #30363d;border-radius:12px;font-size:11px;cursor:pointer">💼 商务男士</button>
        <button onclick="fillPreset('知性优雅女性')" class="preset-btn" style="padding:3px 10px;background:#1f2937;color:#8b949e;border:1px solid #30363d;border-radius:12px;font-size:11px;cursor:pointer">🌸 知性女士</button>
      </div>
      
      <!-- ⚙️ AI 模型配置面板 -->
      <div style="margin-top:10px;border:1px solid #21262d;border-radius:8px;overflow:hidden">
        <div onclick="toggleConfig()" style="display:flex;align-items:center;justify-content:space-between;padding:8px 12px;background:#161b22;cursor:pointer;user-select:none">
          <span style="font-size:12px;color:#8b949e">⚙️ AI 模型配置</span>
          <span id="config-arrow" style="color:#484f58;transition:transform .2s">▼</span>
        </div>
        <div id="config-body" class="hidden" style="padding:12px;display:flex;flex-direction:column;gap:8px">
          <div>
            <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:2px">API 地址</label>
            <input id="cfg-api-base" type="text" placeholder="https://ark.cn-beijing.volces.com/api/v3"
              style="width:100%;padding:6px 8px;border:1px solid #30363d;border-radius:4px;background:#0d1117;color:#e1e4e8;font-size:12px;font-family:monospace"
              onchange="saveConfig()">
          </div>
          <div>
            <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:2px">API Key</label>
            <input id="cfg-api-key" type="password" placeholder="你的 API 密钥"
              style="width:100%;padding:6px 8px;border:1px solid #30363d;border-radius:4px;background:#0d1117;color:#e1e4e8;font-size:12px;font-family:monospace"
              onchange="saveConfig()">
          </div>
          <div>
            <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:2px">模型名称</label>
            <input id="cfg-model" type="text" placeholder="doubao-seedream-5-0-260128"
              style="width:100%;padding:6px 8px;border:1px solid #30363d;border-radius:4px;background:#0d1117;color:#e1e4e8;font-size:12px;font-family:monospace"
              onchange="saveConfig()">
          </div>
          <div style="display:flex;gap:8px;align-items:center">
            <button onclick="testConnection()" id="btn-test-connection"
              style="flex:1;padding:8px;background:#1f6feb;color:#fff;border:1px solid #388bfd;border-radius:6px;font-size:12px;cursor:pointer;font-weight:600">
              🔍 测试连接
            </button>
            <span id="test-result" style="flex:2;font-size:11px;line-height:1.4"></span>
          </div>
          <div style="font-size:10px;color:#484f58;margin-top:4px">💡 支持任何兼容 OpenAI 接口的模型服务</div>
        </div>
      </div>
      
      <div style="display:flex;gap:8px;margin-top:8px">
        <button onclick="previewPortrait()" id="btn-preview-portrait" style="flex:2;padding:8px;background:#21262d;color:#e1e4e8;border:1px solid #30363d;border-radius:6px;font-size:12px;cursor:pointer">🖼 生成预览</button>
        <label style="flex:2;padding:8px;background:#21262d;color:#e1e4e8;border:1px solid #30363d;border-radius:6px;font-size:12px;cursor:pointer;text-align:center;display:flex;align-items:center;justify-content:center;gap:4px">
          📁 上传照片
          <input type="file" id="portrait-upload" accept="image/*" onchange="uploadPortrait()" style="display:none">
        </label>
        <button onclick="saveToPortraitLib()" style="flex:1;padding:8px;background:#21262d;color:#8b949e;border:1px solid #30363d;border-radius:6px;font-size:12px;cursor:pointer" title="保存当前形象">💾</button>
      </div>
      <div id="portrait-library" class="hidden" style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap"></div>
      <div id="server-portraits" style="margin-top:8px"></div>
      <div id="portrait-preview-area" class="hidden" style="margin-top:8px">
        <!-- 图片 + prompt 并排 -->
        <div style="display:flex;gap:8px;align-items:flex-start">
          <div style="flex:0 0 45%;min-width:0">
            <div id="portrait-loading-placeholder" class="portrait-loading">
              <div class="portrait-loading-inner">
                <div class="spinner"></div>
                <div id="portrait-loading-text">等待生成...</div>
              </div>
            </div>
            <img id="portrait-preview-img" src="" style="max-width:100%;max-height:200px;border-radius:8px;border:1px solid #30363d;display:none">
          </div>
          <div id="portrait-prompt-display" class="hidden" style="flex:1;min-width:0;max-height:200px;overflow-y:auto;padding:6px 8px;background:#161b22;border:1px solid #30363d;border-radius:6px;font-size:11px;color:#8b949e;line-height:1.5;white-space:pre-wrap;word-break:break-all"></div>
        </div>
        <div style="font-size:11px;color:#8b949e;margin-top:4px" id="portrait-preview-label"></div>
        <div style="display:flex;gap:8px;margin-top:8px">
          <button onclick="confirmPortrait()" class="hidden" id="btn-confirm-portrait" style="flex:2;padding:8px;background:linear-gradient(135deg,#238636,#2ea043);color:#fff;border:none;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer">✅ 使用此形象</button>
          <button onclick="previewPortrait()" class="hidden" id="btn-regenerate-portrait" style="flex:1;padding:8px;background:#21262d;color:#8b949e;border:1px solid #30363d;border-radius:6px;font-size:13px;cursor:pointer">🔄 重新生成</button>
        </div>
      </div>

      <label>📜 口播剧本 
        <span id="script-stats" style="font-size:11px;color:#8b949e;margin-left:8px">0字 · 预估 0秒</span>
        <button onclick="importScript()" style="float:right;background:#21262d;color:#8b949e;border:1px solid #30363d;border-radius:4px;padding:2px 8px;font-size:11px;cursor:pointer;margin-left:4px">📥 导入TXT</button>
        <button onclick="colloquialize()" id="btn-colloquialize" style="float:right;background:#341a00;color:#d2991d;border:1px solid #d2991d;border-radius:4px;padding:2px 8px;font-size:11px;cursor:pointer;margin-left:4px">💬 口语化</button>
        <input type="file" id="script-file-input" accept=".txt,.md" onchange="doImportScript(this)" style="display:none">
      </label>
      <textarea id="script" placeholder="粘贴或输入口播文案（支持多段落）..." onblur="previewSplit()" oninput="updateScriptStats()"></textarea>

      <!-- 分段编辑区（始终可见） -->
      <div id="preview-result" style="margin-top:10px;padding:10px;background:#0d1117;border-radius:8px;border:1px solid #30363d;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
          <div id="preview-summary" style="color:#8b949e;font-size:13px">📋 粘贴文案后自动分段</div>
          <div>
            <button id="btn-smart-segment" class="hidden" onclick="smartSegment()" style="background:#341a00;color:#d2991d;border:1px solid #d2991d;border-radius:6px;padding:5px 10px;font-size:11px;margin-right:4px;cursor:pointer">🤖 AI 加标点</button>
            <button id="btn-design-actions" onclick="designActions()" style="background:#1a0d33;color:#a371f7;border:1px solid #a371f7;border-radius:6px;padding:5px 10px;font-size:11px;margin-right:4px;cursor:pointer">🎭 AI 动作</button>
            <button onclick="addEmptySegment()" style="background:#0d3320;color:#3fb950;border:1px solid #3fb950;border-radius:6px;padding:5px 10px;font-size:11px;margin-right:4px;cursor:pointer">＋ 新增段</button>
            <button onclick="previewSplit()" style="background:#21262d;color:#8b949e;border:1px solid #30363d;border-radius:6px;padding:5px 10px;font-size:11px;cursor:pointer">🔄 重新切割</button>
          </div>
        </div>
        <div id="preview-segments" style="max-height:420px;overflow-y:auto"></div>
        <div id="quality-warning" style="margin-top:6px"></div>
      </div>

      <div class="row">
        <div><label>⏱ 每段时长(秒)</label><select id="duration"><option value="8">8秒</option><option value="10">10秒</option><option value="12" selected>12秒</option><option value="15">15秒（需2.0模型）</option></select></div>
        <div><label>🗣 语速 (字/秒)</label>
          <div style="display:flex;align-items:center;gap:4px">
            <input type="number" id="speech-rate" value="4.0" min="1.0" max="10.0" step="0.1" 
              style="width:70px;padding:10px 8px;border:1px solid #30363d;border-radius:8px;background:#0d1117;color:#e1e4e8;font-size:14px;text-align:center"
              onchange="updateScriptStats();previewSplit()">
            <span style="font-size:11px;color:#8b949e">字/秒</span>
          </div>
        </div>
      </div>
      <div class="row">
        <div><label>📐 画面比例</label><select id="ratio"><option value="9:16" selected>9:16 竖屏</option><option value="16:9">16:9 横屏</option><option value="1:1">1:1 方形</option></select></div>
      </div>
      <div style="display:flex;align-items:center;gap:8px;margin-top:10px">
        <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:#10b981;cursor:pointer" title="使用 Azure edge-tts 统一音色，多段声音100%一致">
          <input type="checkbox" id="external-tts" style="accent-color:#10b981"> 🎙️ 外部TTS统一音色
        </label>
        <select id="tts-voice" style="font-size:11px;padding:2px 4px;border-radius:4px;background:#21262d;color:#c9d1d9;border:1px solid #30363d;display:none">
          <optgroup label="👨 男声">
            <option value="zh-CN-YunxiNeural">Yunxi 阳光</option>
            <option value="zh-CN-YunjianNeural">Yunjian 激情</option>
            <option value="zh-CN-YunyangNeural">Yunyang 专业</option>
          </optgroup>
          <optgroup label="👩 女声">
            <option value="zh-CN-XiaoxiaoNeural">Xiaoxiao 温暖</option>
            <option value="zh-CN-XiaoyiNeural">Xiaoyi 活泼</option>
            <option value="zh-CN-YunxiaNeural">Yunxia 可爱</option>
          </optgroup>
          <optgroup label="🗣 方言">
            <option value="zh-CN-liaoning-XiaobeiNeural">辽宁 Xiaobei 东北话</option>
            <option value="zh-CN-shaanxi-XiaoniNeural">陕西 Xiaoni 陕西方言</option>
          </optgroup>
        </select>
      </div>

      <button class="btn-primary" id="btn-start" onclick="startPipeline()">🚀 开始生成</button>
      <button class="btn-danger hidden" id="btn-cancel" onclick="abortPipeline()">🛑 取消生成</button>
      
      <button class="btn-primary hidden" id="btn-confirm-generate" onclick="startPipeline()" style="margin-top:8px;background:linear-gradient(135deg,#238636,#2ea043)">✅ 确认无误，开始生成</button>
      <details style="margin-top:12px;font-size:12px;color:#8b949e">
        <summary style="cursor:pointer;padding:4px 0">📦 批量生成（CSV导入）</summary>
        <div style="margin-top:8px;padding:8px;background:#0d1117;border-radius:6px;border:1px solid #30363d">
          <p style="margin:0 0 6px">上传 CSV 文件（列：character, script），每行一条视频</p>
          <input type="file" id="batch-input" accept=".csv" onchange="handleBatchUpload(this)" style="font-size:12px">
          <div id="batch-preview" style="margin-top:6px;font-size:11px"></div>
        </div>
      </details>
    </div>
  </div>
</div>

  <!-- 历史记录页面 (hidden by default) -->
  <div id="tab-history" class="hidden" style="max-width:1100px;margin:0 auto">
    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
        <h2>📼 我的视频</h2>
        <button onclick="loadHistory()" style="padding:6px 12px;background:#21262d;color:#8b949e;border:1px solid #30363d;border-radius:6px;font-size:12px;cursor:pointer">🔄 刷新</button>
      </div>
      <div id="history-list" style="display:grid;grid-template-columns:repeat(auto-fill,140px);gap:8px;justify-content:start">
        <div style="color:#8b949e;text-align:center;padding:40px">加载中...</div>
      </div>
    </div>
  </div>

  <!-- 右侧：进度 / 历史 -->
  <div>
    <!-- 进度面板 -->
    <div class="card" id="progress-card">
      <h2>📊 生成进度</h2>

      <!-- 分段总览（创作时显示） -->
      <div id="segments-overview" style="margin-bottom:12px">
        <div id="overview-empty" style="color:#8b949e;font-size:13px;text-align:center;padding:32px 16px;border:1px dashed #30363d;border-radius:8px">
          📋 左侧粘贴文案后<br>这里展示所有分段的完整 Prompt 预览
        </div>
        <div id="overview-list" class="hidden" style="max-height:500px;overflow-y:auto"></div>
      </div>

      <!-- 步骤条（生成时显示） -->
      <div class="steps hidden" id="progress-steps">
        <div class="step" id="step1"><div class="step-circle">1</div><div class="step-label">🎨 定妆照</div></div>
        <div class="step" id="step2"><div class="step-circle">2</div><div class="step-label">🎬 分段视频</div></div>
        <div class="step" id="step3"><div class="step-circle">3</div><div class="step-label">🔗 拼接成片</div></div>
      </div>

      <!-- 进度条 -->
      <div class="progress-bar"><div class="progress-fill" id="progress-fill"></div></div>
      <div class="progress-label" id="progress-label">等待开始...【v2026.05.04-14:25】</div>

      <!-- 预览 -->
      <div class="preview-area hidden" id="preview-portrait">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
          <span style="font-size:13px;color:#e1e4e8;font-weight:600">🎨 定妆照</span>
        </div>
        <img id="portrait-img" src="" alt="定妆照">
      </div>
      
      <!-- 成片播放区 -->
      <div class="hidden" id="preview-download" style="margin-top:8px">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
          <span style="font-size:13px;color:#e1e4e8;font-weight:600">🎬 成片预览</span>
          <span id="video-duration" style="font-size:11px;color:#8b949e"></span>
        </div>
        <video id="final-video-player" controls style="width:100%;max-height:360px;border-radius:8px;background:#0d1117;border:1px solid #30363d" preload="metadata"></video>
        <div style="display:flex;gap:8px;margin-top:8px">
          <a class="btn-download" id="download-link" href="#" download style="flex:1;text-align:center;margin:0">⬇️ 下载成片</a>
        </div>
      </div>

      <!-- 分段视频预览（可折叠） -->
      <div class="hidden" id="segment-preview" style="margin-top:12px">
        <div onclick="document.getElementById('segment-grid').classList.toggle('hidden');var a=document.getElementById('seg-collapse-arrow');a.textContent=a.textContent==='▼'?'▶':'▼'"
          style="display:flex;align-items:center;justify-content:space-between;cursor:pointer;padding:4px 0;user-select:none">
          <h4 style="color:#e1e4e8;margin:0;font-size:13px;display:flex;align-items:center;gap:6px">
            <span id="seg-collapse-arrow" style="font-size:10px">▼</span> 🎞️ 分段预览
          </h4>
          <span id="seg-count" style="font-size:11px;color:#8b949e"></span>
        </div>
        <div id="segment-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:6px;margin-top:6px"></div>
      </div>

      <!-- 日志 -->
      <div class="log-area" id="log-area">
        <div class="log-entry"><span class="time">--:--:--</span> 就绪，填写表单后点击开始</div>
      </div>
    </div>
  </div>
</div>
</div>

<script>
let currentSession = null;
let eventSource = null;

function addLog(msg, cls) {
  const area = document.getElementById('log-area');
  const now = new Date().toTimeString().slice(0,8);
  const el = document.createElement('div');
  el.className = 'log-entry' + (cls ? ' ' + cls : '');
  el.innerHTML = '<span class="time">' + now + '</span>' + msg;
  area.appendChild(el);
  area.scrollTop = area.scrollHeight;
}

function updateStep(num, state) {
  const el = document.getElementById('step' + num);
  el.className = 'step ' + state;
}

function setProgress(pct, label) {
  document.getElementById('progress-fill').style.width = pct + '%';
  document.getElementById('progress-label').textContent = label || (pct + '%');
}


// 全局存储当前切割结果
let currentSegments = [];

async function previewSplit() {
  const script = document.getElementById('script').value.trim();
  if (!script) { addLog('⚠️ 请先粘贴口播剧本', 'error'); return; }
  
  const duration = parseInt(document.getElementById('duration').value);
  const rate = parseFloat(document.getElementById('speech-rate').value);
  
  addLog(`🔍 切割预览: 语速${rate}字/秒, 每段${duration}s`);
  
  try {
    const resp = await fetch('/api/preview_split', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({script, duration, speech_rate: rate})
    });
    const data = await resp.json();
    currentSegments = data.segments.map(s => ({...s, camera: s.camera || '镜头固定，正面拍摄'}));
    
    document.getElementById('preview-summary').innerHTML = 
      `📊 <b>${data.total_segments}</b> 段 · 预估 <b>${data.total_duration_est}</b>秒`;
    
    // 质量警告
    let warnHtml = '';
    const maxChars = duration * rate;
    if (!data.has_punctuation) {
      warnHtml = `<div style="padding:8px 12px;background:#490202;border-radius:6px;font-size:12px;color:#f85149;margin-bottom:8px">
        ⚠️ 文案缺少标点符号，建议点「🤖 AI 加标点」自动修复
      </div>`;
    }
    document.getElementById('quality-warning').innerHTML = warnHtml;
    
    // 渲染可编辑段落列表
    let html = '';
    data.segments.forEach((s, idx) => {
      html += renderSegmentRow(idx, s, maxChars);
    });
    document.getElementById('preview-segments').innerHTML = html;
    document.getElementById('btn-confirm-generate').classList.remove('hidden');
    document.getElementById('btn-start').classList.add('hidden');
    
    // AI 加标点按钮
    const smartBtn = document.getElementById('btn-smart-segment');
    if (!data.has_punctuation) {
      smartBtn.classList.remove('hidden');
    } else {
      smartBtn.classList.add('hidden');
    }
    
    addLog(`✅ ${data.total_segments}段 · ${data.total_duration_est}s`, 'success');
  } catch(e) {
    addLog('❌ 切割失败: ' + e.message, 'error');
  }
}

function renderSegmentRow(idx, s, maxChars) {
  const overLimit = s.char_count > maxChars * 1.2;
  const borderColor = overLimit ? '#d2991d' : '#6366f1';

  const segAction = s.action || "人物保持自然微笑，眼神看向镜头。";
  
  // 完整 prompt (用于 toggle 展示)
  const char = document.getElementById('character').value.trim() || '人物';
  const env = document.getElementById('environment').value.trim() || '干净背景，柔和自然光';
  const styleAnchor = char + '，' + env + '，' + (s.camera || '镜头固定，正面拍摄，无变焦，无镜头移动') + '，高清画质。';
  const fullPrompt = (styleAnchor + " " + segAction + " 口播：\"" + s.text + "\"").replace(/\\s+/g, ' ');
  const promptId = 'prompt-preview-' + idx;

  return `<div class="seg-row" data-idx="${idx}" style="margin-bottom:6px;padding:8px 10px;background:#161b22;border-radius:6px;border-left:3px solid ${borderColor}">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
        <div style="display:flex;align-items:center;gap:8px">
          <span style="color:#6366f1;font-weight:600;font-size:13px">段${s.index}</span>
          <span style="color:#8b949e;font-size:11px">${s.char_count}字</span>
          <input type="number" class="seg-duration" data-idx="${idx}" value="${s.duration}" 
            min="4" max="15" style="width:42px;padding:2px 4px;font-size:11px;border:1px solid #30363d;border-radius:4px;background:#0d1117;color:#e1e4e8;text-align:center">
          <span style="font-size:11px;color:#8b949e">秒</span>
          ${overLimit ? '<span style="color:#d2991d;font-size:10px">⚠️超长</span>' : ''}
        </div>
        <div style="display:flex;gap:4px;align-items:center">
          <button onclick="splitSegment(${idx})" style="background:none;border:none;color:#3fb950;cursor:pointer;font-size:16px;padding:0 3px;line-height:1" title="拆分为两段">＋</button>
          <button onclick="togglePrompt(${idx})" style="background:none;border:none;color:#58a6ff;cursor:pointer;font-size:12px;padding:2px 4px" title="查看完整prompt">📋</button>
          <button onclick="deleteSegment(${idx})" style="background:none;border:none;color:#f85149;cursor:pointer;font-size:14px;padding:2px 4px" title="删除此段">✕</button>
        </div>
      </div>
      <div style="display:flex;gap:6px;margin-bottom:4px">
        <input type="text" class="seg-action" data-idx="${idx}" value="${segAction.replace(/"/g, '&quot;')}"
          placeholder="动作/情绪..."
          style="flex:1;padding:4px 8px;font-size:11px;border:1px solid #30363d;border-radius:4px;background:#0d1117;color:#a5d6ff;font-family:inherit"
          oninput="onSegActionChange(${idx}, this.value)">
        <input type="text" class="seg-camera" data-idx="${idx}" value="${(s.camera || '镜头固定，正面拍摄').replace(/"/g, '&quot;')}"
          placeholder="运镜..."
          style="flex:1;padding:4px 8px;font-size:11px;border:1px solid #30363d;border-radius:4px;background:#0d1117;color:#7ee787;font-family:inherit"
          oninput="onSegCameraChange(${idx}, this.value)">
      </div>
      <textarea class="seg-text" data-idx="${idx}" rows="2" 
        style="width:100%;padding:6px 8px;font-size:12px;line-height:1.5;border:1px solid #30363d;border-radius:4px;background:#0d1117;color:#e1e4e8;resize:vertical;font-family:inherit"
        oninput="onSegTextChange(${idx}, this.value)">${s.text}</textarea>
      <div id="${promptId}" style="display:none;margin-top:4px;padding:6px 8px;background:#0d1117;border:1px dashed #30363d;border-radius:4px;font-size:11px;color:#8b949e;line-height:1.5;word-break:break-all">
        <span style="color:#6366f1">📋 完整 Prompt → Seedance:</span><br>
        <span style="color:#a5d6ff">${fullPrompt}</span>
      </div>
    </div>`;
}

function onSegTextChange(idx, newText) {
  if (idx < currentSegments.length) {
    currentSegments[idx].text = newText;
    currentSegments[idx].char_count = newText.length;
    // 同步刷新该段的 prompt 预览
    refreshPromptPreview(idx);
    updateSummary();
  }
}

function onSegActionChange(idx, newAction) {
  if (idx < currentSegments.length) {
    currentSegments[idx].action = newAction;
    refreshPromptPreview(idx);
  }
}

function onSegCameraChange(idx, newCamera) {
  if (idx < currentSegments.length) {
    currentSegments[idx].camera = newCamera;
    refreshPromptPreview(idx);
  }
}

// ⚙️ 模型配置面板
function toggleConfig() {
  const body = document.getElementById('config-body');
  const arrow = document.getElementById('config-arrow');
  if (body.classList.contains('hidden')) {
    body.classList.remove('hidden');
    body.style.display = 'flex';
    arrow.style.transform = 'rotate(180deg)';
  } else {
    body.classList.add('hidden');
    arrow.style.transform = '';
  }
}

function saveConfig() {
  localStorage.setItem('koubo_api_base', document.getElementById('cfg-api-base').value.trim());
  localStorage.setItem('koubo_api_key', document.getElementById('cfg-api-key').value.trim());
  localStorage.setItem('koubo_model', document.getElementById('cfg-model').value.trim());
}

function loadConfig() {
  document.getElementById('cfg-api-base').value = localStorage.getItem('koubo_api_base') || '';
  document.getElementById('cfg-api-key').value = localStorage.getItem('koubo_api_key') || '';
  document.getElementById('cfg-model').value = localStorage.getItem('koubo_model') || '';
}

async function testConnection() {
  const btn = document.getElementById('btn-test-connection');
  const result = document.getElementById('test-result');
  btn.disabled = true;
  btn.textContent = '⏳ 测试中...';
  result.innerHTML = '<span style="color:#d2991d">⏳ 正在测试 API 连接...</span>';

  const api_base = document.getElementById('cfg-api-base').value.trim();
  const api_key = document.getElementById('cfg-api-key').value.trim();
  const model = document.getElementById('cfg-model').value.trim();

  if (!api_key) {
    result.innerHTML = '<span style="color:#f85149">❌ 请先填写 API Key</span>';
    btn.disabled = false;
    btn.textContent = '🔍 测试连接';
    return;
  }

  try {
    const resp = await fetch('/api/test_connection', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({api_key, api_base, model})
    });
    const data = await resp.json();

    if (data.ok) {
      let html = '<span style="color:#3fb950">✅ 全部通过！</span><br>';
      for (const r of data.results) {
        const icon = r.ok ? '✅' : '❌';
        const extra = r.latency_ms ? ` <span style="color:#484f58">(${r.latency_ms}ms)</span>` : '';
        html += `<span style="color:#8b949e">${icon} ${r.step}${extra}</span><br>`;
      }
      result.innerHTML = html;
    } else if (data.results) {
      let html = '<span style="color:#f85149">❌ 测试未通过</span><br>';
      for (const r of data.results) {
        const icon = r.ok ? '✅' : '❌';
        html += `<span style="color:#8b949e">${icon} ${r.step}</span>`;
        if (r.error) html += ` <span style="color:#f85149">— ${r.error}</span>`;
        html += '<br>';
      }
      result.innerHTML = html;
    } else {
      result.innerHTML = `<span style="color:#f85149">❌ ${data.error}</span>`;
    }
  } catch(e) {
    result.innerHTML = `<span style="color:#f85149">⚠️ 请求失败: ${e.message}</span>`;
  }
  btn.disabled = false;
  btn.textContent = '🔍 测试连接';
}

// 页面加载时恢复配置
loadConfig();

function togglePrompt(idx) {
  const el = document.getElementById('prompt-preview-' + idx);
  if (el) el.style.display = el.style.display === 'none' ? 'block' : 'none';
}

function refreshPromptPreview(idx) {
  const el = document.getElementById('prompt-preview-' + idx);
  if (!el || idx >= currentSegments.length) return;
  const s = currentSegments[idx];
  const char = document.getElementById('character').value.trim() || '人物';
  const env = document.getElementById('environment').value.trim() || '干净背景，柔和自然光';
  const styleAnchor = char + '，' + env + '，' + (s.camera || '镜头固定，正面拍摄，无变焦，无镜头移动') + '，高清画质。';
  const segAction = s.action || "人物保持自然微笑，眼神看向镜头。";
  const fullPrompt = (styleAnchor + " " + segAction + " 口播：\"" + s.text + "\"").replace(/\\s+/g, ' ');
  el.innerHTML = '<span style="color:#6366f1">📋 完整 Prompt → Seedance:</span><br>' +
    '<span style="color:#a5d6ff">' + fullPrompt + '</span>';
}

function deleteSegment(idx) {
  currentSegments.splice(idx, 1);
  refreshAllSegments();
  addLog(`已删除段${idx+1}`, 'warning');
}

function splitSegment(idx) {
  if (idx >= currentSegments.length) return;
  const s = currentSegments[idx];
  const mid = Math.floor(s.text.length / 2);
  // 尽量在标点处断开
  let cut = mid;
  const puncts = ['。', '，', '？', '！', '；', '、', '.', ',', '?', '!', ';'];
  for (let i = mid; i < s.text.length - 3; i++) {
    if (puncts.includes(s.text[i])) { cut = i + 1; break; }
  }
  if (cut === mid) {
    for (let i = mid; i > 3; i--) {
      if (puncts.includes(s.text[i])) { cut = i + 1; break; }
    }
  }
  const part1 = s.text.slice(0, cut).trim();
  const part2 = s.text.slice(cut).trim();
  if (!part1 || !part2) return;
  
  s.text = part1;
  s.char_count = part1.length;
  const newSeg = {
    text: part2,
    char_count: part2.length,
    duration: s.duration,
    action: '',
    camera: s.camera || '镜头固定，正面拍摄',
    index: 0
  };
  currentSegments.splice(idx + 1, 0, newSeg);
  refreshAllSegments();
  addLog(`✂️ 段${idx+1} 拆分为两段`, 'success');
}

function addEmptySegment() {
  const newSeg = {
    text: '',
    char_count: 0,
    duration: parseInt(document.getElementById('duration').value) || 8,
    action: '',
    camera: '镜头固定，正面拍摄',
    index: 0
  };
  currentSegments.push(newSeg);
  refreshAllSegments();
  addLog('＋ 新增空段', 'success');
}

function refreshAllSegments() {
  currentSegments.forEach((s, i) => s.index = i + 1);
  const maxChars = parseInt(document.getElementById('duration').value) * parseFloat(document.getElementById('speech-rate').value);
  let html = '';
  currentSegments.forEach((s, i) => { html += renderSegmentRow(i, s, maxChars); });
  document.getElementById('preview-segments').innerHTML = html;
  updateSummary();
  renderOverview();
}

function renderOverview() {
  const list = document.getElementById('overview-list');
  const empty = document.getElementById('overview-empty');
  if (!currentSegments.length) {
    list.classList.add('hidden');
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');
  list.classList.remove('hidden');
  
  const char = document.getElementById('character').value.trim() || '(未设置人物)';
  const env = document.getElementById('environment').value.trim() || '干净背景，柔和自然光';
  const totalDur = currentSegments.reduce((sum, s) => sum + s.duration, 0);
  
  let html = `<div style="font-size:12px;color:#8b949e;margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid #21262d">
    共 <b style="color:#e1e4e8">${currentSegments.length}</b> 段 · 预估 <b style="color:#e1e4e8">${totalDur}</b>秒
  </div>`;
  
  currentSegments.forEach((s, i) => {
    const camera = s.camera || '镜头固定，正面拍摄，无变焦，无镜头移动';
    const action = s.action || '人物保持自然微笑，眼神看向镜头。';
    const styleAnchor = char + '，' + env + '，' + camera + '，高清画质。';
    const fullPrompt = (styleAnchor + ' ' + action + ' 口播："' + s.text + '"').replace(/\\s+/g, ' ');
    
    html += `<div style="margin-bottom:8px;padding:8px 10px;background:#0d1117;border-radius:6px;border-left:3px solid #6366f1">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
        <span style="color:#6366f1;font-weight:600;font-size:12px">段${s.index}</span>
        <span style="color:#8b949e;font-size:10px">${s.char_count}字 · ${s.duration}s</span>
      </div>
      <div style="font-size:10px;color:#a5d6ff;margin-bottom:3px;line-height:1.4">${action.slice(0, 80)}${action.length>80?'…':''}</div>
      <div style="font-size:9px;color:#484f58;line-height:1.3;word-break:break-all">${fullPrompt.slice(0, 120)}${fullPrompt.length>120?'…':''}</div>
    </div>`;
  });
  list.innerHTML = html;
}

// 监听语速和时长变化，更新字数统计
document.getElementById('speech-rate').addEventListener('change', updateScriptStats);
document.getElementById('duration').addEventListener('change', updateScriptStats);
document.getElementById('script').addEventListener('input', updateScriptStats);

// TTS 开关 → 显示/隐藏音色选择器
document.getElementById('external-tts').addEventListener('change', function() {
  document.getElementById('tts-voice').style.display = this.checked ? 'inline-block' : 'none';
  if (this.checked) {
    addLog('🎙️ 已启用外部TTS (edge-tts)，多段音色将100%一致', 'info');
  } else {
    addLog('使用 Seedance 内置语音 (多段音色可能有细微差异)', 'info');
  }
});

// 监听全局 duration 输入变化，刷新所有段
document.getElementById('duration').addEventListener('change', () => {
  const newDur = parseInt(document.getElementById('duration').value);
  currentSegments.forEach(s => { s.duration = newDur; });
  const maxChars = newDur * parseFloat(document.getElementById('speech-rate').value);
  let html = '';
  currentSegments.forEach((s, i) => { html += renderSegmentRow(i, s, maxChars); });
  document.getElementById('preview-segments').innerHTML = html;
  updateSummary();
});

// 监听每段 duration 的手动修改
document.addEventListener('input', e => {
  if (e.target.classList.contains('seg-duration')) {
    const idx = parseInt(e.target.dataset.idx);
    if (idx < currentSegments.length) {
      currentSegments[idx].duration = parseInt(e.target.value) || 12;
      updateSummary();
    }
  }
});




const CHARACTER_PRESETS = {
  '专业男讲师': '40岁中国男性，深蓝衬衫，亲和力强，温暖微笑，干净短发，专业形象',
  '年轻女主播': '28岁中国女性，长发淡妆，白色衬衫，温暖亲切的微笑，知性优雅',
  '中年商务男士': '45岁中国男性，灰色西装，稳重端庄，自信微笑，成功商务人士形象',
  '知性优雅女性': '35岁中国女性，及肩短发，淡雅妆容，米色针织衫，温柔知性'
};

function fillPreset(key) {
  const desc = CHARACTER_PRESETS[key];
  document.getElementById('character').value = desc;
  addLog('📋 已选择：' + key, 'success');
}

function updateScriptStats() {
  const text = document.getElementById('script').value;
  const charCount = text.length;
  const rate = parseFloat(document.getElementById('speech-rate').value);
  const estSec = Math.round(charCount / rate);
  const estMin = Math.floor(estSec / 60);
  const estRem = estSec % 60;
  const timeStr = estMin > 0 ? estMin + '分' + estRem + '秒' : estSec + '秒';
  document.getElementById('script-stats').textContent = charCount + '字 · 预估 ' + timeStr;
}

function importScript() {
  document.getElementById('script-file-input').click();
}

function doImportScript(input) {
  const file = input.files[0];
  if (!file) return;
  addLog('📥 读取文件: ' + file.name);
  const reader = new FileReader();
  reader.onload = function(e) {
    document.getElementById('script').value = e.target.result;
    updateScriptStats();
    previewSplit();
    addLog('✅ 文件导入完成: ' + file.name, 'success');
  };
  reader.readAsText(file, 'UTF-8');
}


// ====== 人物形象库 (localStorage) ======
function saveToPortraitLib() {
  const char = document.getElementById('character').value.trim();
  if (!char) { addLog('⚠️ 请先填写人物描述', 'error'); return; }
  
  let lib = JSON.parse(localStorage.getItem('koubo_portrait_lib') || '[]');
  if (lib.length >= 10) { addLog('⚠️ 形象库已满（最多10个）', 'error'); return; }
  
  const imgEl = document.getElementById('portrait-preview-img');
  const portraitUrl = (imgEl && imgEl.src && imgEl.src !== window.location.href) ? imgEl.src : null;
  
  const exists = lib.findIndex(item => item.character === char);
  if (exists >= 0) {
    if (portraitUrl) lib[exists].portraitUrl = portraitUrl;
    lib[exists].added = Date.now();
  } else {
    lib.push({character: char, portraitUrl: portraitUrl, added: Date.now()});
  }
  
  localStorage.setItem('koubo_portrait_lib', JSON.stringify(lib));
  addLog('💾 已保存到形象库' + (portraitUrl ? '（含预览图）' : ''), 'success');
  renderPortraitLib();
}

function autoSaveToLib(url) {
  const char = document.getElementById('character').value.trim();
  if (!char || !url) return;
  
  let lib = JSON.parse(localStorage.getItem('koubo_portrait_lib') || '[]');
  const exists = lib.findIndex(item => item.character === char);
  if (exists >= 0) {
    lib[exists].portraitUrl = url;
    lib[exists].added = Date.now();
  } else if (lib.length < 10) {
    lib.push({character: char, portraitUrl: url, added: Date.now()});
  }
  localStorage.setItem('koubo_portrait_lib', JSON.stringify(lib));
}

function renderPortraitLib() {
  const lib = JSON.parse(localStorage.getItem('koubo_portrait_lib') || '[]');
  const container = document.getElementById('portrait-library');
  if (!container) return;
  if (lib.length === 0) {
    container.classList.add('hidden');
    return;
  }
  container.classList.remove('hidden');
    container.innerHTML = '<div style="font-size:11px;color:#8b949e;margin-bottom:6px">🖼 形象库：</div>' +
    lib.map((item, idx) => {
      const imgSrc = item.portraitUrl || '';
      const imgHtml = imgSrc 
        ? '<img src="' + imgSrc + '" style="width:40px;height:40px;object-fit:cover;border-radius:4px;border:1px solid #30363d;margin-right:6px">' 
        : '<div style="width:40px;height:40px;background:#21262d;border-radius:4px;border:1px solid #30363d;display:flex;align-items:center;justify-content:center;margin-right:6px;font-size:16px">👤</div>';
      return '<div onclick="usePortraitFromLib(' + idx + ')" style="display:flex;align-items:center;padding:6px;background:#161b22;border:1px solid #30363d;border-radius:6px;margin-bottom:4px;cursor:pointer" title="' + item.character + '">'
        + imgHtml
        + '<div style="flex:1;min-width:0"><div style="font-size:11px;color:#e1e4e8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">' + item.character.slice(0,25) + '</div><div style="font-size:10px;color:#484f58">' + new Date(item.added).toLocaleDateString('zh-CN') + '</div></div>'
        + '<span onclick="event.stopPropagation();removeFromPortraitLib(' + idx + ')" style="color:#f85149;cursor:pointer;padding:4px;font-size:14px" title="删除">✕</span></div>';
    }).join('');
}


async function loadServerPortraits() {
  const container = document.getElementById('server-portraits');
  if (!container) return;
  try {
    const resp = await fetch('/api/portraits');
    const data = await resp.json();
    if (!data.portraits || data.portraits.length === 0) return;
    
    let html = '<div style="font-size:11px;color:#8b949e;margin-bottom:6px">🖥 服务器形象库：</div><div style="display:flex;gap:6px;flex-wrap:wrap">';
    data.portraits.forEach(p => {
      const label = p.character ? p.character.slice(0, 20) : p.filename;
      const safeUrl = p.url.replace(/'/g, "\\'");
      const safeChar = (p.character||'').replace(/'/g, "\\'");
      const hasDesc = p.ai_desc && p.ai_desc.length > 0;
      html += `<div style="position:relative;cursor:pointer;text-align:center" title="${p.ai_desc || p.filename}">
        <img src="${p.url}" onclick="useServerPortrait('${safeUrl}', '${safeChar}')"
          style="width:48px;height:48px;object-fit:cover;border-radius:4px;border:1px solid #30363d;${hasDesc?'border-color:#6366f1':''}" onerror="this.style.display='none'">
        <button onclick="event.stopPropagation();deleteServerPortrait('${p.filename}')"
          style="position:absolute;top:-4px;right:-4px;width:16px;height:16px;background:#490202;color:#f85149;border:none;border-radius:50%;font-size:10px;line-height:16px;cursor:pointer;padding:0;display:flex;align-items:center;justify-content:center">✕</button>
        <div style="font-size:9px;color:#8b949e;max-width:48px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${label}</div>
        ${hasDesc ? `<div style="font-size:8px;color:#6366f1;max-width:48px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;margin-top:1px">✨ AI</div>` : ''}
      </div>`;
    });
    html += '</div>';
    container.innerHTML = html;
  } catch(e) {
    container.innerHTML = '';
  }
}

function useServerPortrait(url, character) {
  // 隐藏加载占位符
  const placeholder = document.getElementById('portrait-loading-placeholder');
  if (placeholder) placeholder.style.display = 'none';
  
  const imgEl = document.getElementById('portrait-preview-img');
  imgEl.src = url;
  imgEl.style.display = 'block';
  document.getElementById('portrait-preview-area').classList.remove('hidden');
  document.getElementById('portrait-preview-label').textContent = '🖥 服务器 · ' + (character || '定妆照');
  if (character) {
    document.getElementById('character').value = character;
  }
  uploadedPortraitUrl = url;
  addLog('📸 已选用服务器形象' + (character ? ': ' + character.slice(0,20) : ''), 'success');
}

async function deleteServerPortrait(filename) {
  if (!confirm('删除这张形象照？')) return;
  try {
    const resp = await fetch('/api/portraits/' + encodeURIComponent(filename), { method: 'DELETE' });
    const data = await resp.json();
    if (data.error) throw new Error(data.error);
    addLog('🗑 已删除: ' + filename, 'warning');
    loadServerPortraits();
  } catch(e) {
    addLog('❌ 删除失败: ' + e.message, 'error');
  }
}


function usePortraitFromLib(idx) {
  const lib = JSON.parse(localStorage.getItem('koubo_portrait_lib') || '[]');
  if (idx < lib.length) {
    const item = lib[idx];
    document.getElementById('character').value = item.character;
    if (item.portraitUrl) {
      document.getElementById('portrait-preview-img').src = item.portraitUrl;
      document.getElementById('portrait-preview-label').textContent = '形象库 · ' + item.character.slice(0,15) + '...';
      document.getElementById('portrait-preview-area').classList.remove('hidden');
      document.getElementById('btn-confirm-portrait').classList.remove('hidden');
      document.getElementById('btn-regenerate-portrait').classList.remove('hidden');
    }
    addLog('📋 已选择形象库人物' + (item.portraitUrl ? '（含预览图）' : ''), 'success');
  }
}

function removeFromPortraitLib(idx) {
  let lib = JSON.parse(localStorage.getItem('koubo_portrait_lib') || '[]');
  lib.splice(idx, 1);
  localStorage.setItem('koubo_portrait_lib', JSON.stringify(lib));
  renderPortraitLib();
}



let uploadedPortraitUrl = null;

async function previewPortrait() {
  const character = document.getElementById('character').value.trim();
  if (!character) { addLog('⚠️ 请先填写人物形象描述', 'error'); return; }

  const btn = document.getElementById('btn-preview-portrait');
  const area = document.getElementById('portrait-preview-area');
  const img = document.getElementById('portrait-preview-img');
  const label = document.getElementById('portrait-preview-label');

  // 显示预览区域 + 加载状态
  area.classList.remove('hidden');
  img.style.display = 'none';
  img.src = '';
  document.getElementById('portrait-loading-placeholder').style.display = '';
  document.getElementById('portrait-prompt-display').classList.add('hidden');
  label.textContent = '⏳ 正在连接 AI 模型...';
  label.style.color = '#d2991d';
  document.getElementById('btn-confirm-portrait').classList.add('hidden');
  document.getElementById('btn-regenerate-portrait').classList.add('hidden');

  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> 生成中...';
  addLog('🖼 AI 生成肖像中，请稍候（约 30-60 秒）...');

  // 进度模拟：定时更新状态文案
  const startTime = Date.now();
  const loadingText = document.getElementById('portrait-loading-text');
  const progressTimer = setInterval(() => {
    const elapsed = Math.floor((Date.now() - startTime) / 1000);
    if (elapsed < 10) { label.textContent = '⏳ AI 正在理解人物描述...'; loadingText.textContent = '理解描述中...'; }
    else if (elapsed < 25) { label.textContent = '🎨 AI 正在绘制肖像... (' + elapsed + 's)'; loadingText.textContent = '绘制中... (' + elapsed + 's)'; }
    else if (elapsed < 50) { label.textContent = '🖌️ 正在细化细节... (' + elapsed + 's)'; loadingText.textContent = '细化中... (' + elapsed + 's)'; }
    else { label.textContent = '⏳ 仍在处理中，请耐心等待... (' + elapsed + 's)'; loadingText.textContent = '处理中... (' + elapsed + 's)'; }
  }, 3000);

  try {
    // 读取用户自定义 API 配置
    const api_key = localStorage.getItem('koubo_api_key') || '';
    const model = localStorage.getItem('koubo_model') || '';
    const api_base = localStorage.getItem('koubo_api_base') || '';

    const resp = await fetch('/api/preview_portrait', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        character,
        environment: document.getElementById('environment').value,
        api_key, model, api_base
      })
    });
    const data = await resp.json();

    clearInterval(progressTimer);

    if (data.error) {
      addLog('❌ ' + data.error, 'error');
      document.getElementById('portrait-loading-placeholder').style.display = 'none';
      label.textContent = '❌ 生成失败: ' + data.error;
      label.style.color = '#f85149';
      btn.disabled = false;
      btn.innerHTML = '🖼 重试生成';
      return;
    }

    // 显示生成结果
    document.getElementById('portrait-loading-placeholder').style.display = 'none';
    img.src = data.url;
    img.style.display = '';
    img.style.animation = 'fadeIn 0.5s ease';
    label.textContent = '✅ AI生成预览 · ' + (data.size_kb || '?') + 'KB';
    label.style.color = '#3fb950';
    // 在图片旁边展示 Seedream prompt
    if (data.prompt) {
      const pd = document.getElementById('portrait-prompt-display');
      pd.textContent = data.prompt;
      pd.classList.remove('hidden');
      pd.style.color = '#a371f7';
    }
    document.getElementById('btn-confirm-portrait').classList.remove('hidden');
    document.getElementById('btn-regenerate-portrait').classList.remove('hidden');
    autoSaveToLib(data.url);
    renderPortraitLib();
    addLog('✅ 肖像生成完成，点击「使用此形象」确认', 'success');
    btn.disabled = false;
    btn.innerHTML = '🖼 重新生成';
  } catch(e) {
    clearInterval(progressTimer);
    addLog('❌ 网络错误: ' + e.message, 'error');
    document.getElementById('portrait-loading-placeholder').style.display = 'none';
    label.textContent = '⚠️ 请求失败: ' + e.message;
    label.style.color = '#f85149';
    btn.disabled = false;
    btn.innerHTML = '🖼 重试生成';
  }
}

async function uploadPortrait() {
  const file = document.getElementById('portrait-upload').files[0];
  if (!file) return;
  
  addLog('📁 上传本地照片: ' + file.name);
  
  const formData = new FormData();
  formData.append('portrait', file);
  
  try {
    const resp = await fetch('/api/upload_portrait', {
      method: 'POST',
      body: formData
    });
    const data = await resp.json();
    
    if (data.error) {
      addLog('❌ ' + data.error, 'error');
      return;
    }
    
    uploadedPortraitUrl = data.url;
    document.getElementById('portrait-preview-img').src = data.url;
    document.getElementById('portrait-preview-label').textContent = '本地上传 · ' + file.name;
    document.getElementById('portrait-preview-area').classList.remove('hidden');
    document.getElementById('btn-confirm-portrait').classList.remove('hidden');
    document.getElementById('btn-regenerate-portrait').classList.add('hidden');
    autoSaveToLib(data.url);
    renderPortraitLib();
    addLog('✅ 照片上传成功', 'success');
    
    // 自动 AI 识别生成人物描述
    addLog('🔍 AI 正在识别照片...');
    try {
      const descResp = await fetch('/api/describe_portrait', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({url: data.url})
      });
      const descData = await descResp.json();
      if (descData.description) {
        document.getElementById('character').value = descData.description;
        // 在图片旁边展示生成的 prompt
        const pd = document.getElementById('portrait-prompt-display');
        pd.textContent = descData.description;
        pd.classList.remove('hidden');
        pd.style.color = '#7ee787';
        addLog('✨ AI 已自动生成人物描述并应用', 'success');
        // 自动确认形象，直接用于 Seedance
        uploadedPortraitUrl = data.url;
        const imgEl = document.getElementById('portrait-preview-img');
        imgEl.src = data.url;
        imgEl.style.display = 'block';
        document.getElementById('portrait-preview-area').style.border = '2px solid #238636';
        document.getElementById('portrait-preview-label').textContent = '✨ AI 识别 · 已自动应用';
        document.getElementById('btn-confirm-portrait').classList.add('hidden');
        document.getElementById('btn-regenerate-portrait').classList.remove('hidden');
      }
    } catch(e) {
      addLog('⚠️ AI 识别跳过: ' + e.message, 'warn');
    }
  } catch(e) {
    addLog('❌ 上传失败: ' + e.message, 'error');
  }
}



function confirmPortrait() {
  const btn = document.getElementById('btn-confirm-portrait');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> 已确认';
  btn.style.background = '#213a27';
  
  // 高亮预览框表示已确认
  document.getElementById('portrait-preview-area').style.border = '2px solid #238636';
  document.getElementById('portrait-preview-label').textContent += ' · ✅ 已确认';
  
  // 如果没有上传本地照片，标记为使用AI生成的 URL
  if (!uploadedPortraitUrl) {
    // previewPortrait 生成的图片，URL 是 /api/download/previews/... 
    const img = document.getElementById('portrait-preview-img');
    if (img && img.src) {
      uploadedPortraitUrl = img.src;
    }
  }
  
  addLog('✅ 人物形象已确认，可以开始生成', 'success');
}

function updateSummary() {
  const total = currentSegments.reduce((sum, s) => sum + (s.duration || 12), 0);
  document.getElementById('preview-summary').innerHTML = 
    `📊 <b>${currentSegments.length}</b> 段 · 预估 <b>${total}</b>秒`;
}

async function smartSegment() {
  const script = document.getElementById('script').value.trim();
  if (!script) { addLog('⚠️ 请先粘贴口播剧本', 'error'); return; }
  
  const btn = document.getElementById('btn-smart-segment');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> 处理中...';
  addLog('🤖 AI 加标点中...');
  
  try {
    const duration = parseInt(document.getElementById('duration').value);
    const rate = parseFloat(document.getElementById('speech-rate').value);
    
    const resp = await fetch('/api/smart_segment', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({script, duration, speech_rate: rate})
    });
    const data = await resp.json();
    
    if (data.error) {
      addLog('❌ ' + data.error, 'error');
      btn.disabled = false;
      btn.innerHTML = '🤖 AI 加标点';
      return;
    }
    
    // 更新文本框
    document.getElementById('script').value = data.punctuated_text;
    currentSegments = data.segments.map(s => ({...s, camera: s.camera || '镜头固定，正面拍摄'}));
    
    addLog('✅ AI已加标点, ' + data.total_segments + '段', 'success');
    
    // 重新渲染预览
    const maxChars = duration * rate;
    let html = '';
    data.segments.forEach((s, idx) => { html += renderSegmentRow(idx, s, maxChars); });
    document.getElementById('preview-segments').innerHTML = html;
    document.getElementById('quality-warning').innerHTML = '';
    document.getElementById('preview-summary').innerHTML = 
      `📊 <b>${data.total_segments}</b> 段 · 预估 <b>${data.total_duration_est}</b>秒 · ✅ AI已优化`;
    
    btn.classList.add('hidden');
    btn.disabled = false;
    btn.innerHTML = '🤖 AI 加标点';
  } catch(e) {
    addLog('❌ AI分段失败: ' + e.message, 'error');
    btn.disabled = false;
    btn.innerHTML = '🤖 AI 加标点';
  }
}


async function designActions() {
  const script = document.getElementById('script').value.trim();
  if (!script || currentSegments.length === 0) {
    addLog('⚠️ 请先粘贴剧本并确认分段', 'error');
    return;
  }

  const btn = document.getElementById('btn-design-actions');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> 设计中...';
  addLog('🎭 AI 正在为每段设计动作...');

  try {
    const character = document.getElementById('character').value.trim() || '人物';
    const resp = await fetch('/api/design_actions', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        script,
        character,
        segments: currentSegments.map(s => ({text: s.text, duration: s.duration}))
      })
    });
    const data = await resp.json();

    if (data.error) {
      addLog('❌ ' + data.error, 'error');
      btn.disabled = false;
      btn.innerHTML = '🎭 AI 动作设计';
      return;
    }

    // 更新每段的 action
    (data.actions || []).forEach((action, i) => {
      if (currentSegments[i]) currentSegments[i].action = action;
    });

    addLog(`✅ 已为 ${data.actions.length} 段设计动作`, 'success');

    // 重新渲染预览
    const duration = parseInt(document.getElementById('duration').value);
    const rate = parseFloat(document.getElementById('speech-rate').value);
    const maxChars = duration * rate;
    let html = '';
    currentSegments.forEach((s, i) => { html += renderSegmentRow(i, s, maxChars); });
    document.getElementById('preview-segments').innerHTML = html;
  } catch(e) {
    addLog('❌ 动作设计失败: ' + e.message, 'error');
  }
  btn.disabled = false;
  btn.innerHTML = '🎭 AI 动作设计';
}


async function startPipeline() {
  // uploadedPortraitUrl declared globally above
  const character = document.getElementById('character').value.trim();
  let script = document.getElementById('script').value.trim();
  if (!character && !uploadedPortraitUrl) {
    addLog('❌ 请填写人物描述或上传/预览照片后点击「使用此形象」', 'error');
    return;
  }
  if (!script) {
    addLog('❌ 请粘贴口播剧本', 'error');
    return;
  }
  
  // 如果有手动编辑过的分段，用分段文本拼回完整剧本
  if (currentSegments.length > 0) {
    script = currentSegments.map(s => s.text).join('');
  }

  const btn = document.getElementById('btn-start');
  const confirmBtn = document.getElementById('btn-confirm-generate');
  if (confirmBtn) { confirmBtn.disabled = true; confirmBtn.innerHTML = '<span class="spinner"></span> 提交中...'; }
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> 提交中...'; }
  document.getElementById('btn-cancel').classList.remove('hidden');
  addLog('📤 提交任务...');

  try {
    const resp = await fetch('/api/start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        character, script,
        duration: parseInt(document.getElementById('duration').value),
        ratio: document.getElementById('ratio').value,
        speech_rate: parseFloat(document.getElementById('speech-rate').value),
        custom_segments: currentSegments.length > 0 ? currentSegments.map(s => ({text: s.text, duration: s.duration, action: s.action || ""})) : null,
        portrait_url: uploadedPortraitUrl,
        environment: document.getElementById('environment').value,
        external_tts: document.getElementById('external-tts').checked,
        tts_voice: document.getElementById('tts-voice').value,
      })
    });
    const data = await resp.json();
    if (data.error) throw new Error(data.error);

    currentSession = data.session_id;
    addLog('✅ 任务已启动: ' + currentSession);
    // 切换到进度模式
    document.getElementById('segments-overview').classList.add('hidden');
    document.getElementById('progress-steps').classList.remove('hidden');
    updateStep(1, 'active');
    setProgress(0, '0%');

    // 连接 SSE
    connectSSE(currentSession);
  } catch (e) {
    addLog('❌ ' + e.message, 'error');
    btn.disabled = false;
    btn.innerHTML = '🚀 开始生成';
  }
}

async function abortPipeline() {
  if (!currentSession) return;
  const btn = document.getElementById('btn-cancel');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> 取消中...';
  try {
    await fetch('/api/abort/' + currentSession, { method: 'POST' });
    addLog('🛑 已请求取消生成', 'warn');
  } catch(e) {
    addLog('❌ 取消失败: ' + e.message, 'error');
  }
  // 立即重置所有按钮状态，不等待 SSE 回传
  const startBtn = document.getElementById('btn-start');
  const confirmBtn = document.getElementById('btn-confirm-generate');
  if (startBtn) { startBtn.disabled = false; startBtn.innerHTML = '🚀 开始生成'; }
  if (confirmBtn) { confirmBtn.disabled = false; confirmBtn.innerHTML = '✅ 确认无误，开始生成'; }
  btn.disabled = false;
  btn.innerHTML = '🛑 取消生成';
  btn.classList.add('hidden');
}


async function loadSegments(sid) {
  try {
    const resp = await fetch('/api/segments/' + sid);
    const data = await resp.json();
    if (!data.segments || data.segments.length === 0) return;
    
    const grid = document.getElementById('segment-grid');
    let html = '';
    data.segments.forEach(seg => {
      html += `<div class="seg-card">
        <video controls preload="metadata" src="${seg.url}"></video>
        <div class="seg-label">
          <span>${seg.name}</span>
          <a href="${seg.url}" download>⬇️ ${seg.size_kb}KB</a>
        </div>
      </div>`;
    });
    grid.innerHTML = html;
    document.getElementById('segment-preview').classList.remove('hidden');
    const cnt = document.getElementById('seg-count');
    if (cnt) cnt.textContent = `${data.total} 段`;
    addLog(`🎞️ 已加载 ${data.total} 个分段视频`, 'success');
  } catch(e) {
    addLog('⚠️ 分段加载失败: ' + e.message, 'warn');
  }
}


// ====== 草稿保存 ======
function saveDraft() {
  const draft = {
    character: document.getElementById('character').value,
    script: document.getElementById('script').value,
    duration: document.getElementById('duration').value,
    speechRate: document.getElementById('speech-rate').value,
    ratio: document.getElementById('ratio').value,
    environment: document.getElementById('environment').value,
    timestamp: Date.now()
  };
  localStorage.setItem('koubo_draft', JSON.stringify(draft));
}

function loadDraft() {
  const raw = localStorage.getItem('koubo_draft');
  if (!raw) return false;
  try {
    const draft = JSON.parse(raw);
    if (draft.character) document.getElementById('character').value = draft.character;
    if (draft.script) { 
      document.getElementById('script').value = draft.script; 
      updateScriptStats();
    }
    if (draft.duration) document.getElementById('duration').value = draft.duration;
    if (draft.speechRate) document.getElementById('speech-rate').value = draft.speechRate;
    if (draft.ratio) document.getElementById('ratio').value = draft.ratio;
    if (draft.environment) document.getElementById('environment').value = draft.environment;
    addLog('📂 已恢复上次草稿', 'success');
    return true;
  } catch(e) { return false; }
}

// Auto-save on input change
let draftSaveTimer;
function autoSaveDraft() {
  clearTimeout(draftSaveTimer);
  draftSaveTimer = setTimeout(saveDraft, 1000);
}
document.addEventListener('input', autoSaveDraft);
document.addEventListener('change', autoSaveDraft);

// ====== Tab 切换 ======
function showTab(tab) {
  const createTab = document.getElementById('tab-create');
  const historyTab = document.getElementById('tab-history');
  const progressCard = document.getElementById('progress-card');
  
  if (tab === 'create') {
    if (createTab) createTab.classList.remove('hidden');
    if (historyTab) historyTab.classList.add('hidden');
    if (progressCard) progressCard.classList.remove('hidden');
    document.getElementById('nav-create').className = 'nav-btn active';
    document.getElementById('nav-create').style.background = '#6366f1';
    document.getElementById('nav-create').style.color = '#fff';
    document.getElementById('nav-history').className = 'nav-btn';
    document.getElementById('nav-history').style.background = '#21262d';
    document.getElementById('nav-history').style.color = '#8b949e';
  } else {
    if (createTab) createTab.classList.add('hidden');
    if (historyTab) { historyTab.classList.remove('hidden'); historyTab.style.gridColumn = '1 / -1'; historyTab.style.width = '100%'; }
    if (progressCard) progressCard.classList.add('hidden');
    document.getElementById('nav-history').className = 'nav-btn active';
    document.getElementById('nav-history').style.background = '#6366f1';
    document.getElementById('nav-history').style.color = '#fff';
    document.getElementById('nav-create').className = 'nav-btn';
    document.getElementById('nav-create').style.background = '#21262d';
    document.getElementById('nav-create').style.color = '#8b949e';
    loadHistory();
  }
}

// ====== 历史记录 ======
async function loadHistory() {
  const list = document.getElementById('history-list');
  try {
    const resp = await fetch('/api/history');
    const data = await resp.json();
    if (data.length === 0) {
      list.innerHTML = '<div style="color:#8b949e;text-align:center;padding:40px;grid-column:1/-1">还没有生成过视频，去创作第一条吧 🎬</div>';
      return;
    }
    let html = '';
    data.forEach(item => {
      html += `<div class="history-card" onclick="window.open('${item.final_url}','_blank')">
        <div class="thumb-wrap">
          <video src="${item.final_url}" preload="metadata" muted loop
            onmouseenter="this.play()" onmouseleave="this.pause();this.currentTime=0"></video>
          <div class="card-overlay">
            <span style="color:#fff;font-size:9px">${item.size_mb}MB · ${item.segments}段</span>
          </div>
        </div>
        <div class="card-info">
          <div class="card-meta">
            <div class="vid">${item.created.replace(/ /g,' ')}</div>
          </div>
          <div class="card-actions" onclick="event.stopPropagation()">
            <a href="${item.final_url}" download style="padding:2px 6px;background:#238636;color:#fff;border-radius:3px;font-size:9px;text-decoration:none">⬇</a>
            <button onclick="regenerateFromHistory('${item.session_id}')" style="padding:2px 4px;background:#21262d;color:#8b949e;border:1px solid #30363d;border-radius:3px;font-size:9px;cursor:pointer">🔄</button>
            <button onclick="deleteHistory('${item.session_id}')" style="padding:2px 4px;background:#490202;color:#f85149;border:1px solid #f85149;border-radius:3px;font-size:9px;cursor:pointer">🗑</button>
          </div>
        </div>
      </div>`;
    });
    list.innerHTML = html;
  } catch(e) {
    list.innerHTML = '<div style="color:#f85149;text-align:center;padding:40px">加载失败</div>';
  }
}

async function deleteHistory(sid) {
  if (!confirm('确认删除这个视频？')) return;
  try {
    await fetch('/api/history/' + sid, {method: 'DELETE'});
    addLog('🗑 已删除: ' + sid, 'warning');
    loadHistory();
  } catch(e) {
    addLog('❌ 删除失败', 'error');
  }
}

function regenerateFromHistory(sid) {
  showTab('create');
  addLog('📋 历史视频 ID: ' + sid + '（重新生成功能待实现）');
}

// ====== 一键口语化 ======
async function colloquialize() {
  const script = document.getElementById('script').value.trim();
  if (!script) { addLog('⚠️ 请先粘贴文案', 'error'); return; }
  
  const btn = document.getElementById('btn-colloquialize');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> 口语化中...';
  addLog('💬 一键口语化处理中...');
  
  try {
    const resp = await fetch('/api/colloquialize', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({script})
    });
    const data = await resp.json();
    if (data.error) throw new Error(data.error);
    
    document.getElementById('script').value = data.colloquial;
    updateScriptStats();
    previewSplit();
    addLog('✅ 口语化完成', 'success');
  } catch(e) {
    addLog('❌ ' + e.message, 'error');
  }
  btn.disabled = false;
  btn.innerHTML = '💬 口语化';
}



// ====== 批量生成 ======
function handleBatchUpload(input) {
  const file = input.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = function(e) {
    const lines = e.target.result.split('\n').filter(l => l.trim());
    if (lines.length < 2) { addLog('⚠️ CSV 至少需要表头 + 1条数据', 'error'); return; }
    const header = lines[0].toLowerCase();
    const charIdx = header.split(',').findIndex(h => h.includes('character'));
    const scriptIdx = header.split(',').findIndex(h => h.includes('script'));
    if (charIdx < 0 || scriptIdx < 0) { addLog('⚠️ CSV 需要 character 和 script 列', 'error'); return; }
    
    const tasks = [];
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(',');
      const character = (cols[charIdx] || '').trim();
      const script = cols.slice(scriptIdx).join(',').trim();
      if (character && script) tasks.push({character, script});
    }
    
    document.getElementById('batch-preview').innerHTML = 
      `共 <b>${tasks.length}</b> 条待生成 · ` +
      `<button onclick="runBatchDirect()" style="padding:4px 10px;background:#238636;color:#fff;border:none;border-radius:4px;font-size:11px;cursor:pointer">🚀 开始批量生成</button>`;
    addLog(`📦 批量导入 ${tasks.length} 条`, 'success');
    window._batchTasks = tasks;
  };
  reader.readAsText(file, 'UTF-8');
}

async function runBatchDirect() {
  const tasks = window._batchTasks || [];
  if (tasks.length === 0) return;
  addLog(`📦 批量生成 ${tasks.length} 条开始...`);
  
  for (let i = 0; i < tasks.length; i++) {
    const t = tasks[i];
    document.getElementById('character').value = t.character;
    document.getElementById('script').value = t.script;
    updateScriptStats();
    document.getElementById('batch-preview').innerHTML = `⏳ 正在生成 ${i+1}/${tasks.length}...`;
    try {
      await startPipeline();
    } catch(e) {
      addLog(`❌ 第${i+1}条失败: ${e.message}`, 'error');
    }
    if (i < tasks.length - 1) {
      addLog(`⏸ 等待5秒后继续...`);
      await new Promise(r => setTimeout(r, 5000));
    }
  }
  document.getElementById('batch-preview').innerHTML = `✅ 批量完成 ${tasks.length} 条`;
  addLog(`🎉 批量生成完成`, 'success');
}


function connectSSE(sid) {
  if (eventSource) eventSource.close();

  eventSource = new EventSource('/api/events/' + sid);

  eventSource.addEventListener('log', e => {
    const d = JSON.parse(e.data);
    addLog(d.message);
  });

  eventSource.addEventListener('stage', e => {
    const d = JSON.parse(e.data);
    addLog('📍 ' + d.label);
    if (d.stage === 'portrait') updateStep(1, 'active');
    if (d.stage === 'video') updateStep(1, 'done'); updateStep(2, 'active');
    if (d.stage === 'concat') updateStep(1, 'done'); updateStep(2, 'done'); updateStep(3, 'active');
  });

  eventSource.addEventListener('progress', e => {
    const d = JSON.parse(e.data);
    const segPct = d.percent || 0;
    const total = d.total_segments || 1;
    const seg = d.segment || 1;
    // 阶段2占总体 10%-85%
    const overallPct = 10 + ((seg - 1 + segPct/100) / total) * 75;
    setProgress(Math.round(overallPct), `段${seg}/${total} · ${segPct}%`);
  });

  eventSource.addEventListener('portrait_ready', e => {
    const d = JSON.parse(e.data);
    document.getElementById('portrait-img').src = d.url;
    document.getElementById('preview-portrait').classList.remove('hidden');
    updateStep(1, 'done');
    updateStep(2, 'active');
    setProgress(10, '定妆照完成 ✓');
  });

  eventSource.addEventListener('segment_done', e => {
    const d = JSON.parse(e.data);
    addLog(`段${d.segment} 完成 (${d.time_s}s, ${d.size_kb}KB)`, 'success');
  });

  eventSource.addEventListener('concat_done', e => {
    const d = JSON.parse(e.data);
    addLog(`拼接完成 (${d.size_mb}MB)`, 'success');
  });

  eventSource.addEventListener('complete', e => {
    const d = JSON.parse(e.data);
    updateStep(1, 'done');
    updateStep(2, 'done');
    updateStep(3, 'done');
    setProgress(100, '🎉 完成！');
    document.getElementById('download-link').href = d.final_url;
    document.getElementById('preview-download').classList.remove('hidden');
    // 设置视频播放器
    const player = document.getElementById('final-video-player');
    if (player && d.final_url) player.src = d.final_url;
    const startBtn = document.getElementById('btn-start');
    const confirmBtn = document.getElementById('btn-confirm-generate');
    if (startBtn) { startBtn.disabled = false; startBtn.innerHTML = '🚀 再来一条'; startBtn.classList.remove('hidden'); }
    if (confirmBtn) { confirmBtn.disabled = false; confirmBtn.innerHTML = '✅ 确认无误，开始生成'; }
    document.getElementById('btn-cancel').classList.add('hidden');
    addLog('🎉 全部完成！点击下载成片', 'success');
    loadSegments(currentSession);
    eventSource.close();
  });

  eventSource.addEventListener('error', e => {
    let msg = '未知错误';
    let actionRequired = false;
    try {
      const d = JSON.parse(e.data);
      msg = d.message;
      actionRequired = d.action_required;
    } catch(_) {}
    
    if (actionRequired) {
      // 隧道不可达 → 弹窗让用户选择
      document.getElementById('tunnel-error-modal').classList.remove('hidden');
      window._tunnelErrorSession = currentSession;
    } else {
      addLog('❌ ' + msg, 'error');
    }
    document.getElementById('btn-start').disabled = false;
    document.getElementById('btn-start').innerHTML = '🚀 开始生成';
    document.getElementById('btn-cancel').classList.add('hidden');
    document.getElementById('segments-overview').classList.remove('hidden');
    document.getElementById('progress-steps').classList.add('hidden');
    eventSource.close();
  });

  eventSource.addEventListener('aborted', e => {
    let msg = '用户取消';
    try { msg = JSON.parse(e.data).message; } catch(_) {}
    addLog('🛑 ' + msg, 'warn');
    document.getElementById('btn-start').disabled = false;
    document.getElementById('btn-start').innerHTML = '🚀 开始生成';
    document.getElementById('btn-cancel').classList.add('hidden');
    document.getElementById('segments-overview').classList.remove('hidden');
    document.getElementById('progress-steps').classList.add('hidden');
    eventSource.close();
  });

  eventSource.onerror = () => {
    // SSE 连接断开，静默处理（会自动重连）
  };
}

// ====== 隧道状态 ======
async function refreshTunnelStatus() {
  const dot = document.getElementById('tunnel-status-dot');
  const text = document.getElementById('tunnel-status-text');
  const btn = document.getElementById('tunnel-reconnect-btn');
  try {
    const resp = await fetch('/api/tunnel/status');
    const s = await resp.json();
    if (s.status === 'connected') {
      dot.style.background = '#238636';
      text.textContent = '隧道: 已连接';
      text.style.color = '#238636';
      btn.style.display = 'none';
    } else if (s.status === 'process_only') {
      dot.style.background = '#d2991d';
      text.textContent = '隧道: 进程存活但不可达';
      text.style.color = '#d2991d';
      btn.style.display = 'inline-block';
    } else {
      dot.style.background = '#f85149';
      text.textContent = '隧道: 未连接';
      text.style.color = '#f85149';
      btn.style.display = 'inline-block';
    }
  } catch(e) {
    dot.style.background = '#484f58';
    text.textContent = '隧道: 检测失败';
    text.style.color = '#484f58';
    btn.style.display = 'inline-block';
  }
}

// 重连按钮 → 调用同样的隧道重启 API，然后刷新状态
async function tunnelReconnect() {
  const btn = document.getElementById('tunnel-reconnect-btn');
  btn.disabled = true;
  btn.textContent = '⏳ ...';
  try {
    const resp = await fetch('/api/tunnel/restart', {method: 'POST'});
    const data = await resp.json();
    if (data.ok) {
      addLog('✅ 隧道已重连: ' + data.url.slice(0, 50) + '...', 'success');
    } else {
      addLog('❌ 重连失败: ' + data.error, 'error');
    }
  } catch(e) {
    addLog('❌ 重连异常: ' + e.message, 'error');
  }
  btn.disabled = false;
  btn.textContent = '🔄 重连';
  refreshTunnelStatus();
}

// ====== 隧道错误处理 ======
async function tunnelRetry() {
  const btn = document.querySelector('#tunnel-error-modal button');  // 第一个按钮
  if (btn) { btn.disabled = true; btn.textContent = '⏳ 重启隧道中...'; }
  addLog('🔧 正在重启公网隧道...', 'info');
  
  try {
    const resp = await fetch('/api/tunnel/restart', {method: 'POST'});
    const data = await resp.json();
    if (data.ok) {
      addLog('✅ 隧道已重启: ' + data.url.slice(0, 60) + '...', 'success');
      document.getElementById('tunnel-error-modal').classList.add('hidden');
      refreshTunnelStatus();
      // 自动重试生成
      addLog('🔄 隧道就绪，自动重新生成...', 'info');
      setTimeout(() => startPipeline(), 500);
    } else {
      addLog('❌ 隧道重启失败: ' + (data.error || '未知错误'), 'error');
      if (btn) { btn.disabled = false; btn.textContent = '① 重启隧道后重试'; }
    }
  } catch(e) {
    addLog('❌ 隧道重启异常: ' + e.message, 'error');
    if (btn) { btn.disabled = false; btn.textContent = '① 重启隧道后重试'; }
  }
}

function tunnelSwitchAI() {
  document.getElementById('tunnel-error-modal').classList.add('hidden');
  // 清除本地照片引用，让管道走 Seedream AI 生成
  uploadedPortraitUrl = null;
  const imgEl = document.getElementById('portrait-preview-img');
  if (imgEl) imgEl.src = '';
  document.getElementById('portrait-preview-area').classList.add('hidden');
  addLog('🎨 已切换到 AI 生成模式，将用 Seedream 生成定妆照。请重新点击「开始生成」', 'info');
}

function tunnelManualURL() {
  document.getElementById('tunnel-error-modal').classList.add('hidden');
  const url = prompt('请粘贴照片的公网 URL（图床/OSS 等）:');
  if (url && url.startsWith('http')) {
    uploadedPortraitUrl = url;
    const imgEl = document.getElementById('portrait-preview-img');
    if (imgEl) { imgEl.src = url; imgEl.style.display = 'block'; }
    document.getElementById('portrait-preview-area').classList.remove('hidden');
    document.getElementById('portrait-preview-label').textContent = '外部URL';
    addLog('🔗 已设置公网 URL，请重新点击「开始生成」', 'success');
  } else if (url) {
    addLog('⚠️ URL 格式不正确，需要以 http 开头', 'error');
  }
}

// 页面初始化
loadDraft();
renderPortraitLib();
loadServerPortraits();
refreshTunnelStatus();
// 每30秒自动刷新隧道状态
setInterval(refreshTunnelStatus, 30000);
</script>
<!-- 隧道错误弹窗 -->
<div id="tunnel-error-modal" class="hidden" style="position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.7);z-index:999;display:flex;align-items:center;justify-content:center">
  <div style="background:#161b22;border:1px solid #30363d;border-radius:12px;padding:24px;max-width:440px;width:90%">
    <h3 style="color:#f85149;margin:0 0 12px">⚠️ 公网隧道不可达</h3>
    <p style="color:#8b949e;font-size:13px;margin:0 0 16px;line-height:1.6">本地照片无法被云端 Seedance 访问。请选择处理方式：</p>
    <button onclick="tunnelRetry()" style="width:100%;padding:10px;margin-bottom:8px;background:#238636;color:#fff;border:none;border-radius:6px;font-size:13px;cursor:pointer">① 重启隧道后重试</button>
    <button onclick="tunnelSwitchAI()" style="width:100%;padding:10px;margin-bottom:8px;background:#1a0d33;color:#a371f7;border:1px solid #a371f7;border-radius:6px;font-size:13px;cursor:pointer">② 切换到「🎨 AI生成」模式</button>
    <button onclick="tunnelManualURL()" style="width:100%;padding:10px;margin-bottom:8px;background:#21262d;color:#8b949e;border:1px solid #30363d;border-radius:6px;font-size:13px;cursor:pointer">③ 手动粘贴公网 URL</button>
    <button onclick="document.getElementById('tunnel-error-modal').classList.add('hidden')" style="width:100%;padding:8px;background:transparent;color:#484f58;border:none;font-size:12px;cursor:pointer">✕ 关闭</button>
  </div>
</div>

</body>
</html>
"""

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    print("\n🎬 koubo Web 控制台")
    print(f"   http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

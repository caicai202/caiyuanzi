#!/usr/bin/env python3
"""
火山引擎 ARK API 公共客户端
pipeline.py 和 web_server.py 共享的 API 调用逻辑

用法:
    from ark_client import (
        get_key, get_headers,
        generate_portrait, create_video_task, query_task, wait_for_task,
        download_with_retry,
    )
"""
import os
import sys
import time
import requests
from pathlib import Path

# 尝试从 .env 加载环境变量
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

# ============================================================
# 配置
# ============================================================
ARK_BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"
ARK_API_KEY = os.environ.get("ARK_API_KEY", "")
if not ARK_API_KEY:
    print("⚠️  未设置 ARK_API_KEY，请创建 .env 文件或设置环境变量", file=sys.stderr)

SEEDREAM_MODEL = "doubao-seedream-5-0-260128"
SEEDANCE_15_PRO = "doubao-seedance-1-5-pro-251215"
SEEDANCE_2 = "doubao-seedance-2-0-260128"

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def get_key():
    return ARK_API_KEY


def get_headers():
    return {
        "Authorization": f"Bearer {ARK_API_KEY}",
        "Content-Type": "application/json",
    }


def _bypass_session():
    """创建绕过系统代理的 requests Session"""
    s = requests.Session()
    s.trust_env = False
    return s


api_session = _bypass_session()


# ============================================================
# 图片生成 — Seedream 5.0
# ============================================================
def generate_portrait(prompt: str, size: str = "1080x1920", n: int = 1):
    """
    调用 Seedream 生成定妆照。
    返回 (urls, saved_paths)
    """
    headers = get_headers()
    payload = {
        "model": SEEDREAM_MODEL,
        "prompt": prompt,
        "size": size,
        "n": n,
        "output_format": "png",
    }
    resp = requests.post(
        f"{ARK_BASE_URL}/images/generations",
        headers=headers,
        json=payload,
        timeout=120,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Seedream 失败 HTTP {resp.status_code}: {resp.text[:500]}")

    data = resp.json()
    urls = [img["url"] for img in data.get("data", [])]
    if not urls:
        raise RuntimeError("未返回图片 URL")

    saved = []
    for i, url in enumerate(urls):
        img_data = requests.get(url, timeout=30).content
        path = OUTPUT_DIR / f"portrait_{i}.png"
        path.write_bytes(img_data)
        saved.append(str(path))

    return urls, saved


def generate_portrait_raw(prompt: str, size: str = "1440x2560",
                          api_key: str = "", model: str = "") -> tuple[bytes, str]:
    """
    生成定妆照并返回原始图片数据（供 web_server 和 pipeline 共用）。
    支持自定义 API key 和模型。
    返回 (image_bytes, image_url)。
    """
    key = api_key or ARK_API_KEY
    mdl = model or SEEDREAM_MODEL
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload: dict = {"model": mdl, "prompt": prompt, "size": size, "n": 1}
    if "lite" in mdl:
        payload["output_format"] = "png"

    resp = api_session.post(
        f"{ARK_BASE_URL}/images/generations",
        headers=headers,
        json=payload,
        timeout=120,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Seedream 失败 HTTP {resp.status_code}: {resp.text[:300]}")

    img_url = resp.json()["data"][0]["url"]
    img_data = download_with_retry(img_url, "定妆照", max_retries=3, timeout=60)
    return img_data, img_url


# ============================================================
# 视频生成 — Seedance
# ============================================================
def create_video_task(
    model: str,
    content: list,
    duration: int = 15,
    resolution: str = "720p",
    ratio: str = "9:16",
    generate_audio: bool = True,
    return_last_frame: bool = True,
    seed: int = -1,
) -> dict:
    """创建视频生成任务"""
    payload = {
        "model": model,
        "content": content,
        "duration": duration,
        "resolution": resolution,
        "ratio": ratio,
        "generate_audio": generate_audio,
        "return_last_frame": return_last_frame,
        "seed": seed,
    }
    resp = requests.post(
        f"{ARK_BASE_URL}/contents/generations/tasks",
        headers=get_headers(),
        json=payload,
        timeout=60,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"创建任务失败 HTTP {resp.status_code}: {resp.text[:500]}")
    return resp.json()


def query_task(task_id: str) -> dict:
    """查询任务状态"""
    resp = requests.get(
        f"{ARK_BASE_URL}/contents/generations/tasks/{task_id}",
        headers=get_headers(),
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"查询任务失败: {resp.text[:300]}")
    return resp.json()


def wait_for_task(task_id: str, poll_interval: int = 20, max_wait: int = 900) -> dict:
    """轮询等待任务完成"""
    return _poll_task(task_id, poll_interval, max_wait)


def wait_for_task_with_progress(task_id: str, on_progress=None,
                                poll_interval: int = 10, max_wait: int = 900) -> dict:
    """
    轮询等待任务完成，带进度回调。
    on_progress(status, elapsed_seconds) 在每个轮询周期被调用。
    """
    return _poll_task(task_id, poll_interval, max_wait, on_progress)


def _poll_task(task_id: str, poll_interval: int, max_wait: int,
               on_progress=None) -> dict:
    start = time.time()
    last_status = ""
    while time.time() - start < max_wait:
        result = query_task(task_id)
        status = result.get("status", "unknown")
        elapsed = int(time.time() - start)

        if status != last_status:
            print(f"   [{elapsed}s] 状态: {status}")
            last_status = status

        if on_progress:
            on_progress(status, elapsed)

        if status == "succeeded":
            return result
        elif status == "failed":
            raise RuntimeError(f"任务失败: {result.get('error', {})}")
        elif status == "expired":
            raise RuntimeError("任务超时过期")

        time.sleep(poll_interval)

    raise TimeoutError(f"任务超时 ({max_wait}s): {query_task(task_id).get('status')}")


# ============================================================
# 下载工具
# ============================================================
def download_with_retry(url: str, label: str = "", max_retries: int = 3, timeout: int = 120) -> bytes:
    """下载视频/图片，带指数退避重试"""
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = api_session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                wait = 2 ** attempt
                print(f"  ⚠️ {label} 下载失败 (尝试 {attempt}/{max_retries}): {e}")
                print(f"     {wait}s 后重试...")
                time.sleep(wait)
    raise RuntimeError(f"{label} 下载失败（{max_retries}次重试后）: {last_error}")


# ============================================================
# 视频拼接 — ffmpeg concat（供 web_server 和 pipeline 共用）
# ============================================================
import subprocess as _sp


def concat_videos(segment_paths: list[str], output_path: str, on_log=None) -> str:
    """
    使用 ffmpeg concat 拼接视频段。
    先尝试 -c copy（无损），失败则回退到 filter_complex 重编码。
    返回输出文件路径。
    """
    def log(msg: str):
        if on_log:
            on_log(msg)
        else:
            print(msg)

    concat_list = Path(output_path).with_suffix(".txt")
    lines = [f"file '{p}'" for p in segment_paths]
    concat_list.write_text("\n".join(lines), encoding="utf-8")

    try:
        result = _sp.run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", str(concat_list), "-c", "copy", output_path,
        ], capture_output=True, text=True, timeout=300)

        if result.returncode == 0:
            log(f"✅ 视频拼接完成 → {output_path}")
            return output_path

        log("⚠️ -c copy 拼接失败，尝试 filter_complex 重编码...")
        filter_parts = " ".join([f"[{i}:v][{i}:a]" for i in range(len(segment_paths))])
        filter_str = f"{filter_parts} concat=n={len(segment_paths)}:v=1:a=1 [v][a]"
        cmd = ["ffmpeg", "-y"]
        for p in segment_paths:
            cmd.extend(["-i", p])
        cmd.extend(["-filter_complex", filter_str, "-map", "[v]", "-map", "[a]",
                     "-c:v", "libx264", "-crf", "23", "-c:a", "aac", "-b:a", "128k",
                     output_path])
        result2 = _sp.run(cmd, capture_output=True, text=True, timeout=300)

        if result2.returncode != 0:
            raise RuntimeError(f"重编码失败:\n{result2.stderr[:500]}")
        log(f"✅ 重编码拼接完成 → {output_path}")
        return output_path
    finally:
        if concat_list.exists():
            concat_list.unlink()

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
import time
import requests
from pathlib import Path

# ============================================================
# 配置
# ============================================================
ARK_BASE_URL = "https://ark.cn-beijing.volces.com/api/v3"
ARK_API_KEY = os.environ["ARK_API_KEY"]

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
    start = time.time()
    last_status = ""
    while time.time() - start < max_wait:
        result = query_task(task_id)
        status = result.get("status", "unknown")

        if status != last_status:
            elapsed = int(time.time() - start)
            print(f"   [{elapsed}s] 状态: {status}")
            last_status = status

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

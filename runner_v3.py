#!/usr/bin/env python3
"""
[DEPRECATED] 此脚本已废弃。
用途: 手动逐段轮询已提交的 Seedance 任务并下载视频。
此功能已整合到 pipeline.py 和 web_server.py 中，仅保留作参考。
硬编码路径（/home/administrator/videopipe/）和 task ID 不可直接复用。
"""
import json
import time
import requests
import json
import time
import requests
import os
from pathlib import Path

ARK_KEY = os.environ.get("ARK_API_KEY", "")
if not ARK_KEY:
    raise SystemExit("未设置 ARK_API_KEY 环境变量")
HEADERS = {"Authorization": f"Bearer {ARK_KEY}", "Content-Type": "application/json"}
MODEL = "doubao-seedance-1-5-pro-251215"
BASE = "https://ark.cn-beijing.volces.com/api/v3/contents/generations/tasks"
OUT = Path("/home/administrator/videopipe/output")
LOG = OUT / "run.log"

def log(msg):
    print(msg, flush=True)
    with open(LOG, "a") as f:
        f.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")

with open("/home/administrator/videopipe/segments.json") as f:
    ALL_SEGMENTS = json.load(f)

SEG2_TASK = "cgt-20260502204839-b8zsp"

log("=== Starting from segment 2 ===")

def poll_and_download(task_id, seg_num):
    log(f"Seg{seg_num}: polling {task_id}")
    start = time.time()
    while time.time() - start < 900:
        q = requests.get(f"{BASE}/{task_id}", headers=HEADERS, timeout=30)
        d = q.json()
        st = d.get("status", "?")
        if st == "succeeded":
            vu = d.get("content", {}).get("video_url", "")
            lf = d.get("content", {}).get("last_frame_url", "")
            if not vu:
                raise Exception("No video_url")
            vpath = OUT / f"seg{seg_num:02d}.mp4"
            vpath.write_bytes(requests.get(vu, timeout=60).content)
            t = int(time.time() - start)
            log(f"Seg{seg_num} DONE {t}s -> {vpath} ({vpath.stat().st_size//1024}KB)")
            return lf
        elif st in ("failed", "expired"):
            raise Exception(f"Seg{seg_num} {st}: {d.get('error','')}")
        elapsed = int(time.time() - start)
        if elapsed % 30 < 15:
            log(f"Seg{seg_num} [{elapsed}s] {st}")
        time.sleep(15)

def create_task(seg, ref_url, seg_num):
    payload = {
        "model": MODEL,
        "content": [
            {"type": "text", "text": seg["prompt"]},
            {"type": "image_url", "image_url": {"url": ref_url}, "role": "first_frame"},
        ],
        "duration": seg["duration"], "resolution": "720p", "ratio": "9:16",
        "generate_audio": True, "return_last_frame": True,
    }
    r = requests.post(BASE, headers=HEADERS, json=payload, timeout=60)
    if r.status_code != 200:
        raise Exception(f"Create seg{seg_num} failed: {r.text[:300]}")
    return r.json()["id"]

# Poll segment 2 (already submitted)
lf = poll_and_download(SEG2_TASK, 2)

# Run segments 3-7
for i in range(2, 7):
    seg = ALL_SEGMENTS[i]
    n = i + 1
    log(f"--- Seg {n}/7 ({seg['duration']}s) ---")
    tid = create_task(seg, lf, n)
    log(f"Seg{n} task: {tid}")
    lf = poll_and_download(tid, n)

log("=== ALL 7 SEGMENTS COMPLETE ===")
for p in sorted(OUT.glob("seg*.mp4")):
    log(f"  {p.name}")

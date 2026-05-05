#!/usr/bin/env python3
"""Run segments 2-7, starting from known seg2 task ID"""
import json, time, requests, sys
from pathlib import Path

ARK_KEY = "d423fa6a-53e3-4159-8590-ec9fbc5171ca"
HEADERS = {"Authorization": f"Bearer {ARK_KEY}", "Content-Type": "application/json"}
MODEL = "doubao-seedance-1-5-pro-251215"
BASE = "https://ark.cn-beijing.volces.com/api/v3/contents/generations/tasks"
OUT = Path("/home/administrator/videopipe/output")

with open("/home/administrator/videopipe/segments.json") as f:
    ALL_SEGMENTS = json.load(f)

SEG2_TASK = "cgt-20260502204839-b8zsp"

def poll_and_download(task_id, seg_num, ref_override=None):
    """Poll task, download video, return last_frame_url."""
    print(f"Segment {seg_num}: polling {task_id}", flush=True)
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
            print(f"DONE {t}s -> {vpath} ({vpath.stat().st_size//1024}KB)", flush=True)
            return lf
        elif st in ("failed", "expired"):
            raise Exception(f"{st}: {d.get('error','')}")
        elapsed = int(time.time() - start)
        if elapsed % 30 < 15:
            print(f"  [{elapsed}s] {st}", flush=True)
        time.sleep(15)

def create_task(seg, ref_url):
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
        raise Exception(f"Create failed: {r.text[:300]}")
    return r.json()["id"]

# Poll segment 2
lf = poll_and_download(SEG2_TASK, 2)

# Run segments 3-7
for i in range(2, 7):
    seg = ALL_SEGMENTS[i]
    n = i + 1
    print(f"\n--- Seg {n}/7 ({seg['duration']}s) ---", flush=True)
    tid = create_task(seg, lf)
    print(f"Task: {tid}", flush=True)
    lf = poll_and_download(tid, n)

print(f"\n✅ ALL 7 segments complete!", flush=True)
for p in sorted(OUT.glob("seg*.mp4")):
    print(f"  {p.name}")

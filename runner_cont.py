#!/usr/bin/env python3
"""Continue from segment 2 using last_frame from segment 1"""
import json, time, requests, sys
from pathlib import Path

ARK_KEY = "d423fa6a-53e3-4159-8590-ec9fbc5171ca"
HEADERS = {"Authorization": f"Bearer {ARK_KEY}", "Content-Type": "application/json"}
MODEL = "doubao-seedance-1-5-pro-251215"
OUT = Path("/home/administrator/videopipe/output")

LAST_FRAME_URL = sys.argv[1]  # from segment 1

with open("/home/administrator/videopipe/segments.json") as f:
    SEGMENTS = json.load(f)

# Start from segment 2 (index 1)
SEGMENTS = SEGMENTS[1:]
print(f"🎬 Continuing from seg2: {len(SEGMENTS)} segments, {sum(s['duration'] for s in SEGMENTS)}s", flush=True)

results = []
ref_url = LAST_FRAME_URL

for idx, seg in enumerate(SEGMENTS):
    n = idx + 2  # actual segment number
    d = seg["duration"]
    print(f"\n--- Seg {n}/7 ({d}s) ---", flush=True)

    payload = {
        "model": MODEL,
        "content": [
            {"type": "text", "text": seg["prompt"]},
            {"type": "image_url", "image_url": {"url": ref_url}, "role": "first_frame"},
        ],
        "duration": d, "resolution": "720p", "ratio": "9:16",
        "generate_audio": True, "return_last_frame": True,
    }

    r = requests.post(
        "https://ark.cn-beijing.volces.com/api/v3/contents/generations/tasks",
        headers=HEADERS, json=payload, timeout=60
    )
    if r.status_code != 200:
        print(f"FAIL create: {r.text[:300]}", flush=True)
        sys.exit(1)
    tid = r.json()["id"]
    print(f"Task: {tid}", flush=True)

    start = time.time()
    result = None
    while time.time() - start < 900:
        q = requests.get(
            f"https://ark.cn-beijing.volces.com/api/v3/contents/generations/tasks/{tid}",
            headers=HEADERS, timeout=30
        )
        data = q.json()
        st = data.get("status", "?")
        if st == "succeeded":
            result = data
            break
        elif st in ("failed", "expired"):
            print(f"FAIL: {st} {data.get('error','')}", flush=True)
            sys.exit(1)
        elapsed = int(time.time() - start)
        if elapsed % 30 < 15:
            print(f"  [{elapsed}s] {st}", flush=True)
        time.sleep(15)

    video_url = result.get("content", {}).get("video_url", "")
    last_frame = result.get("content", {}).get("last_frame_url", "")

    vpath = OUT / f"seg{n:02d}.mp4"
    vpath.write_bytes(requests.get(video_url, timeout=60).content)
    elapsed_total = int(time.time() - start)
    print(f"DONE in {elapsed_total}s -> {vpath} ({vpath.stat().st_size//1024}KB)", flush=True)

    results.append({"n": n, "path": str(vpath), "last_frame": last_frame})
    ref_url = last_frame or ref_url

(OUT / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2))
print(f"\n✅ ALL DONE: {len(results)} segments (2-{len(results)+1})", flush=True)
for r in results:
    print(f"  Seg{r['n']}: {r['path']}", flush=True)

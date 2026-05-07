#!/usr/bin/env python3
"""
公网隧道管理器 — 使用 serveo.net SSH 暴露 localhost:5000
用法: python3 tunnel.py
输出: 写入项目根目录 .tunnel_url 文件（与 web_server.py 读取路径一致）
"""
import subprocess
import re
import time
import sys
import os
import atexit
from pathlib import Path

TUNNEL_FILE = str(Path(__file__).parent / ".tunnel_url")

def main():
    proc = subprocess.Popen(
        ['ssh', '-o', 'StrictHostKeyChecking=no',
         '-o', 'ServerAliveInterval=30',
         '-R', '80:localhost:5000', 'serveo.net'],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    atexit.register(lambda: proc.terminate())

    start = time.time()
    url = None
    while time.time() - start < 15:
        line = proc.stdout.readline()
        if not line:
            break
        print(line.strip())
        m = re.search(r'https://[^\s]+', line)
        if m:
            url = m.group(0)
            break

    if not url:
        print("❌ 获取隧道 URL 失败")
        sys.exit(1)

    with open(TUNNEL_FILE, 'w') as f:
        f.write(url)
    # 写入 PID 供自动关闭使用（与 web_server.py 读取路径一致）
    pid_file = str(Path(__file__).parent / ".tunnel_pid")
    with open(pid_file, 'w') as f:
        f.write(str(os.getpid()))
    print(f"\n✅ 公网隧道: {url}")
    print(f"   已写入: {TUNNEL_FILE}")

    # 保持隧道运行
    try:
        for line in proc.stdout:
            pass
    except KeyboardInterrupt:
        print("\n隧道已关闭")

if __name__ == "__main__":
    main()

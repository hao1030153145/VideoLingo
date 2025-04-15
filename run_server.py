#!/usr/bin/env python3
import os
import sys
import signal
import argparse
import subprocess
import psutil
import time

# 配置
PID_FILE = "server.pid"
LOG_FILE = "server.log"

def get_script_path():
    """获取脚本所在目录的绝对路径"""
    return os.path.dirname(os.path.abspath(__file__))

def is_running():
    """检查服务器是否正在运行"""
    try:
        if os.path.exists(PID_FILE):
            with open(PID_FILE, 'r') as f:
                pid = int(f.read().strip())
                # 检查进程是否存在
                process = psutil.Process(pid)
                return process.is_running() and "python" in process.name().lower()
        return False
    except (ProcessLookupError, psutil.NoSuchProcess, ValueError):
        return False

def start_server():
    """启动服务器"""
    if is_running():
        print("Server is already running!")
        return

    print("Starting server...")
    try:
        # 使用 nohup 在后台运行服务器
        with open(LOG_FILE, 'a') as log, open(LOG_FILE, 'a') as err:
            process = subprocess.Popen([
                "nohup", sys.executable, "-m", "uvicorn",
                "video_main:app",
                "--host", "0.0.0.0",
                "--port", "8000",
                "--workers", "1",
                "--ws-ping-interval", "20",
                "--ws-ping-timeout", "20"
            ],
                stdout=log,
                stderr=err,
                preexec_fn=os.setsid)

        # 保存 PID
        with open(PID_FILE, 'w') as f:
            f.write(str(process.pid))

        print(f"Server started with PID: {process.pid}")
        print(f"Logs are being written to: {LOG_FILE}")

    except Exception as e:
        print(f"Failed to start server: {e}")
        sys.exit(1)

def stop_server():
    """停止服务器"""
    if not is_running():
        print("Server is not running!")
        return

    try:
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())

        # 获取进程组ID
        process = psutil.Process(pid)
        pgid = os.getpgid(pid)

        # 终止整个进程组
        os.killpg(pgid, signal.SIGTERM)

        # 等待进程结束
        try:
            process.wait(timeout=5)
        except psutil.TimeoutExpired:
            os.killpg(pgid, signal.SIGKILL)

        os.remove(PID_FILE)
        print("Server stopped successfully!")

    except Exception as e:
        print(f"Error stopping server: {e}")
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)

def server_status():
    """查看服务器状态"""
    if not is_running():
        print("Server is not running")
        return

    try:
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        process = psutil.Process(pid)

        print(f"Server is running (PID: {pid})")
        print(f"CPU Usage: {process.cpu_percent()}%")
        print(f"Memory Usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")
        print(f"Running Time: {time.time() - process.create_time():.2f} seconds")
        print(f"Log file: {os.path.abspath(LOG_FILE)}")
    except Exception as e:
        print(f"Error getting server status: {e}")

def main():
    parser = argparse.ArgumentParser(description='Server Management Script')
    parser.add_argument('action', choices=['start', 'stop', 'restart', 'status'],
                        help='Action to perform')

    args = parser.parse_args()

    # 切换到脚本所在目录
    os.chdir(get_script_path())

    if args.action == 'start':
        start_server()
    elif args.action == 'stop':
        stop_server()
    elif args.action == 'restart':
        stop_server()
        time.sleep(2)  # 等待服务器完全停止
        start_server()
    elif args.action == 'status':
        server_status()

if __name__ == "__main__":
    main()
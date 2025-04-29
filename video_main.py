import asyncio
from multiprocessing.dummy import current_process

from st_components.imports_and_utils import *
from starlette.websockets import WebSocketState
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import os, sys, shutil
import threading
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.config_utils import load_key
from core.step1_ytdlp import find_video_files
from time import sleep
import re
import subprocess
from translations.translations import translate as t

OUTPUT_DIR = "output"

SUB_VIDEO = "output/output_sub.mp4"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# 确保输出目录存在
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=OUTPUT_DIR), name="static")
app.mount("/assets", StaticFiles(directory="html/assets"), name="assets")

# 2. 返回 index.html
@app.get("/")
def read_index():
    return FileResponse("html/index.html")

# 3. fallback，支持 SPA 刷新路由（如果你以后加路由可以用这个）
@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    file_path = os.path.join("html", full_path)
    if os.path.exists(file_path):
        return FileResponse(file_path)
    else:
        return FileResponse("html/index.html")

@app.post("/api/upload")
async def upload_video(file: UploadFile = File(...)):
    try:
        # 清理旧文件
        if os.path.exists(OUTPUT_DIR):
            shutil.rmtree(OUTPUT_DIR)
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # 安全处理文件名
        raw_name = file.filename.replace(' ', '_')
        name, ext = os.path.splitext(raw_name)
        clean_name = re.sub(r'[^\w\-_\.]', '', name) + ext.lower()

        # 保存文件
        file_path = os.path.join(OUTPUT_DIR, clean_name)
        with open(file_path, "wb") as f:
            content = await file.read()
            f.write(content)

        return JSONResponse({
            "type": "video",
            "path": f"/static/{clean_name}"
        })
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/videos")
def list_videos(type: str):
    if type == "src":
        video_file = find_video_files()
        return {"videos": [{"name": os.path.basename(video_file), "url": f"/static/{os.path.basename(video_file)}"}]}
    elif type == "sub":
        processed_videos = []
        if os.path.exists(SUB_VIDEO):
            processed_videos.append({
                "name": os.path.basename(SUB_VIDEO),
                "url": f"/static/{os.path.basename(SUB_VIDEO)}"
            })
        return {"videos": processed_videos}

@app.delete("/api/videos")
def delete_video():
    try:
        video_file = find_video_files()
        print(f"获得所有的视频文件: {video_file}")
        os.remove(video_file)
        if os.path.exists(OUTPUT_DIR):
            shutil.rmtree(OUTPUT_DIR)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(404, "File not found")

# 启动翻译任务的接口
@app.post("/api/translate")
async def start_translation(background_tasks: BackgroundTasks):
    task_id = str(time.time())
    print(f"start_translation: {task_id}")
    background_tasks.add_task(run_process_text, task_id)
    return {"task_id": task_id}

# 存储WebSocket连接
active_connections = {}

@app.websocket("/ws/{task_id}")
async def websocket_endpoint(websocket: WebSocket, task_id: str):
    await websocket.accept()
    active_connections[task_id] = websocket

    # 建立链接的时候首先看看是否有历史的进度
    print(f"websocket progress_store info : {progress_store}")
    print(f"websocket progress_history info : {progress_history}")
    current_process = progress_store.get(task_id, {"status": "not_found"})

    if current_process.get("status") in ["completed", "error"]:
        # 先发送历史记录（如果有的话）
        if task_id in progress_history:
            for progress in progress_history[task_id]:
                await websocket.send_json(progress)
                await asyncio.sleep(0.5)

        # 然后清空历史记录以及关闭链接
        if task_id in progress_history:
            del progress_history[task_id]
        await websocket.close()
        return

        # 然后发送当前进度
    if task_id in progress_history:
        for progress in progress_history[task_id]:
            await websocket.send_json(progress)
            await asyncio.sleep(0.5)

    try:
        while True:
            await websocket.receive_text()  # 保持连接活跃
    except WebSocketDisconnect as e:
        if e.code == 1000:
            print("Client disconnected normally")
        else:
            print(f"Abnormal disconnect: {e}")
    finally:
        await del_active_connections()

async def notify_progress(task_id: str, progress: dict):
    """通过WebSocket发送进度更新"""
    # 初始化历史记录
    print(f"notify_progress current progress_history: {progress_history}")
    progress_record = progress.copy()
    # 如果task_id不在active_connections中，则将进度添加到历史记录中
    if task_id not in active_connections:
        # 将进度添加到历史记录中
        print(f"notify_progress to history: {task_id}, {progress_record}")
        progress_history[task_id].append(progress_record)
        # 如何任务已经完成或者出错，则五分钟后清理历史记录
        if progress.get("status") in ["completed", "error"]:
            await clean_task_history(task_id)

    if task_id in active_connections:
        try:
            ws = active_connections[task_id]
            await ws.send_json(progress)
            # 如何任务已经完成或者出错，立即关闭链接
            if progress.get("status") in ["completed", "error"]:
                await ws.close()
                await del_active_connections()
                await clean_task_history(task_id)
        except:
            await del_active_connections()

HISTORY_CLEANUP_DELAY = 300  # 5分钟后清理历史记录
async def clean_task_history(task_id: str):
    """延迟清理任务历史"""
    await asyncio.sleep(HISTORY_CLEANUP_DELAY)
    if task_id in progress_history:
        del progress_history[task_id]
    if task_id in progress_store:
        del progress_store[task_id]
    print(f"Cleaned up history for task {task_id}")

async def del_active_connections():
    # 创建字典键的副本进行迭代
    for task_id in list(active_connections.keys()):
        connection = active_connections.pop(task_id, None)
        if connection:
            try:
                # 检查连接状态再关闭
                print(f"Closing connection {task_id}, state: {connection.client_state}")
                if connection.client_state !=  WebSocketState.DISCONNECTED:
                    await connection.close()
            except Exception as e:
                print(f"Error closing connection {task_id}: {e}")

progress_store = {}    # 存储每个任务的当前进度
progress_history = {}  # 存储每个任务的进度历史
def run_process_text(task_id: str):
    # 初始化进度历史
    progress_history[task_id] = []
    progress_store[task_id] = {"status": "running", "progress": 0, "timestamp": int(time.time())}
    try:
        # 包装原有处理流程
        async def wrapped_process():
            try:
                progress_store[task_id]["progress"] = 10
                progress_store[task_id]["timestamp"] = int(time.time())
                progress_store[task_id].update({"current_step": "Preparing output folder..."})
                await notify_progress(task_id, progress_store[task_id])

                # Using Whisper for transcription...
                progress_store[task_id]["progress"] = 25
                progress_store[task_id]["timestamp"] = int(time.time())
                progress_store[task_id].update({"current_step": "Using Whisper for transcription..."})
                await notify_progress(task_id, progress_store[task_id])
                try:
                    step2_whisperX.transcribe()
                except Exception as e:
                    error_msg = f"Transcription failed: {str(e)}"
                    raise RuntimeError(error_msg)

                # Splitting long sentences...
                progress_store[task_id]["progress"] = 40
                progress_store[task_id]["timestamp"] = int(time.time())
                progress_store[task_id].update({"current_step": "Splitting long sentences..."})
                await notify_progress(task_id, progress_store[task_id])
                try:
                    step3_1_spacy_split.split_by_spacy()
                    step3_2_splitbymeaning.split_sentences_by_meaning()
                except Exception as e:
                    error_msg = f"Splitting long sentences failed: {str(e)}"
                    raise RuntimeError(error_msg)

                # Summarizing and translating...
                progress_store[task_id]["progress"] = 55
                progress_store[task_id]["timestamp"] = int(time.time())
                progress_store[task_id].update({"current_step": "Summarizing and translating..."})
                await notify_progress(task_id, progress_store[task_id])
                try:
                    step4_1_summarize.get_summary()
                    step4_2_translate_all.translate_all()
                except Exception as e:
                    error_msg = f"Summarizing and translating failed: {str(e)}"
                    raise RuntimeError(error_msg)

                # Processing and aligning subtitles...
                progress_store[task_id]["progress"] = 70
                progress_store[task_id]["timestamp"] = int(time.time())
                progress_store[task_id].update({"current_step": "Processing and aligning subtitles..."})
                await notify_progress(task_id, progress_store[task_id])
                try:
                    step5_splitforsub.split_for_sub_main()
                    step6_generate_final_timeline.align_timestamp_main()
                except Exception as e:
                    error_msg = f"Processing and aligning subtitles failed: {str(e)}"
                    raise RuntimeError(error_msg)

                # Merging subtitles to video...
                progress_store[task_id]["progress"] = 85
                progress_store[task_id]["timestamp"] = int(time.time())
                progress_store[task_id].update({"current_step": "Merging subtitles to video..."})
                await notify_progress(task_id, progress_store[task_id])
                try:
                    step7_merge_sub_to_vid.merge_subtitles_to_video()
                except Exception as e:
                    error_msg = f"Merging subtitles to video failed: {str(e)}"
                    raise RuntimeError(error_msg)

                # Subtitle processing complete! 🎉
                progress_store[task_id]["progress"] = 100
                progress_store[task_id]["timestamp"] = int(time.time())
                progress_store[task_id]["status"] = "completed"
                progress_store[task_id].update({"current_step": "Subtitle processing complete! 🎉"})
                await notify_progress(task_id, progress_store[task_id])
            except Exception as e:
                error_msg = str(e)
                progress_store[task_id].update({
                    "status": "error",
                    "error": error_msg,
                    "current_step": f"Error: {error_msg}",
                    "timestamp": int(time.time())
                })
                await notify_progress(task_id, progress_store[task_id])
                raise
        def run_async_process():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(wrapped_process())
            finally:
                loop.close()

        # 在后台线程中运行
        thread = threading.Thread(target=run_async_process)
        thread.start()

    except Exception as e:
        progress_store[task_id] = {"status": "error", "message": str(e)}
        # 创建新的事件循环来发送错误消息
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(notify_progress(task_id, progress_store[task_id]))
        finally:
            loop.close()

# 获取进度的接口
@app.get("/api/progress/{task_id}")
def get_progress(task_id: str):
    print(f"get_progress: {task_id}")
    return progress_store.get(task_id, {"status": "not_found"})

# 导出srt文件的接口
@app.get("/api/srt_zip")
def srt_zip():
    output_dir = "output"
    file_name = "subtitles.zip"
    zip_path = os.path.join(output_dir, file_name)
    try:
        with zipfile.ZipFile(zip_path, "w") as zip_file:
            for file_name in os.listdir(output_dir):
                if file_name.endswith(".srt"):
                    file_path = os.path.join(output_dir, file_name)
                    if os.path.isfile(file_path):
                        with open(file_path, "rb") as f:
                            zip_file.writestr(file_name, f.read())

        # 返回zip文件的URL
        return {"url": f"/static/{file_name}"}
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"文件生成失败: {str(e)}")


from pydantic import BaseModel
from core.config_utils import update_key, load_key

# 定义配置模型
class ConfigUpdate(BaseModel):
    source_language: str
    target_language: str
    burn_subtitles: bool
    demucs_voice: bool

src_langs = [
    {"label": "🇺🇸 English", "value": "en"},
    {"label": "🇨🇳 简体中文", "value": "zh"},
    {"label": "🇪🇸 Español", "value": "es"},
    {"label": "🇷🇺 Русский", "value": "ru"},
    {"label": "🇫🇷 Français", "value": "fr"},
    {"label": "🇩🇪 Deutsch", "value": "de"},
    {"label": "🇮🇹 Italiano", "value": "it"},
    {"label": "🇯🇵 日本語", "value": "ja"}
]

sub_langs = [
    {"label": "🇨🇳 简体中文", "value": "简体中文"},
    {"label": "🇺🇸 英语", "value": "英语"},
    {"label": "🇰🇷 韩语", "value": "韩语"},
    {"label": "🇫🇷 法语", "value": "法语"},
    {"label": "🇯🇵 日語", "value": "日語"}
]

@app.get("/api/languages")
async def get_languages():
    """获取可用的源语言和目标语言选项"""
    try:
        return {
            "src_langs": src_langs,
            "sub_langs": sub_langs
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get language options: {str(e)}")

@app.post("/api/config")
async def update_config(config: ConfigUpdate):
    """更新配置接口"""
    try:

        # 检测输入的源语言是否属于上面的语言列表
        valid_source_languages = [item["value"] for item in src_langs]
        if config.source_language not in valid_source_languages:
            return {
                "status": False,
                "message": "source_language only support auto, en, zh, es, ru, fr, de, it, ja",
                "code": 400
            }

        # 更新源语言
        update_key("whisper.language", config.source_language)

        # 更新目标语言
        update_key("target_language", config.target_language)

        # 更新字幕嵌入设置
        update_key("burn_subtitles", config.burn_subtitles)

        # 更新人声分离增强设置
        update_key("demucs", config.demucs_voice)

        return {
            "status": "success",
            "message": "Configuration updated successfully",
            "config": {
                "source_language": config.source_language,
                "target_language": config.target_language,
                "burn_subtitles": config.burn_subtitles,
                "demucs_voice": config.demucs_voice
            }
        }
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Configuration key not found: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update configuration: {str(e)}")

@app.get("/api/config")
async def get_config():
    """获取当前配置"""
    try:
        config = {
            "source_language": load_key("whisper.language"),
            "target_language": load_key("target_language"),
            "burn_subtitles": load_key("burn_subtitles"),
            "demucs_voice": load_key("demucs")
        }
        return config
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Configuration key not found: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load configuration: {str(e)}")


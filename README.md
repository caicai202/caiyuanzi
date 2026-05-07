# 口播成片 (koubo)

火山引擎 ARK AI 口播视频生成管道 —— 照片 + 剧本 = 口型同步成片。

## 工作流程

```
用户上传照片 → Seedream 5.0 定妆照 → Seedance 1.5 Pro 分段生成 → ffmpeg 合成 → 最终视频
```

- **Seedream 5.0**：根据用户照片和角色描述生成 AI 定妆照
- **Seedance 1.5 Pro**：根据剧本分段生成口型同步视频（口型精准 > 音色一致）
- **ffmpeg**：拼接所有片段输出最终成片

## 快速开始

### 环境要求

- Python 3.10+
- ffmpeg
- 火山引擎 ARK API Key

### 安装

```bash
cd videopipe
pip install -r requirements.txt  # flask flask-cors urllib3 requests
```

### 配置

复制 `.env.example` 为 `.env`，填入火山引擎 API Key：

```env
ARK_API_KEY=ark-xxxxxxxxxx
```

### 启动 Web 控制台

```bash
python3 web_server.py
```

浏览器打开 `http://localhost:5000`，上传照片、输入剧本即可生成。

### 命令行模式

```bash
# 使用剧本文件
python3 pipeline.py --script demo_script.txt --character "30岁知性女主播，短发，职业装"

# 交互式输入
python3 pipeline.py --interactive

# 指定 Seedance 版本
python3 pipeline.py --model 2.0 --script 剧本.txt --character "..."
```

## 项目结构

```
videopipe/
├── web_server.py      # Web 控制台 (Flask + SSE 实时进度)
├── pipeline.py        # 命令行管道
├── ark_client.py      # 火山引擎 ARK API 客户端
├── runner_v3.py       # 任务执行器
├── tts_engine.py      # TTS 引擎
├── tunnel.py          # 内网穿透
├── segments.json      # 分段配置
├── demo_script.txt    # 示例剧本
├── output/            # 输出目录（视频/图片）
└── .env               # API Key 配置（不提交 git）
```

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/submit` | 提交生成任务 |
| GET | `/api/status/<id>` | 查询任务状态 |
| GET | `/api/stream/<id>` | SSE 实时进度 |
| GET | `/api/download/<id>` | 下载成片 |
| GET | `/api/config` | 查看/更新配置 |
| POST | `/api/config` | 更新配置 |

## 技术栈

- **火山引擎 ARK**：Seedream 5.0 + Seedance 1.5 Pro
- **后端**：Flask + waitress
- **前端**：原生 HTML/CSS/JS
- **视频处理**：ffmpeg
- **部署**：WSL2 Ubuntu 24.04

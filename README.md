# CloudDrive2 Auto Refresh

🚀 CloudDrive2 目录自动刷新工具。监控本地文件变动，支持多服务器联动、路径映射、写入稳定检测、防抖队列、失败自动复查与青龙通知。

文件或目录在本地挂载点发生变动后，脚本通过 gRPC 调用 `GetSubFiles(forceRefresh=True)`，刷新各台 CD2 上对应目录。所有启用的服务共用路径映射，每台拥有独立连接和任务队列。

## ✨ 功能

- 👀 通过 watchdog 递归监听目录的新增、删除、移动及修改事件。
- 🖼️ 等待文件非零且大小、修改时间连续稳定后安排刷新，适用于先创建 0KB 占位文件再写入的图片等文件。
- ⏳ 同目录事件合并防抖，按目录深度优先处理父目录。
- 🖥️ 同一次本地变动刷新多台 CD2，各台独立处理失败与重试。
- 🔁 最终失败记录落盘，默认每 10 分钟复查，重启后恢复失败目标。
- 🔔 支持青龙 `notify.send`；其他环境输出通知到控制台。
- 🩺 输出分阶段启动日志，并检查监听、队列及调度线程状态。

## 📁 文件

```text
clouddrive2-auto-refresh/
├── clouddrive2_auto_refresh.py  # 主脚本，服务器及路径配置在文件开头
├── clouddrive.proto         # CloudDrive2 协议定义
├── requirements.txt        # 运行及协议生成依赖
├── LICENSE                 # GNU GPL v3.0 许可证
└── README.md
```

仓库包含以上五个文件。运行时会在脚本目录生成 `cd2_failed_refreshes.json`，请确保目录可写。两个生成模块、失败记录、日志和 `__pycache__` 属于本地文件；如需提交修改，只选择项目源文件。

## 🚀 使用

### 1. 安装依赖并生成 gRPC 模块

进入项目目录，在运行脚本的 Python 环境中执行：

```bash
python3 -m pip install --upgrade -r requirements.txt
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto
```

第二条命令会在当前目录生成 `clouddrive_pb2.py` 和 `clouddrive_pb2_grpc.py`，必须与主脚本放在同一个目录。两个文件由使用者在运行环境中生成，不提交到仓库。

在青龙面板中，也可以通过「依赖管理 → Python3」安装 `watchdog`、`grpcio`、`grpcio-tools` 和 `protobuf`，再通过终端进入脚本目录执行生成命令。首次部署，以及更新协议或更换 Python 环境后，都先重新生成模块再启动监控。

### 2. 配置 CD2 服务器

编辑 `clouddrive2_auto_refresh.py` 开头的 `CD2_SERVERS`：

```python
CD2_SERVERS = [
    {
        'name': 'CD2-1',
        'enabled': True,
        'host': 'cd2-1.example.com:443',
        'use_https': True,
        'token': '填写这台 CD2 的完整 API 令牌',
    },
]
```

| 配置 | 说明 |
| --- | --- |
| `name` | 每台服务的唯一名称，用于日志、通知及失败记录 |
| `enabled` | 是否启用；至少启用一台 |
| `host` | gRPC 地址，格式为 `域名:端口` 或 `IP:端口`，不带协议或路径 |
| `use_https` | 使用 TLS 填 `True`，明文连接填 `False` |
| `token` | 该台 CD2 的 API 令牌；脚本校验 UUID 格式，可带 `Bearer ` 前缀 |

需要多台服务时复制配置条目，每台填写自己的地址和令牌。仓库中的示例默认均未启用，令牌留空；个人配置中的令牌不要提交到公开仓库。

### 3. 配置路径映射

```python
RAW_RULES = """
/ql/115open/CD2 => /115open/CD2
"""
```

左侧是脚本所在机器可以访问的本地挂载目录，右侧是 CD2 内部目录。右侧路径必须在所有启用的服务中具有相同名称与层级。多个目录每行填写一条；有重叠时，把更具体的子目录规则放在前面。

每台 CD2 按脚本的配置说明设置缓存：

1. 「设置 → 系统 → 目录缓存」启用缓存持久化。
2. 对需要刷新的目录打开「属性 → 缓存管理」，将缓存时间设为 0 秒。

### 4. 启动监控

```bash
python3 -u clouddrive2_auto_refresh.py
```

青龙面板的任务命令：

```bash
task clouddrive2_auto_refresh.py
```

这是持续运行的监控任务。部署时保留一个实例，避免定时任务反复启动重叠实例，并确认运行环境不会到时自动终止长任务。脚本启动后监听后续事件；启动前已经存在的目录差异不会自动全量补刷。

## ⚙️ 默认参数

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `DEBOUNCE_SECONDS` | `10` | 同目录最后一次事件后等待 10 秒入队 |
| `FILE_SETTLE_INTERVAL` | `1` | 每秒检查待写文件 |
| `FILE_SETTLE_CHECKS` | `3` | 连续 3 次检测到大小与修改时间不变后通过；首次非零采样只建立基准 |
| `FILE_SETTLE_TIMEOUT` | `300` | 待写文件最多等待 5 分钟 |
| `NOTIFY_FILE_SETTLE_TIMEOUT` | `True` | 写入等待超时发送通知 |
| `FILE_TIMEOUT_NOTIFY_LIMIT` | `20` | 每条超时通知最多列出 20 个文件 |
| `REFRESH_FILE_GRANDPARENT` | `False` | 是否额外刷新文件的爷爷目录 |
| `CONNECTION_IDLE_TIMEOUT` | `300` | gRPC 连接空闲 5 分钟后关闭 |
| `WATCH_START_WARNING_SECONDS` | `30` | 监听初始化超过阈值后告警 |
| `FAILED_REFRESH_RETRY_SECONDS` | `600` | 最终失败后每 10 分钟复查 |
| `NOTIFY_FIRST_REFRESH_FAILURE` | `False` | 首次最终失败默认只记录，延迟复查仍失败才告警 |

文件仍为 0KB 或无法读取时，稳定计数保持为 0。超时后移除该次等待并输出文件路径；后续新事件可以重新登记等待。大小和修改时间稳定是本地采样判断，云端上传完成及远端索引可见时间仍由云盘和 CD2 决定。

刷新成功后结束该任务；默认不安排成功后的固定补刷。刷新失败最多尝试 3 次，仍失败则保存目标，后续定时复查。目录不存在等情况会按脚本的父目录及删除处理逻辑另行处理。

## 🧩 更新协议与生成模块

协议及 API 用法参见 [CloudDrive2 官方 gRPC API 指南](https://www.clouddrive2.com/api/CloudDrive2_gRPC_API_Guide.html)。本项目附带的 `clouddrive.proto` 协议版本字段为 `1.1.2`。应使用与服务 API 匹配的协议；协议版本号不是 CD2 应用版本号。

需要更新时，将新协议保存为 `clouddrive.proto`。使用这个文件名生成，才能得到主脚本导入的 `clouddrive_pb2` 模块。

```bash
python3 -m pip install --upgrade -r requirements.txt
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto
```

请在项目目录执行命令，一起重新生成两个模块。生成和运行使用同一个 Python 环境；导入报版本不匹配时，安装依赖后重新生成模块。

## 🔍 常见问题

- **脚本提示至少启用一台 CD2**：填写地址、令牌并将该条目的 `enabled` 改为 `True`。
- **协议模块导入失败**：检查两个生成文件是否与主脚本同目录，依赖版本是否符合生成代码要求。
- **认证或权限错误**：检查每台服务的 API 令牌、授权路径及目录列举权限；参照官方指南配置。
- **没有检测到变动**：先确认映射目录已完成监听初始化。脚本依赖系统文件事件，具体挂载方式是否会产生事件需实际验证。
- **目录监听初始化缓慢**：检查挂载是否正常、子目录数量及 Linux inotify 限额；各映射目录独立初始化。
- **图片一直为 0KB**：5 分钟后输出写入超时并按配置通知，检查上传进度和挂载状态。
- **没有收到手机通知**：青龙中需要正确配置 `notify` 渠道；普通 Python 环境默认只打印通知内容。

## 📄 License

本项目原创代码采用 [GNU General Public License v3.0](LICENSE)（GPL-3.0-only）许可。

`clouddrive.proto` 为 CloudDrive2 上游协议文件，保留其原有权利及许可。

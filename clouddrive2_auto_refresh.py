# SPDX-License-Identifier: GPL-3.0-only
"""CloudDrive2 Auto Refresh：多服务器目录自动刷新工具。

使用前：
1. 安装依赖：python3 -m pip install -r requirements.txt
2. 在脚本目录使用与 CD2 API 匹配的 clouddrive.proto 生成 Python 模块：
   python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto
   生成的 clouddrive_pb2.py、clouddrive_pb2_grpc.py 必须与脚本在同目录。
3. 填写文件开头的 CD2_SERVERS 和 RAW_RULES，启用至少一台，再运行脚本。
   python3 -u clouddrive2_auto_refresh.py

青龙环境会使用 notify.send；其他环境没有 notify 时会输出到控制台。
首次最终失败默认只记录，10 分钟后复查仍失败才告警，后续持续复查。
失败清单写入脚本同目录的 cd2_failed_refreshes.json，该目录需有写入权限。
此文件包含示例配置，运行生成的失败清单和日志会包含真实路径、服务名称及错误详情。
"""


# ================= 使用前填写这两块配置 =================
# 【1. CD2 服务器配置】
# 每台 CD2 占一个字典；添加更多服务时复制一个条目，字典之间保留逗号。
# 填好地址和令牌后，将 enabled 改为 True；至少启用一台。
#
# name：显示在日志和通知中的名称，每台必须唯一，使用后尽量保持不变。
# enabled：True 表示启用，False 表示跳过该台。
# host：gRPC 地址，填写“域名:端口”或“IP:端口”，不要带 http://、https:// 或路径。
#       HTTPS 反向代理一般使用 443；直连时填写实际的 gRPC 服务端口。
# use_https：地址启用 TLS 时填 True，明文连接时填 False；此项不是启用开关。
# token：直接粘贴该台 CD2 的完整 API 令牌，每台使用自己的令牌。
#
# 以下为分享用示例：地址需要替换，真实令牌已清空，两台默认均未启用。
CD2_SERVERS = [
    {
        'name': 'CD2-1',
        'enabled': False,
        'host': 'cd2-1.example.com:443',
        'use_https': True,
        'token': '',  # 填写第一台的 API 令牌
    },
    {
        'name': 'CD2-2',
        'enabled': False,
        'host': 'cd2-2.example.com:19798',
        'use_https': False,
        'token': '',  # 填写第二台的 API 令牌
    },
]

# 【2. 监控目录与路径映射】
# 所有启用的 CD2 共用以下映射，同一次本地变动会刷新到全部服务。
# 使用前，请在每台 CD2 上完成以下缓存设置：
# 1. 进入“设置 → 系统 → 目录缓存”，勾选“启用缓存持久化”。
# 2. 对每个需要同步的目录，打开“属性 → 缓存管理”，将缓存时间设为 0 秒。
#
# 每行格式：本机挂载目录 => CD2 内部目录路径。
# 左侧必须是运行脚本的机器能访问的实际目录。
# 右侧目录的名称和层级必须在所有启用的 CD2 中保持一致。
# 按实际情况修改或删除示例；监控多个目录时，每个目录单独填写一行。
# 映射按从上到下的顺序匹配；有重叠时，将更具体的子目录规则放在前面。
RAW_RULES = """
/ql/115open/CD2 => /115open/CD2
"""
# ================= 配置填写结束 =================


import os
import sys
import time
import threading
import queue
import traceback
import json
import re

# 必须放在所有第三方模块导入之前，否则导入阶段卡住时青龙只会显示“开始执行”。
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except (AttributeError, OSError):
    pass

print("⏳ [启动 1/4] Python 已启动，正在导入 watchdog...", flush=True)
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except Exception as e:
    print(f"❌ 导入 watchdog 失败: {type(e).__name__}: {e}", flush=True)
    traceback.print_exc()
    raise
print("✅ [启动 1/4] watchdog 导入成功。", flush=True)
# 安装模块： grpcio grpcio-tools
# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto

# 尝试导入青龙自带通知，本地调试时提供 fallback
print("⏳ [启动 2/4] 正在导入青龙 notify...", flush=True)
try:
    from notify import send
except ImportError:
    def send(title, content):
        print(f"[未找到 notify 模块] 通知内容:\n{title}\n{content}")
    print("⚠️ [启动 2/4] 未找到 notify，将使用控制台输出。", flush=True)
except Exception as e:
    print(f"❌ 导入 notify 失败: {type(e).__name__}: {e}", flush=True)
    traceback.print_exc()
    raise
else:
    print("✅ [启动 2/4] notify 导入成功。", flush=True)

# 青龙等后台环境可能会缓冲标准输出，导致脚本其实卡住了但日志迟迟看不到。
def _thread_exception_handler(args):
    """让后台线程的未捕获异常带完整堆栈输出，避免线程静默退出。"""
    print(f"\n❌ 后台线程 {args.thread.name} 异常退出: {args.exc_value}", flush=True)
    traceback.print_exception(args.exc_type, args.exc_value,
                              args.exc_traceback, file=sys.stderr)

if hasattr(threading, "excepthook"):
    threading.excepthook = _thread_exception_handler

def safe_send(title, content, timeout=15):
    """发送通知，但不允许通知接口无限期卡住主程序。"""
    result = {'error': None}
    done = threading.Event()

    def _send():
        try:
            send(title, content)
        except Exception as e:
            result['error'] = e
        finally:
            done.set()

    thread = threading.Thread(target=_send, name="notify-send", daemon=True)
    thread.start()
    if not done.wait(timeout):
        print(f"⚠️ 通知发送超过 {timeout} 秒，已跳过等待，监控程序继续运行。", flush=True)
        return False
    if result['error'] is not None:
        print(f"⚠️ 通知发送失败: {result['error']}", flush=True)
        return False
    return True

# ================= 运行参数（一般使用默认值） =================
# 防抖等待时间（秒）：文件发生变动后，等待 10 秒。
# 如果 10 秒内还有新变动，则重新计时；如果 10 秒内无变动，则触发刷新。
DEBOUNCE_SECONDS = 10

# 文件写入完成判定：连续若干次检测到“大小和修改时间均不再变化”后，才刷新目录。
# 这同时修复了 0KB 占位文件若没有后续 modified 事件就永远漏刷的问题。
FILE_SETTLE_INTERVAL = 1
FILE_SETTLE_CHECKS = 3
FILE_SETTLE_TIMEOUT = 300
# 0KB、文件不存在或持续写入超过等待上限时，是否发送青龙通知。
NOTIFY_FILE_SETTLE_TIMEOUT = True
# 单条通知最多展示的文件数，避免异常批量任务产生过长消息。
FILE_TIMEOUT_NOTIFY_LIMIT = 20

# 文件发生变化时，是否额外刷新它的爷爷目录。
# 默认关闭，避免一次文件改名刷新两层目录。
# 只有确认挂载盘仍会漏报“新建/移入目录”时，才建议改为 True。
REFRESH_FILE_GRANDPARENT = False

# 空闲断开时间（秒）：5分钟没有任何刷新任务时，自动销毁 gRPC 连接
CONNECTION_IDLE_TIMEOUT = 300

# watchdog 对 recursive=True 的目录会在启动时遍历全部子目录并注册 inotify。
# 超过此时间仍未完成时只报警，不阻塞其他网盘继续启动。
WATCH_START_WARNING_SECONDS = 30

# 最终失败后每 10 分钟复查；首次仅记录，复查仍失败才告警。
FAILED_REFRESH_RETRY_SECONDS = 600
NOTIFY_FIRST_REFRESH_FAILURE = False
FAILED_REFRESH_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 'cd2_failed_refreshes.json')

# 全局线程锁，防止 watchdog 多线程与 Timer 线程同时修改字典导致数据丢失
_stats_lock = threading.Lock()

# 用于记录 10 秒防抖期间的文件变动统计
pending_stats = {}
# 废弃 Timer 线程池，改为轻量级的时间戳字典，防止海量文件导致线程爆炸
debounce_targets = {}
# 等待写入稳定的文件：path -> 状态字典
settling_files = {}
# (CD2 名称 + 远端目录) -> 最终失败记录；各台独立复查，JSON 不保存 token。
failed_refreshes = {}
_retry_stopping = threading.Event()

# ==========================================

# 尝试导入 gRPC 编译后的模块
print("⏳ [启动 3/4] 正在导入 grpc 和 clouddrive 模块...", flush=True)
try:
    import grpc
    import clouddrive_pb2
    import clouddrive_pb2_grpc
except Exception as e:
    msg = ("❌ 导入 gRPC 模块失败！请先安装 requirements.txt 中的依赖，"
           "再在脚本目录执行：\n"
           "python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto\n"
           f"{type(e).__name__}: {e}")
    print(msg, flush=True)
    traceback.print_exc()
    safe_send("CD2 监控致命错误", msg)
    exit(1)
print("✅ [启动 3/4] grpc 和 clouddrive 模块导入成功。", flush=True)

def build_server_config(configs, shared_rules):
    """校验 CD2 配置；所有服务共用一份路径映射。"""
    mappings = []
    for line in shared_rules.strip().splitlines():
        if '=>' in line:
            left, right = [part.strip().rstrip('/') for part in line.split('=>', 1)]
            if left and right:
                mappings.append((left, right))
    if not mappings:
        raise ValueError('没有有效的公共路径映射规则')
    servers = {}
    for config in configs:
        if not config.get('enabled', True):
            continue
        name = str(config.get('name', '')).strip()
        host = str(config.get('host', '')).strip()
        token = str(config.get('token', '')).strip()
        if not name or name in servers:
            raise ValueError('每台启用的 CD2 必须配置唯一且非空的 name')
        if not host or '://' in host:
            raise ValueError(f'{name}: host 不能为空，也不能带 http:// 或 https:// 前缀')
        if token.startswith('Bearer '):
            token = token[7:].strip()
        if not re.fullmatch(
                r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
                r'[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', token):
            raise ValueError(f'{name}: API 令牌格式不正确，请复制完整令牌')
        token = 'Bearer ' + token
        servers[name] = dict(config, name=name, host=host, token=token,
                             use_https=config.get('use_https', True))
    if not servers:
        raise ValueError('至少需要启用一台 CD2')
    return servers, mappings


# 本地目录统一监听，文件事件分发给所有启用的 CD2。
SERVERS, MAPPINGS = build_server_config(CD2_SERVERS, RAW_RULES)
DEFAULT_SERVER_ID = next(iter(SERVERS))
refresh_queues = {name: queue.PriorityQueue() for name in SERVERS}
worker_threads = {}
_task_counter = 0  # 全局编号，作为同深度的 FIFO 序号和复查任务标记


def failure_key(server_id, remote_path):
    return json.dumps([server_id, remote_path], ensure_ascii=False)


def _save_failed_refreshes_locked():
    """调用方持有 _stats_lock；原子替换 JSON，排队标记不写入磁盘。"""
    try:
        snapshot = {
            path: {k: v for k, v in record.items() if k != 'queued_id'}
            for path, record in failed_refreshes.items()
        }
        temp_path = FAILED_REFRESH_FILE + '.tmp'
        with open(temp_path, 'w', encoding='utf-8') as file:
            json.dump(snapshot, file, ensure_ascii=False, indent=2)
        os.replace(temp_path, FAILED_REFRESH_FILE)
    except Exception as e:
        print(f"⚠️ 失败记录写入磁盘失败，内存中的定时重试仍会继续: {e}", flush=True)


def load_failed_refreshes():
    """恢复各台失败记录；旧单 CD2 格式归属第一台，不复制给其他服务。"""
    try:
        with open(FAILED_REFRESH_FILE, encoding='utf-8') as file:
            records = json.load(file)
        with _stats_lock:
            for old_key, saved_record in records.items():
                record = dict(saved_record)
                server_id = record.get('server_id', DEFAULT_SERVER_ID)
                remote_path = record.get('remote_path', old_key)
                if server_id not in SERVERS:
                    print(f"⚠️ 跳过已停用 CD2 的失败记录: {server_id}", flush=True)
                    continue
                if get_mapped_remote_path(record['local_dir']) != remote_path:
                    continue
                record.update(server_id=server_id, remote_path=remote_path, queued_id=None)
                failed_refreshes[failure_key(server_id, remote_path)] = record
        print(f"📋 已恢复 {len(failed_refreshes)} 个失败目标: {FAILED_REFRESH_FILE}", flush=True)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"⚠️ 读取失败记录失败: {e}", flush=True)


def clear_failed_refresh(remote_path, reason='刷新成功', server_id=None):
    """仅清除指定 CD2 的记录，其他 CD2 的同名目录不受影响。"""
    server_id = server_id or DEFAULT_SERVER_ID
    with _stats_lock:
        record = failed_refreshes.pop(failure_key(server_id, remote_path), None)
        if record is not None:
            _save_failed_refreshes_locked()
    if record is not None:
        print(f"✅ [{server_id}] 已清除失败记录（{reason}）: {remote_path}", flush=True)


def remember_failed_refresh(local_dir, remote_path, stats, error, server_id=None):
    """按 CD2 和目录收集最终失败；复查失败告警，再安排 10 分钟后的复查。"""
    server_id = server_id or DEFAULT_SERVER_ID
    key = failure_key(server_id, remote_path)
    now = time.time()
    is_recheck = stats.get('_failed_retry_id') is not None
    saved_stats = {k: v for k, v in stats.items()
                   if k not in ('_failed_retry_id', 'inversion_retries')}
    with _stats_lock:
        record = failed_refreshes.get(key)
        if is_recheck and (record is None
                           or record.get('queued_id') != stats['_failed_retry_id']):
            return
        first_failure = record is None
        if first_failure:
            record = {
                'server_id': server_id, 'remote_path': remote_path,
                'first_failed': now, 'next_retry': now + FAILED_REFRESH_RETRY_SECONDS,
                'failure_count': 0, 'retry_rounds': 0, 'queued_id': None,
            }
            failed_refreshes[key] = record
        if is_recheck:
            record['retry_rounds'] += 1
            record['next_retry'] = now + FAILED_REFRESH_RETRY_SECONDS
            record['queued_id'] = None
        record.update(local_dir=local_dir, stats=saved_stats,
                      last_failed=now, last_error=error)
        record['failure_count'] += 1
        retry_rounds = record['retry_rounds']
        _save_failed_refreshes_locked()
    print(f"📋 [{server_id}] 最终失败已记录，已纳入 {FAILED_REFRESH_RETRY_SECONDS // 60} 分钟复查机制: {remote_path}", flush=True)
    if is_recheck or (first_failure and NOTIFY_FIRST_REFRESH_FAILURE):
        content = (
            f"CD2: {server_id} ({SERVERS[server_id]['host']})\n"
            f"路径: {remote_path}\n错误: {error}\n"
            f"已完成延迟复查: {retry_rounds} 轮\n"
            f"仍未成功，将在 {FAILED_REFRESH_RETRY_SECONDS // 60} 分钟后继续复查。"
        )
        try:
            safe_send(f"CD2 目录刷新持续失败 [{server_id}]" if is_recheck
                      else f"CD2 目录刷新重试失败 [{server_id}]", content)
        except Exception as e:
            print(f"⚠️ [{server_id}] 失败告警发送异常，已保留重试记录: {e}", flush=True)


def dispatch_failed_refreshes():
    """到期目标只进入其所属 CD2 的队列，每个目标最多一个复查任务。"""
    global _task_counter
    if _retry_stopping.is_set():
        return
    now = time.time()
    with _stats_lock:
        due = [(key, record) for key, record in failed_refreshes.items()
               if record.get('queued_id') is None and now >= record['next_retry']]
    due.sort(key=lambda item: len(item[1]['remote_path'].strip('/').split('/')))
    for key, record in due:
        server_id, remote_path = record['server_id'], record['remote_path']
        local_dir = record['local_dir']
        if get_mapped_remote_path(local_dir) != remote_path or not os.path.isdir(local_dir):
            clear_failed_refresh(remote_path, '目录已删除、移走或映射已变更', server_id)
            continue
        with _stats_lock:
            current = failed_refreshes.get(key)
            if (current is not record or current.get('queued_id') is not None
                    or now < current['next_retry']):
                continue
            _task_counter += 1
            counter = _task_counter
            current['queued_id'] = counter
            stats = dict(current['stats'])
            stats['inversion_retries'] = 0
            stats['_failed_retry_id'] = counter
        depth = len(remote_path.strip('/').split('/'))
        refresh_queues[server_id].put((depth, counter, (local_dir, remote_path, stats)))
        print(f"🔁 [{server_id}] 失败目录已到期，加入复查队列: {remote_path}", flush=True)

def grpc_worker(server_id):
    """每台 CD2 一个串行消费者；一台超时不会阻塞其他台。"""
    refresh_queue = refresh_queues[server_id]
    while True:
        task = None
        try:
            item = refresh_queue.get()
            if item is None or item[2] is None:
                break
            priority, counter, task = item
            local_dir, remote_path, stats = task
            retry_id = stats.get('_failed_retry_id')
            if retry_id is not None:
                with _stats_lock:
                    record = failed_refreshes.get(failure_key(server_id, remote_path))
                    if record is None or record.get('queued_id') != retry_id:
                        continue
            if not os.path.isdir(local_dir):
                clear_failed_refresh(remote_path, '本地目录已删除或移走', server_id)
                continue
            actual_grpc_refresh(local_dir, remote_path, stats, server_id)
            time.sleep(1)
        except Exception as e:
            print(f"⚠️ [{server_id}] 队列任务处理异常: {e}")
            if task is not None:
                local_dir, remote_path, stats = task
                remember_failed_refresh(local_dir, remote_path, stats, f'队列异常: {e}', server_id)
        finally:
            refresh_queue.task_done()

# 各 CD2 的 worker 在主程序中启动；复用同一份本地监听和文件稳定检测。

# 统一的防抖调度中心 (事件循环)
def debounce_dispatcher():
    """单线程循环扫描时间戳字典，代替原先无数个 Timer 线程"""
    while True:
        time.sleep(1)
        now = time.time()
        to_trigger = []
        with _stats_lock:
            # 遍历寻找所有到期的防抖任务
            for (server_id, remote_path), (target_time, local_dir) in list(debounce_targets.items()):
                if now >= target_time:
                    to_trigger.append((local_dir, remote_path, server_id))
                    del debounce_targets[(server_id, remote_path)]


        # 释放锁之后再推进队列，防止阻塞
        for l_dir, r_path, server_id in to_trigger:
            push_to_queue(l_dir, r_path, server_id)

        # 复用现有调度循环，不为失败目录创建新线程，也不阻塞正常任务。
        dispatch_failed_refreshes()

# 启动事件循环
dispatcher_thread = threading.Thread(target=debounce_dispatcher, daemon=True)
dispatcher_thread.start()

def schedule_file_settle(path, action):
    """登记文件并等待其真正写完；重复事件只重置稳定计数，不创建线程。"""
    now = time.time()
    with _stats_lock:
        old = settling_files.get(path)
        # created/moved_dest 比后续 modified 更能表达这次变动的真实含义。
        if old and old['action'] in ('created', 'moved_dest') and action == 'modified':
            action = old['action']
        settling_files[path] = {
            'action': action,
            'first_seen': old['first_seen'] if old else now,
            'last_size': None,
            'last_mtime_ns': None,
            'stable_checks': 0,
        }

def file_settle_dispatcher():
    """轮询待写文件，大小非零且连续稳定后才产生一次目录刷新事件。"""
    while True:
        time.sleep(FILE_SETTLE_INTERVAL)
        now = time.time()
        ready = []
        expired = []

        with _stats_lock:
            paths = list(settling_files)

        for path in paths:
            try:
                stat_result = os.stat(path)
                size = stat_result.st_size
                mtime_ns = stat_result.st_mtime_ns
            except (FileNotFoundError, OSError):
                size = None
                mtime_ns = None

            with _stats_lock:
                state = settling_files.get(path)
                if state is None:
                    continue

                if now - state['first_seen'] >= FILE_SETTLE_TIMEOUT:
                    expired.append(path)
                    settling_files.pop(path, None)
                    continue

                if size is None or size == 0:
                    state['stable_checks'] = 0
                    state['last_size'] = size
                    state['last_mtime_ns'] = mtime_ns
                    continue

                if size == state['last_size'] and mtime_ns == state['last_mtime_ns']:
                    state['stable_checks'] += 1
                else:
                    state['stable_checks'] = 0
                    state['last_size'] = size
                    state['last_mtime_ns'] = mtime_ns

                if state['stable_checks'] >= FILE_SETTLE_CHECKS:
                    ready.append((path, state['action'], size))
                    settling_files.pop(path, None)

        if expired:
            for path in expired:
                print(f"⚠️ 文件等待写入超时，未触发刷新: {path}")

            if NOTIFY_FILE_SETTLE_TIMEOUT:
                shown_paths = expired[:FILE_TIMEOUT_NOTIFY_LIMIT]
                path_lines = "\n".join(f"- {path}" for path in shown_paths)
                omitted_count = len(expired) - len(shown_paths)
                if omitted_count > 0:
                    path_lines += f"\n- 另有 {omitted_count} 个文件未列出"

                timeout_minutes = round(FILE_SETTLE_TIMEOUT / 60, 1)
                content = (
                    f"共有 {len(expired)} 个文件等待写入超过 "
                    f"{FILE_SETTLE_TIMEOUT} 秒（约 {timeout_minutes} 分钟），"
                    f"已放弃本次目录刷新。\n"
                    f"请检查文件是否一直为 0KB、已被移走或挂载盘是否异常：\n"
                    f"{path_lines}"
                )
                try:
                    safe_send("CD2 文件写入超时", content)
                except Exception as notify_err:
                    print(f"⚠️ 文件写入超时通知发送失败: {notify_err}")

        for path, action, size in ready:
            parent_dir = os.path.dirname(path)
            if not os.path.isdir(parent_dir) or not get_mapped_remote_path(parent_dir):
                continue
            print(f"✅ 文件已稳定 ({size} bytes)，安排刷新: {path}")
            record_stat(parent_dir, action)
            trigger_refresh(parent_dir)

            if REFRESH_FILE_GRANDPARENT:
                grandparent_dir = os.path.dirname(parent_dir)
                if get_mapped_remote_path(grandparent_dir):
                    record_stat(grandparent_dir, action)
                    trigger_refresh(grandparent_dir)

settle_thread = threading.Thread(target=file_settle_dispatcher, daemon=True)
settle_thread.start()

# 每台 CD2 拥有独立连接、token 和空闲定时器。
_channel_lock = threading.Lock()
_grpc_connections = {
    name: {'channel': None, 'stub': None, 'timer': None, 'generation': 0}
    for name in SERVERS
}


def close_grpc_connection(server_id=None, idle_generation=None):
    """指定名称只关闭该台；省略名称用于退出时关闭全部连接。"""
    with _channel_lock:
        names = [server_id] if server_id is not None else list(_grpc_connections)
        for name in names:
            state = _grpc_connections[name]
            if idle_generation is not None and state['generation'] != idle_generation:
                continue  # 已取消的旧定时器不能关闭新连接
            state['generation'] += 1
            if state['timer'] is not None:
                state['timer'].cancel()
                state['timer'] = None
            if state['channel'] is not None:
                try:
                    state['channel'].close()
                except Exception:
                    pass
                state['channel'] = None
                state['stub'] = None
                print(f"\n🔌 [{name}] gRPC 连接已关闭并清理。\n")


def get_grpc_stub(server_id=None):
    """获取指定 CD2 的连接与独立认证信息。"""
    server_id = server_id or DEFAULT_SERVER_ID
    config = SERVERS[server_id]
    with _channel_lock:
        state = _grpc_connections[server_id]
        if state['timer'] is not None:
            state['timer'].cancel()
        state['generation'] += 1
        state['timer'] = threading.Timer(
            CONNECTION_IDLE_TIMEOUT, close_grpc_connection,
            args=(server_id, state['generation']))
        state['timer'].daemon = True
        state['timer'].start()
        if state['channel'] is None:
            print(f"🔌 [{server_id}] 正在建立新的 gRPC 连接: {config['host']}")
            if config['use_https']:
                credentials = grpc.ssl_channel_credentials()
                state['channel'] = grpc.secure_channel(config['host'], credentials)
            else:
                state['channel'] = grpc.insecure_channel(config['host'])
            state['stub'] = clouddrive_pb2_grpc.CloudDriveFileSrvStub(state['channel'])
        return state['stub'], (('authorization', config['token']),)


def get_mapped_remote_path(local_path):
    """所有 CD2 共用映射，按目录边界匹配。"""
    local_path_norm = local_path.replace("\\", "/").rstrip("/")
    for left_base, right_base in MAPPINGS:
        left_base_norm = left_base.rstrip("/")
        if local_path_norm == left_base_norm or local_path_norm.startswith(left_base_norm + "/"):
            return local_path_norm.replace(left_base_norm, right_base.rstrip("/"), 1)
    return None

def actual_grpc_refresh(local_dir, remote_path, stats, server_id=None):
    """真正执行指定 CD2 的 gRPC 刷新。"""
    server_id = server_id or DEFAULT_SERVER_ID
    config = SERVERS[server_id]
    pending_key = (server_id, local_dir)

    def log(message):
        # 多台同时刷新时，每条日志带名称，便于对应结果。
        print(f'[{server_id}] {message}', flush=True)

    # 动态获取统计，容错并合并 moved_dest
    created_cnt = stats.get('created', 0)
    deleted_cnt = stats.get('deleted', 0)
    # 一次重命名会记录 moved_from 和 moved_dest 两个端点，展示时按一次操作计数。
    moved_cnt = (stats.get('moved', 0)
                 + max(stats.get('moved_from', 0), stats.get('moved_dest', 0)))
    modified_cnt = stats.get('modified', 0)
    start_time = stats.get('start_time', time.time())

    # 计算耗时 (如果发生了倒挂重排，这里会算出真实的累加耗时)
    wait_duration = round(time.time() - start_time, 2)
    total_changes = created_cnt + deleted_cnt + moved_cnt + modified_cnt

    time_str = time.strftime('%H:%M:%S')

    log(f"\n{'='*40}")
    log(f"[{time_str}] 🚀 执行队列中的目录刷新任务")
    log(f"本地父级目录: {local_dir}")
    log(f"远端刷新目标: {remote_path}")
    log(f"从变动到执行耗时: {wait_duration} 秒")
    log(f"累计变动事件: {total_changes} 次 (新增:{created_cnt} 删除:{deleted_cnt} 移动:{moved_cnt} 修改:{modified_cnt})")
    log(f"当前 CD2 队列排队剩余: {refresh_queues[server_id].qsize()} 个任务")

    MAX_RETRIES = 3   # 最大重试次数
    RETRY_DELAY = 1   # 每次失败后等待 1 秒再试

    for attempt in range(MAX_RETRIES):
        if attempt > 0:
            log(f"🔄 正在进行第 {attempt + 1}/{MAX_RETRIES} 次重试...")
        else:
            protocol_str = "HTTPS" if config['use_https'] else "HTTP"
            log(f"正在向 {config['host']} ({protocol_str}) 发送 gRPC 刷新指令...")

        try:
            # 从管理器获取 stub 和 metadata，确保连接可用
            stub, metadata = get_grpc_stub(server_id)

            req = clouddrive_pb2.ListSubFileRequest(
                path=remote_path,
                forceRefresh=True
            )

            # 发起调用并计算返回的数据包数量，带 30秒 超时强制阻断
            response_count = sum(1 for _ in stub.GetSubFiles(req, metadata=metadata, timeout=30))

            # 任意一次成功（包括正常文件事件）都会清除该目录的失败记录。
            clear_failed_refresh(remote_path, server_id=server_id)
            log(f"✅ 服务器响应: 已成功执行 GetSubFiles (forceRefresh=True)")
            log(f"本次请求共收到 {response_count} 个数据包")
            log(f"{'='*40}\n")
            break

        except grpc.RpcError as e:
            details = e.details() or ""
            code = e.code()

            # 智能区分“真删除”与“时序倒挂”
            if code == grpc.StatusCode.NOT_FOUND or "文件不存在或已删除" in details:
                if os.path.exists(local_dir):
                    # 限制最大倒挂重试次数，防止网盘忽略非法字符文件夹导致的无限死锁
                    inv_retries = stats.get('inversion_retries', 0)
                    if inv_retries < 3 and stats.get('_failed_retry_id') is None:
                        log(f"🔄 时序倒挂保护 (第{inv_retries+1}/3次): 父级目录尚未刷新完毕。将该目录重新推回防抖队列，延迟重试...")
                        # 💡安全地将统计数据原封不动塞回去合并
                        with _stats_lock:
                            stats['inversion_retries'] = inv_retries + 1
                            if pending_key not in pending_stats:
                                pending_stats[pending_key] = stats
                            else:
                                for k, v in stats.items():
                                    if k not in ['start_time', 'inversion_retries']:
                                        pending_stats[pending_key][k] = pending_stats[pending_key].get(k, 0) + v
                                pending_stats[pending_key]['inversion_retries'] = max(
                                    pending_stats[pending_key].get('inversion_retries', 0),
                                    stats['inversion_retries'])

                        # 先补刷父目录，让 CD2 重新发现这个子目录
                        parent_dir = os.path.dirname(local_dir)
                        if get_mapped_remote_path(parent_dir) and os.path.isdir(parent_dir):
                            record_stat(parent_dir, 'retry_parent', server_id)
                            trigger_refresh(parent_dir, server_id)

                        # 再安排当前目录重试
                        trigger_refresh(local_dir, server_id)
                    else:
                        # 本地仍存在的 NOT_FOUND 也属于最终失败，不能静默丢弃。
                        remember_failed_refresh(local_dir, remote_path, stats,
                                                f'{code.name}: {details}', server_id)
                else:
                    # 情况 B：本地也没有 -> 真正的删除或移出，无需重试。
                    clear_failed_refresh(remote_path, '本地目录已删除或移走', server_id)
                    log("💡 智能拦截: 目标目录已不存在(本地已删除/移走)，正常现象，已拦截报警。")
                log(f"{'='*40}\n")
                break

            # 如果遇到网络断开、超时、服务端不可用等异常，强制销毁当前长连接
            if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.INTERNAL):
                log(f"🔌 检测到网络连接断开或异常 ({code.name})，正在销毁并准备重建 gRPC 连接...")
                close_grpc_connection(server_id)

            err_msg = f"❌ gRPC 通信失败 (状态码: {code.name})\n详情: {details}"
            log(f"⚠️ 刷新失败: {code.name}")

            # 判断是否还有重试机会
            if attempt < MAX_RETRIES - 1:
                log(f"⏳ 等待 {RETRY_DELAY} 秒后重试...")
                time.sleep(RETRY_DELAY)
            else:
                remember_failed_refresh(local_dir, remote_path, stats,
                                        f'连续 {MAX_RETRIES} 次失败，{code.name}: {details}', server_id)
                log(f"{'='*40}\n")

        except Exception as e:
            err_msg = f"⚠️ gRPC 发生未知异常: {e}"
            log(err_msg)

            log("🔌 发生未知异常，销毁当前连接以防卡死...")
            close_grpc_connection(server_id)

            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                remember_failed_refresh(local_dir, remote_path, stats,
                                        f'连续 {MAX_RETRIES} 次未知异常: {e}', server_id)
                log(f"{'='*40}\n")

def push_to_queue(local_dir, remote_path, server_id=None):
    """按 CD2 提取防抖统计，推入该台的深度优先队列。"""
    global _task_counter
    server_id = server_id or DEFAULT_SERVER_ID
    with _stats_lock:
        stats = pending_stats.pop((server_id, local_dir), {})
        _task_counter += 1
        counter = _task_counter
    if not stats or not os.path.isdir(local_dir):
        return
    depth = len(remote_path.strip('/').split('/'))
    refresh_queues[server_id].put((depth, counter, (local_dir, remote_path, stats)))

def trigger_refresh(local_dir, server_id=None):
    """普通事件刷新全部 CD2；失败恢复只重试指定 CD2。"""
    remote_path = get_mapped_remote_path(local_dir)
    if not remote_path:
        return
    names = [server_id] if server_id is not None else SERVERS
    with _stats_lock:
        for name in names:
            debounce_targets[(name, remote_path)] = (
                time.time() + DEBOUNCE_SECONDS, local_dir)


def record_stat(local_dir, action, server_id=None):
    """普通事件分发给全部 CD2，统计和失败重试仍按服务隔离。"""
    if not get_mapped_remote_path(local_dir):
        return
    names = [server_id] if server_id is not None else SERVERS
    with _stats_lock:
        for name in names:
            key = (name, local_dir)
            if key not in pending_stats:
                pending_stats[key] = {'start_time': time.time()}
            pending_stats[key][action] = pending_stats[key].get(action, 0) + 1

def _is_same_or_child(path, root_path):
    """判断 path 是否等于 root_path，或位于其目录树下。"""
    path_norm = os.path.normpath(path)
    root_norm = os.path.normpath(root_path)
    return path_norm == root_norm or path_norm.startswith(root_norm + os.sep)

def cancel_pending_subtree(root_path):
    """取消已删除/移走目录自身及所有下级目录的待刷新状态。"""
    with _stats_lock:
        for key in list(pending_stats):
            if _is_same_or_child(key[1], root_path):
                pending_stats.pop(key, None)

        for remote_path, (_, local_dir) in list(debounce_targets.items()):
            if _is_same_or_child(local_dir, root_path):
                debounce_targets.pop(remote_path, None)

        for path in list(settling_files):
            if _is_same_or_child(path, root_path):
                settling_files.pop(path, None)

        # 目录已删除或移走时，同步取消其延迟复查，防止误报。
        removed = False
        for remote_path, record in list(failed_refreshes.items()):
            if _is_same_or_child(record['local_dir'], root_path):
                failed_refreshes.pop(remote_path, None)
                removed = True
        if removed:
            _save_failed_refreshes_locked()

def nearest_existing_mapped_dir(local_dir):
    """向上寻找仍存在且位于映射范围内的最近目录。"""
    current = os.path.normpath(local_dir)
    while get_mapped_remote_path(current):
        if os.path.isdir(current):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return None

class SmartWatcher(FileSystemEventHandler):
    @staticmethod
    def _event_log(action, src_path, dest_path=None, is_directory=False):
        """输出原始事件，便于确认挂载层实际上报了什么。"""
        kind = "目录" if is_directory else "文件"
        if dest_path:
            print(f"📌 [{action}] {kind}: {src_path} -> {dest_path}")
        else:
            print(f"📌 [{action}] {kind}: {src_path}")

    def process_path(self, path, action, is_directory=False):
        """
        处理路径变动：智能区分文件与文件夹，避免过度级联刷新
        """
        # 1. 物理探测容错：挂载盘下新建/移动文件夹时 is_directory 常误报为 False
        if action in ['created', 'moved_dest'] and os.path.isdir(path):
            is_directory = True

        # 文件新建/写入/移入不能立即刷新：FUSE 常先创建 0KB 占位文件，且未必
        # 保证后续一定上报 modified。统一交给稳定性轮询，避免早刷和漏刷。
        if not is_directory and action in ['created', 'modified', 'moved_dest']:
            schedule_file_settle(path, action)
            print(f"⏳ 等待文件写入稳定: {path}")
            return

        # 获取直接父目录。删除/移出整棵目录时，中间父目录可能也已经消失，
        # 此时一路折叠到仍然存在的最近父目录，避免刷新整串无效路径。
        parent_dir = os.path.dirname(path)
        if action in ['deleted', 'moved_from']:
            parent_dir = nearest_existing_mapped_dir(parent_dir)
            if not parent_dir:
                return

        # 触发 1：刷新直接父目录
        record_stat(parent_dir, action)
        trigger_refresh(parent_dir)

        # 触发 2（可选）：部分挂载盘只上报内部文件、不上报新建/移入的文件夹，
        # 这种环境才需要连带刷新爷爷目录。默认关闭，避免普通文件变动多刷一层。
        if REFRESH_FILE_GRANDPARENT and not is_directory:
            grandparent_dir = os.path.dirname(parent_dir)
            if get_mapped_remote_path(grandparent_dir):
                record_stat(grandparent_dir, action)
                trigger_refresh(grandparent_dir)

        # 触发 3：深度感知逻辑：如果移入的是【文件夹】，将自身及底层所有子文件夹加入队列
        # 注意：如果是 deleted（删除），就不需要遍历了，因为本地已经没这个目录了
        if is_directory and action in ['created', 'moved_dest']:
            if os.path.exists(path):
                # 顺便把这个文件夹自身也加入刷新列表
                record_stat(path, action)
                trigger_refresh(path)

                # 递归扫描底层所有子文件夹
                try:
                    for root_dir, dirs, files in os.walk(path):
                        for d in dirs:
                            sub_dir = os.path.join(root_dir, d)
                            # 将所有深层子文件夹加入防抖队列
                            record_stat(sub_dir, action)
                            trigger_refresh(sub_dir)
                except Exception as e:
                    print(f"⚠️ 递归扫描深层目录失败: {e}")

    def on_created(self, event):
        self._event_log('created', event.src_path,
                        is_directory=event.is_directory)
        self.process_path(event.src_path, 'created', event.is_directory)

    def on_deleted(self, event):
        self._event_log('deleted', event.src_path,
                        is_directory=event.is_directory)
        if event.is_directory:
            cancel_pending_subtree(event.src_path)
        else:
            with _stats_lock:
                settling_files.pop(event.src_path, None)
        self.process_path(event.src_path, 'deleted', event.is_directory)

    def on_moved(self, event):
        # 部分 FUSE 挂载会把目录重命名误报为文件；目标仍存在时可据此纠正。
        is_directory = event.is_directory or os.path.isdir(event.dest_path)
        self._event_log('renamed', event.src_path, event.dest_path,
                        is_directory=is_directory)

        if is_directory:
            cancel_pending_subtree(event.src_path)

        # 将移动事件拆解为：原路径(当做删除处理) + 新路径(当做移入处理)
        # 无论同目录改名还是跨目录移动，都会刷新旧父目录和新父目录。
        self.process_path(event.src_path, 'moved_from', is_directory)
        self.process_path(event.dest_path, 'moved_dest', is_directory)

    def on_modified(self, event):
        # 文件夹自身的属性(修改时间等)被修改，无需触发刷新
        if event.is_directory:
            return
        self._event_log('modified', event.src_path, is_directory=False)
        self.process_path(event.src_path, 'modified', False)


if __name__ == "__main__":
    print(f"✅ [启动 4/4] 进入主程序，Python: {sys.version.split()[0]}", flush=True)
    load_failed_refreshes()
    for server_id in SERVERS:
        worker = threading.Thread(target=grpc_worker, args=(server_id,),
                                  name=f'grpc:{server_id}', daemon=True)
        worker_threads[server_id] = worker
        worker.start()
    # 使用系统原生文件事件（Linux 下为 inotify），无周期性目录扫描。
    watcher = SmartWatcher()
    observers = []

    # 不再监听整个根目录，改为精准监听每个映射的本地目录 ===
    watch_count = 0
    mapping_logs = []

    for local_path, remote_path in MAPPINGS:
        # 先输出再访问挂载点：若失效的 FUSE 挂载卡住，可从最后一行看出具体路径。
        print(f"🔎 正在检查监控目录: {local_path}", flush=True)
        try:
            if os.path.isdir(local_path):
                # 每个挂载点使用独立 Observer，避免一个超大或失效挂载阻塞其他盘。
                observer = Observer()
                observer.schedule(watcher, local_path, recursive=True)
                observer_state = {
                    'local_path': local_path,
                    'remote_path': remote_path,
                    'observer': observer,
                    'started': threading.Event(),
                    'error': None,
                    'start_time': None,
                    'warning_sent': False,
                }
                observers.append(observer_state)
                watch_count += 1
                mapping_logs.append(f"✅ 已登记: {local_path} => {remote_path}")
                print(f"✅ 目录检查通过并已登记: {local_path}", flush=True)
            else:
                # 如果目录不存在，做个记录但不会导致程序崩溃
                mapping_logs.append(f"❌ 未找到: {local_path} => {remote_path} (已跳过该目录)")
                print(f"⚠️ 目录不存在或不是文件夹，已跳过: {local_path}", flush=True)
        except Exception as e:
            mapping_logs.append(f"❌ 监听失败: {local_path} => {remote_path} ({e})")
            print(f"❌ 检查或登记监控目录失败: {local_path}: {e}", flush=True)
            traceback.print_exc()

    if watch_count == 0:
        err_msg = "没有任何有效的本地监控目录存在，请检查配置或挂载状态！"
        print(f"致命错误：{err_msg}", flush=True)
        safe_send("CD2 监控启动失败", err_msg)
        exit(1)

    def start_observer(observer_state):
        """单独启动一个挂载点；递归建立 inotify 时不阻塞其他挂载点。"""
        local_path = observer_state['local_path']
        observer_state['start_time'] = time.time()
        print(f"🔧 正在为该目录递归建立监听: {local_path}", flush=True)
        try:
            observer_state['observer'].start()
            observer_state['started'].set()
            duration = round(time.time() - observer_state['start_time'], 2)
            print(f"✅ watchdog 监听启动成功 ({duration} 秒): {local_path}", flush=True)
        except Exception as e:
            observer_state['error'] = e
            print(f"❌ watchdog 监听启动失败: {local_path}\n"
                  f"   {type(e).__name__}: {e}", flush=True)
            traceback.print_exc()

    print("🔧 正在分别启动各网盘的 watchdog 监听线程...", flush=True)
    for observer_state in observers:
        threading.Thread(
            target=start_observer,
            args=(observer_state,),
            name=f"watch-start:{observer_state['local_path']}",
            daemon=True,
        ).start()

    # 给正常挂载一个很短的启动窗口；超大目录继续在后台初始化，不挡住主程序。
    startup_deadline = time.time() + 3
    while time.time() < startup_deadline:
        if all(state['started'].is_set() or state['error'] is not None
               for state in observers):
            break
        time.sleep(0.1)

    started_count = sum(state['started'].is_set() for state in observers)
    pending_count = watch_count - started_count - sum(
        state['error'] is not None for state in observers)
    print(f"📊 watchdog 启动状态: 已启动 {started_count}，"
          f"仍在初始化 {pending_count}，总计 {watch_count}", flush=True)

    # 格式化输出漂亮的启动日志
    mapping_str = "\n".join(mapping_logs)
    start_msg = (
        f"🚀 CD2 智能目录刷新监控已启动\n"
        f"{'='*40}\n"
        f"【监控目录与映射规则】\n"
        f"{mapping_str}\n"
        f"{'='*40}\n"
        f"⚙️ 运行参数：\n"
        f"- 监听方式: 系统原生事件 (native)\n"
        f"- 文件变动额外刷新爷爷目录: {'开启' if REFRESH_FILE_GRANDPARENT else '关闭'}\n"
        f"- 防抖等待: {DEBOUNCE_SECONDS} 秒 (已启用事件循环护航)\n"
        f"- 文件稳定检测: 每 {FILE_SETTLE_INTERVAL} 秒检查，连续 {FILE_SETTLE_CHECKS} 次稳定\n"
        f"- CD2 服务: {', '.join(name + ' (' + config['host'] + ')' for name, config in SERVERS.items())}\n"
        f"- 队列机制: 基于树状深度的优先队列已启用\n"
        f"- 最终失败复查: 每 {FAILED_REFRESH_RETRY_SECONDS // 60} 分钟，复查失败继续告警\n"
        f"- 失败记录文件: {FAILED_REFRESH_FILE}\n"
        f"已登记 {watch_count} 个本地目录，其中 {started_count} 个已完成监听初始化，"
        f"{pending_count} 个仍在递归建立监听。"
    )

    print(start_msg, flush=True)
    safe_send("CD2 监控任务上线", start_msg)

    try:
        last_health_check = time.time()
        while True:
            time.sleep(1)
            if time.time() - last_health_check >= 30:
                last_health_check = time.time()
                dead_threads = []
                watch_errors = []
                for state in observers:
                    local_path = state['local_path']
                    if state['error'] is not None:
                        watch_errors.append(
                            f"{local_path}: {type(state['error']).__name__}: {state['error']}"
                        )
                    elif state['started'].is_set():
                        if not state['observer'].is_alive():
                            dead_threads.append(f"watchdog({local_path})")
                    elif (state['start_time'] is not None
                          and time.time() - state['start_time'] >= WATCH_START_WARNING_SECONDS
                          and not state['warning_sent']):
                        state['warning_sent'] = True
                        elapsed = round(time.time() - state['start_time'])
                        warning = (
                            f"⚠️ watchdog 初始化已超过 {elapsed} 秒: {local_path}\n"
                            "该目录可能子目录过多、挂载响应缓慢，或正在接近 inotify 限额；"
                            "其他网盘不受影响。"
                        )
                        print(warning, flush=True)
                        safe_send("CD2 监听初始化缓慢", warning)
                if watch_errors:
                    raise RuntimeError("监听启动失败: " + " | ".join(watch_errors))
                for server_id, worker in worker_threads.items():
                    if not worker.is_alive():
                        dead_threads.append(f"gRPC队列({server_id})")
                if not dispatcher_thread.is_alive():
                    dead_threads.append("防抖调度")
                if not settle_thread.is_alive():
                    dead_threads.append("文件稳定检测")
                if dead_threads:
                    raise RuntimeError(f"后台线程已停止: {', '.join(dead_threads)}")
    except KeyboardInterrupt:
        _retry_stopping.set()
        print("\n停止监控并等待队列清空...")
        for state in observers:
            if state['started'].is_set():
                state['observer'].stop()

        # 退出时清理长连接
        close_grpc_connection()

        # 等待队列中的剩余任务处理完毕再退出 (注入退出信号)
        for refresh_queue in refresh_queues.values():
            refresh_queue.put((float('inf'), 0, None))
        for worker in worker_threads.values():
            worker.join()
        close_grpc_connection()
    except Exception as e:
        _retry_stopping.set()
        print(f"\n❌ 监控主循环异常退出: {e}", flush=True)
        traceback.print_exc()
        safe_send("CD2 监控运行异常", str(e))
        for state in observers:
            if state['started'].is_set():
                state['observer'].stop()
        close_grpc_connection()
        raise

    for state in observers:
        if state['started'].is_set():
            state['observer'].join()

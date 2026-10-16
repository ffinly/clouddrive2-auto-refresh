# CloudDrive2 Auto Refresh

[简体中文](README.md) | [繁體中文](README.zh-TW.md) | **English**

---

🚀 A CloudDrive2 directory auto-refresh tool. Monitor local file changes and coordinate refreshes across multiple servers, with path mapping, write-stability checks, event debouncing, automatic failure rechecks, and Qinglong notifications.

When a file or directory changes under a local mount point, the script calls `GetSubFiles(forceRefresh=True)` via gRPC to refresh the corresponding directory on each CD2 server. All enabled servers share the same path mappings, while each server has its own connection and task queue.

## ✨ Features

- 👀 Recursively monitor file and directory creation, deletion, moves, and modifications with watchdog.
- 🖼️ Wait until files are non-zero and their size and modification time remain stable before scheduling a refresh. This handles images and other files created as 0KB placeholders before data is written.
- ⏳ Merge and debounce events for the same directory. Prioritize parent directories by directory depth.
- 🖥️ Refresh multiple CD2 servers from one local change, with independent failure handling and retries for each server.
- 🔁 Persist failed refresh targets and recheck them every 10 minutes by default. Restore pending failures after a restart.
- 🔔 Use Qinglong's `notify.send` when available; otherwise print notifications to the console.
- 🩺 Show staged startup logs and check the health of watcher, queue, and dispatcher threads.

## 📁 Files

```text
clouddrive2-auto-refresh/
├── clouddrive2_auto_refresh.py  # Main script; server and path settings are at the top
├── clouddrive.proto            # CloudDrive2 protocol definition
├── requirements.txt           # Runtime and protocol-generation dependencies
├── LICENSE                    # GNU GPL v3.0 license
├── README.md               # Simplified Chinese (default)
├── README.zh-TW.md         # Traditional Chinese
└── README.en.md            # English
```

The repository contains these files. At runtime, the script creates `cd2_failed_refreshes.json` beside the main script, so that directory must be writable. Generated modules, failure records, logs, and `__pycache__` are local runtime files; when submitting changes, select only project source files.

## 🚀 Usage

### 1. Install dependencies and generate the gRPC modules

From the project directory, run these commands in the Python environment that will run the script:

```bash
python3 -m pip install --upgrade -r requirements.txt
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto
```

The second command generates `clouddrive_pb2.py` and `clouddrive_pb2_grpc.py` in the current directory. Keep both files beside the main script. Generate them in your own runtime environment; they are not committed to the repository.

In Qinglong, you can also install `watchdog`, `grpcio`, `grpcio-tools`, and `protobuf` under Dependency Management → Python3, then open a terminal in the script directory and run the generation command. Generate both modules before the first run, and regenerate them after changing the protocol or Python environment.

### 2. Configure CD2 servers

Edit `CD2_SERVERS` at the top of `clouddrive2_auto_refresh.py`:

```python
CD2_SERVERS = [
    {
        'name': 'CD2-1',
        'enabled': True,
        'host': 'cd2-1.example.com:443',
        'use_https': True,
        'token': 'PASTE_THIS_SERVER_API_TOKEN_HERE',
    },
]
```

| Setting | Description |
| --- | --- |
| `name` | A unique name for the server, used in logs, notifications, and failure records |
| `enabled` | Whether to enable this server; enable at least one |
| `host` | gRPC address as `hostname:port` or `IP:port`, without a scheme or path |
| `use_https` | `True` for TLS, `False` for a plaintext connection |
| `token` | This server's API token; the script validates its UUID format and accepts an optional `Bearer ` prefix |

To use multiple servers, duplicate the configuration entry and provide each server's address and token. The repository's sample entries are disabled and their tokens are empty. Do not commit personal tokens to a public repository.

### 3. Configure path mappings

```python
RAW_RULES = """
/ql/115open/CD2 => /115open/CD2
"""
```

The left side is a local mount directory accessible to the machine running the script. The right side is the internal CD2 directory. The right-side name and hierarchy must be identical on every enabled server. Use one mapping per line. When mappings overlap, put the more specific subdirectory first.

Configure caching on each CD2 server as described in the script:

1. Under Settings → System → Directory Cache, enable cache persistence.
2. Open Properties → Cache Management for each directory to refresh, and set its cache duration to 0 seconds.

### 4. Start monitoring

```bash
python3 -u clouddrive2_auto_refresh.py
```

Qinglong task command:

```bash
task clouddrive2_auto_refresh.py
```

This is a long-running monitor. Keep only one instance, avoid overlapping instances started by recurring tasks, and ensure the runtime does not automatically terminate long-running jobs. The script monitors events that occur after startup; it does not automatically refresh all directory differences that already existed before startup.

## ⚙️ Default Settings

| Setting | Default | Purpose |
| --- | --- | --- |
| `DEBOUNCE_SECONDS` | `10` | Queue a directory 10 seconds after its latest event |
| `FILE_SETTLE_INTERVAL` | `1` | Check pending files once per second |
| `FILE_SETTLE_CHECKS` | `3` | Require 3 consecutive unchanged size/mtime checks; the first non-zero sample only establishes a baseline |
| `FILE_SETTLE_TIMEOUT` | `300` | Wait up to 5 minutes for a pending file |
| `NOTIFY_FILE_SETTLE_TIMEOUT` | `True` | Notify when the write-stability wait times out |
| `FILE_TIMEOUT_NOTIFY_LIMIT` | `20` | List up to 20 files in each timeout notification |
| `REFRESH_FILE_GRANDPARENT` | `False` | Whether to additionally refresh a file's grandparent directory |
| `CONNECTION_IDLE_TIMEOUT` | `300` | Close a gRPC connection after 5 idle minutes |
| `WATCH_START_WARNING_SECONDS` | `30` | Warn when watcher initialization exceeds this threshold |
| `FAILED_REFRESH_RETRY_SECONDS` | `600` | Recheck a target every 10 minutes after final failure |
| `NOTIFY_FIRST_REFRESH_FAILURE` | `False` | Record the first final failure without alerting; alert if a delayed recheck also fails |

If a file remains at 0KB or cannot be read, its stability counter stays at 0. After a timeout, that pending wait is removed and its path is logged; a later event can register a new wait. Stable size and modification time are only local observations. Upload completion and remote index visibility still depend on the cloud drive and CD2.

A successful refresh completes that task; no fixed follow-up refresh is scheduled by default. A failed refresh is attempted up to 3 times. If it still fails, the target is saved for periodic rechecks. Missing directories and similar cases follow the script's parent-directory and deletion-handling logic.

## 🧩 Update the Protocol and Regenerate Modules

See the [official CloudDrive2 gRPC API guide](https://www.clouddrive2.com/api/CloudDrive2_gRPC_API_Guide.html) for protocol and API usage. The bundled `clouddrive.proto` declares protocol version `1.1.2`. Use a protocol compatible with your server's API; the protocol version is not the CD2 application version.

When updating, save the new protocol as `clouddrive.proto`. This filename produces the `clouddrive_pb2` modules imported by the main script.

```bash
python3 -m pip install --upgrade -r requirements.txt
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto
```

Run these commands in the project directory and regenerate both modules together. Use the same Python environment for generation and execution. If an import reports incompatible versions, update dependencies and regenerate the modules.

## 🔍 Troubleshooting

- **The script requires at least one enabled CD2 server:** Provide an address and token, and set `enabled` to `True`.
- **Protocol modules cannot be imported:** Keep both generated files beside the script and check that dependency versions meet the generated code's requirements.
- **Authentication or permission errors:** Check each server's token, authorized paths, and directory-listing permission. Follow the official guide.
- **No changes are detected:** Confirm that watcher initialization finished for the mapped directory. The script relies on native filesystem events; verify that your mount method generates them.
- **Watcher initialization is slow:** Check mount availability, the number of subdirectories, and Linux inotify limits. Each mapped directory initializes independently.
- **An image remains at 0KB:** After 5 minutes, the script logs a write timeout and notifies if configured. Check upload progress and mount status.
- **No phone notifications arrive:** Configure Qinglong's `notify` channels. Outside Qinglong, notifications are printed to the console by default.

## 📈 Star History

<a href="https://www.star-history.com/?repos=ffinly%2Fclouddrive2-auto-refresh&amp;type=date&amp;legend=top-left">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=ffinly/clouddrive2-auto-refresh&amp;type=date&amp;theme=dark&amp;legend=top-left" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=ffinly/clouddrive2-auto-refresh&amp;type=date&amp;legend=top-left" />
    <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=ffinly/clouddrive2-auto-refresh&amp;type=date&amp;legend=top-left" />
  </picture>
</a>

## 📄 License

Original code in this project is licensed under the [GNU General Public License v3.0](LICENSE) (GPL-3.0-only).

`clouddrive.proto` is an upstream CloudDrive2 protocol file and retains its original rights and licensing.

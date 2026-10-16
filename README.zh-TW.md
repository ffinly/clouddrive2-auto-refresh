# CloudDrive2 Auto Refresh

[简体中文](README.md) | **繁體中文** | [English](README.en.md)

---

🚀 CloudDrive2 目錄自動刷新工具。監控本地文件變動，支持多服務器聯動、路徑映射、寫入穩定檢測、防抖隊列、失敗自動複查與青龍通知。

文件或目錄在本地掛載點發生變動後，腳本通過 gRPC 調用 `GetSubFiles(forceRefresh=True)`，刷新各臺 CD2 上對應目錄。所有啓用的服務共用路徑映射，每臺擁有獨立連接和任務隊列。

## ✨ 功能

- 👀 通過 watchdog 遞歸監聽目錄的新增、刪除、移動及修改事件。
- 🖼️ 等待文件非零且大小、修改時間連續穩定後安排刷新，適用於先創建 0KB 佔位文件再寫入的圖片等文件。
- ⏳ 同目錄事件合併防抖，按目錄深度優先處理父目錄。
- 🖥️ 同一次本地變動刷新多臺 CD2，各臺獨立處理失敗與重試。
- 🔁 最終失敗記錄落盤，默認每 10 分鐘複查，重啓後恢復失敗目標。
- 🔔 支持青龍 `notify.send`；其他環境輸出通知到控制檯。
- 🩺 輸出分階段啓動日誌，並檢查監聽、隊列及調度線程狀態。

## 📁 文件

```text
clouddrive2-auto-refresh/
├── clouddrive2_auto_refresh.py  # 主腳本，服務器及路徑配置在文件開頭
├── clouddrive.proto         # CloudDrive2 協議定義
├── requirements.txt        # 運行及協議生成依賴
├── LICENSE                 # GNU GPL v3.0 許可證
├── README.md               # 簡體中文（預設）
├── README.zh-TW.md         # 繁體中文
└── README.en.md            # English
```

倉庫包含以上文件。運行時會在腳本目錄生成 `cd2_failed_refreshes.json`，請確保目錄可寫。兩個生成模塊、失敗記錄、日誌和 `__pycache__` 屬於本地文件；如需提交修改，只選擇項目源文件。

## 🚀 使用

### 1. 安裝依賴並生成 gRPC 模塊

進入項目目錄，在運行腳本的 Python 環境中執行：

```bash
python3 -m pip install --upgrade -r requirements.txt
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto
```

第二條命令會在當前目錄生成 `clouddrive_pb2.py` 和 `clouddrive_pb2_grpc.py`，必須與主腳本放在同一個目錄。兩個文件由使用者在運行環境中生成，不提交到倉庫。

在青龍面板中，也可以通過「依賴管理 → Python3」安裝 `watchdog`、`grpcio`、`grpcio-tools` 和 `protobuf`，再通過終端進入腳本目錄執行生成命令。首次部署，以及更新協議或更換 Python 環境後，都先重新生成模塊再啓動監控。

### 2. 配置 CD2 服務器

編輯 `clouddrive2_auto_refresh.py` 開頭的 `CD2_SERVERS`：

```python
CD2_SERVERS = [
    {
        'name': 'CD2-1',
        'enabled': True,
        'host': 'cd2-1.example.com:443',
        'use_https': True,
        'token': '填寫這臺 CD2 的完整 API 令牌',
    },
]
```

| 配置 | 說明 |
| --- | --- |
| `name` | 每臺服務的唯一名稱，用於日誌、通知及失敗記錄 |
| `enabled` | 是否啓用；至少啓用一臺 |
| `host` | gRPC 地址，格式爲 `域名:端口` 或 `IP:端口`，不帶協議或路徑 |
| `use_https` | 使用 TLS 填 `True`，明文連接填 `False` |
| `token` | 該臺 CD2 的 API 令牌；腳本校驗 UUID 格式，可帶 `Bearer ` 前綴 |

需要多臺服務時複製配置條目，每臺填寫自己的地址和令牌。倉庫中的示例默認均未啓用，令牌留空；個人配置中的令牌不要提交到公開倉庫。

### 3. 配置路徑映射

```python
RAW_RULES = """
/ql/115open/CD2 => /115open/CD2
"""
```

左側是腳本所在機器可以訪問的本地掛載目錄，右側是 CD2 內部目錄。右側路徑必須在所有啓用的服務中具有相同名稱與層級。多個目錄每行填寫一條；有重疊時，把更具體的子目錄規則放在前面。

每臺 CD2 按腳本的配置說明設置緩存：

1. 「設置 → 系統 → 目錄緩存」啓用緩存持久化。
2. 對需要刷新的目錄打開「屬性 → 緩存管理」，將緩存時間設爲 0 秒。

### 4. 啓動監控

```bash
python3 -u clouddrive2_auto_refresh.py
```

青龍面板的任務命令：

```bash
task clouddrive2_auto_refresh.py
```

這是持續運行的監控任務。部署時保留一個實例，避免定時任務反覆啓動重疊實例，並確認運行環境不會到時自動終止長任務。腳本啓動後監聽後續事件；啓動前已經存在的目錄差異不會自動全量補刷。

## ⚙️ 默認參數

| 參數 | 默認值 | 作用 |
| --- | --- | --- |
| `DEBOUNCE_SECONDS` | `10` | 同目錄最後一次事件後等待 10 秒入隊 |
| `FILE_SETTLE_INTERVAL` | `1` | 每秒檢查待寫文件 |
| `FILE_SETTLE_CHECKS` | `3` | 連續 3 次檢測到大小與修改時間不變後通過；首次非零採樣只建立基準 |
| `FILE_SETTLE_TIMEOUT` | `300` | 待寫文件最多等待 5 分鐘 |
| `NOTIFY_FILE_SETTLE_TIMEOUT` | `True` | 寫入等待超時發送通知 |
| `FILE_TIMEOUT_NOTIFY_LIMIT` | `20` | 每條超時通知最多列出 20 個文件 |
| `REFRESH_FILE_GRANDPARENT` | `False` | 是否額外刷新文件的爺爺目錄 |
| `CONNECTION_IDLE_TIMEOUT` | `300` | gRPC 連接空閒 5 分鐘後關閉 |
| `WATCH_START_WARNING_SECONDS` | `30` | 監聽初始化超過閾值後告警 |
| `FAILED_REFRESH_RETRY_SECONDS` | `600` | 最終失敗後每 10 分鐘複查 |
| `NOTIFY_FIRST_REFRESH_FAILURE` | `False` | 首次最終失敗默認只記錄，延遲複查仍失敗才告警 |

文件仍爲 0KB 或無法讀取時，穩定計數保持爲 0。超時後移除該次等待並輸出文件路徑；後續新事件可以重新登記等待。大小和修改時間穩定是本地採樣判斷，雲端上傳完成及遠端索引可見時間仍由雲盤和 CD2 決定。

刷新成功後結束該任務；默認不安排成功後的固定補刷。刷新失敗最多嘗試 3 次，仍失敗則保存目標，後續定時複查。目錄不存在等情況會按腳本的父目錄及刪除處理邏輯另行處理。

## 🧩 更新協議與生成模塊

協議及 API 用法參見 [CloudDrive2 官方 gRPC API 指南](https://www.clouddrive2.com/api/CloudDrive2_gRPC_API_Guide.html)。本項目附帶的 `clouddrive.proto` 協議版本字段爲 `1.1.2`。應使用與服務 API 匹配的協議；協議版本號不是 CD2 應用版本號。

需要更新時，將新協議保存爲 `clouddrive.proto`。使用這個文件名生成，才能得到主腳本導入的 `clouddrive_pb2` 模塊。

```bash
python3 -m pip install --upgrade -r requirements.txt
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. clouddrive.proto
```

請在項目目錄執行命令，一起重新生成兩個模塊。生成和運行使用同一個 Python 環境；導入報版本不匹配時，安裝依賴後重新生成模塊。

## 🔍 常見問題

- **腳本提示至少啓用一臺 CD2**：填寫地址、令牌並將該條目的 `enabled` 改爲 `True`。
- **協議模塊導入失敗**：檢查兩個生成文件是否與主腳本同目錄，依賴版本是否符合生成代碼要求。
- **認證或權限錯誤**：檢查每臺服務的 API 令牌、授權路徑及目錄列舉權限；參照官方指南配置。
- **沒有檢測到變動**：先確認映射目錄已完成監聽初始化。腳本依賴系統文件事件，具體掛載方式是否會產生事件需實際驗證。
- **目錄監聽初始化緩慢**：檢查掛載是否正常、子目錄數量及 Linux inotify 限額；各映射目錄獨立初始化。
- **圖片一直爲 0KB**：5 分鐘後輸出寫入超時並按配置通知，檢查上傳進度和掛載狀態。
- **沒有收到手機通知**：青龍中需要正確配置 `notify` 渠道；普通 Python 環境默認只打印通知內容。

## 📈 Star History

<a href="https://www.star-history.com/?repos=ffinly%2Fclouddrive2-auto-refresh&amp;type=date&amp;legend=top-left">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=ffinly/clouddrive2-auto-refresh&amp;type=date&amp;theme=dark&amp;legend=top-left" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=ffinly/clouddrive2-auto-refresh&amp;type=date&amp;legend=top-left" />
    <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=ffinly/clouddrive2-auto-refresh&amp;type=date&amp;legend=top-left" />
  </picture>
</a>

## 📄 License

本項目原創代碼採用 [GNU General Public License v3.0](LICENSE)（GPL-3.0-only）許可。

`clouddrive.proto` 爲 CloudDrive2 上游協議文件，保留其原有權利及許可。

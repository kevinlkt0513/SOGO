# PostgreSQL to MongoDB 數據遷移與同步工具

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

數據遷移 Python 工具，專為將 PostgreSQL 中的業務資料遷移並持續同步至 MongoDB 而設計。此工具不僅支援全量、增量、斷點續傳等多種模式，更內建了符合特定業務規則的「主表驅動」遷移邏輯。

---

## 核心功能

* **🎯 業務邏輯驅動**: 嚴格以 `gif_hcc_event` 為主表，只遷移存在於此表的 `event_no` 及其所有關聯資料，確保資料的業務準確性。
* **🔄 多種同步模式**:
    * **`--full-sync`**: 完整遷移所有符合規則的歷史資料。
    * **`--incremental`**: 定期同步新增或修改的資料。
    * **`--resume`**: 在任何中斷（如網路問題、手動停止）後，從上次的進度點無縫恢復。
* **🧩 智慧資料合併**: 自動將多個 PG 表（如 `gif_event`, `gif_hcc_event`）的資訊合併成結構化的 MongoDB 文件。
* **⚡ 效能優先**: 內建索引檢查與建立功能（需權限），並透過批次處理 (`BATCH_SIZE`) 兼顧效能與記憶體使用。
* **📄 詳細日誌記錄**: 所有操作均有詳細、易於追蹤的日誌檔案，採用標準化命名 (`YYYY-MM-DD_HHMMSS_任務名.log`)，方便監控與排錯。
* **⚙️ 高度可設定**: 所有資料庫連線資訊、批次大小等均透過 `.env` 檔案進行設定，無需修改程式碼。

## 架構與資料流程

本工具的遷移流程嚴格遵循以 `gif_hcc_event` 為「白名單」的原則：

```
[ PostgreSQL 來源 (Source) ]
  ├─ gif_hcc_event (主表/白名單)
  ├─ gif_event (資訊表)
  ├─ gif_hcc_event_attendee (子表)
  └─ ... (其他關聯表)
          |
          | (1. 腳本啟動時，先從主表載入 event_no 白名單)
          V
+------------------------------------------------------+
|             Python 遷移腳本                          |
|                                                      |
|  [ Event_no 白名單 ] <--- (所有查詢都以此為過濾條件)   |
|                                                      |
|  [ fetch_batch() ]  ->  [ upsert_batch() ]           |
|  (批次讀取 PG)          (批次寫入 Mongo)             |
+------------------------------------------------------+
          |
          | (2. 只處理白名單內的資料，並寫入 MongoDB)
          V
[ MongoDB 目標 (Target) ]
  ├─ events (合併後的活動主文件)
  └─ event_attendees (關聯的參加者)
```

## 環境準備

* Python 3.9+
* PostgreSQL Server
* MongoDB Server

## 安裝與設定

1.  **克隆 (Clone) 倉庫**
    ```bash
    git clone [您的倉庫 URL]
    cd [您的專案目錄]
    ```

2.  **建立並啟用虛擬環境 (建議)**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **安裝依賴套件**
    ```bash
    pip install -r requirements.txt
    ```

4.  **設定環境變數**
    複製範本檔案，並根據您的環境填寫實際的資料庫連線資訊。
    ```bash
    cp .env.example .env
    ```
    接著編輯 `.env` 檔案：
    ```ini
    # PostgreSQL Settings
    PG_HOST=your-pg-host.rds.amazonaws.com
    PG_PORT=5432
    PG_USER=mongo_reader
    PG_PASSWORD=your_secure_password
    PG_DBNAME=your_database

    # MongoDB Settings
    MONGO_URI=mongodb://user:pass@host/dbname

    # Migration Settings
    BATCH_SIZE=1000
    ```

## 使用說明

所有功能均透過 `migrate.py` 腳本搭配命令列參數執行。

| 指令 (`Command`)          | 說明 (`Description`)                                                                       | 範例 (`Example`)                                                         |
| ------------------------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------ |
| `--full-sync`             | **全量同步**：首次將所有符合規則的歷史資料從 PG 遷移到 Mongo。僅需執行一次。               | `python migrate.py --full-sync`                                          |
| `--incremental`           | **增量同步**：同步自上次任務結束以來，PG 中新增或修改過的資料。建議設定為排程任務。          | `python migrate.py --incremental`                                        |
| `--resume`                | **斷點恢復**：當任務意外中斷後，從上次記錄的斷點繼續執行。                                 | `python migrate.py --resume`                                             |
| `--correction`            | **手動校正**：針對特定表格和時間範圍，強制重跑一次資料同步。                                 | `python migrate.py --correction attendees 2025-08-15T10:00:00 2025-08-15T11:00:00` |
| `--show-status`           | **顯示狀態**：以 JSON 格式印出目前所有表格的同步進度與斷點資訊。                           | `python migrate.py --show-status`                                        |
| `--reset`                 | **重置進度**：【**危險**】清除 `checkpoint.json`，所有同步進度將歸零。                     | `python migrate.py --reset`                                              |


## 建議工作流程

1.  **首次部署**
    1.  完成上述的安裝與設定。
    2.  **【關鍵步驟】** 聯絡您的資料庫管理員 (DBA)，確保 PostgreSQL 和 MongoDB 都已建立必要的效能索引。
    3.  執行一次全量同步：`python migrate.py --full-sync`。
    4.  檢查日誌 (`logs/` 目錄下) 與 MongoDB 資料，確認首次遷移成功。

2.  **日常維運**
    1.  在伺服器上設定排程任務 (如 Crontab)，定期執行增量同步。例如，每 5 分鐘執行一次：
        ```crontab
        */5 * * * * cd /path/to/your/project && /path/to/your/project/venv/bin/python migrate.py --incremental >> /dev/null 2>&1
        ```
    2.  定期監控 `logs/` 目錄，檢查是否有 `ERROR` 級別的日誌。

## 授權 (License)

本專案採用 [MIT License](LICENSE) 授權。
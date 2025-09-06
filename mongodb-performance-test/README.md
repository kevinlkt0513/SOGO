```markdown
# 遠東 SOGO 百貨「點換金」專案 - MongoDB 效能壓力測試

## 1. 專案簡介

本專案旨在對遠東 SOGO 百貨「點換金」系統從 PostgreSQL 遷移至 MongoDB Atlas 後的效能進行全面的負載與壓力測試。

測試核心目標是驗證 MongoDB Atlas M10 叢集在不同數據規模與並發負載下的效能表現、擴展性及穩定性，並為未來的硬體規格選型提供量化的數據支援。

## 2. 專案結構

```

.
├── reports/                \# 存放所有 Locust 測試報告 (日誌與 CSV 檔案)
├── coupon\_test.py          \# 測試場景：高競爭更新 (搶券)
├── insert\_test.py          \# 測試場景：純粹批量寫入
├── mixed\_test.py           \# 測試場景：混合讀寫
├── query\_test.py           \# 測試場景：純粹讀取查詢
├── update\_test.py          \# 測試場景：純粹更新
├── .env.uat.example        \# 環境變數設定檔範本
└── requirements.txt        \# Python 依賴套件

````

## 3. 環境設定

### 3.1 前置需求

* Python 3.8+
* pip

### 3.2 安裝步驟

1.  **Clone 專案庫**
    ```bash
    git clone [您的專案庫 URL]
    cd sogo-mongodb-performance-test
    ```

2.  **建立並啟用虛擬環境** (建議)
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **安裝依賴套件**
    ```bash
    pip install -r requirements.txt
    ```

4.  **設定環境變數**
    複製範本檔案，並填入您的 MongoDB Atlas 連線資訊。
    ```bash
    cp .env.uat.example .env.uat
    ```
    接著編輯 `.env.uat` 檔案。**強烈建議使用標準連線格式 (Standard Connection String)**，以確保在 Locust 測試環境下的最佳相容性。

    ```ini
    # 請從 Atlas UI 的連線選項中，選擇 "Drivers" -> "Python"，並複製標準連線字串 (Standard Connection String)
    # 它看起來會像這樣，明確列出所有主機：
    MONGO_URI="mongodb://mongoConsultant:your_password@pl-0-ap-northeast-1.tafudp.mongodb.net:1024,pl-0-ap-northeast-1.tafudp.mongodb.net:1025,pl-0-ap-northeast-1.tafudp.mongodb.net:1026/?replicaSet=atlas-gd585r-shard-0&authSource=admin&retryWrites=true&w=majority"
    DB_NAME="your_database_name"
    ```

## 4. 如何執行測試

本專案使用 Locust 進行壓力測試。所有測試指令均應在專案根目錄下執行。

### 4.1 指令語法模板

```bash
locust -f [腳本檔案] --headless --users [並發使用者數] --spawn-rate [每秒啟動使用者數] -t [持續時間] --logfile [日誌路徑] --csv [CSV報告路徑]
````

  * `-f`: 指定要執行的測試腳本。
  * `--headless`: 在無 UI 的模式下執行。
  * `--users`: 總模擬使用者數量。
  * `--spawn-rate`: 每秒鐘啟動多少個使用者。
  * `-t`: 測試總持續時間 (例如 `30m`, `1h`)。
  * `--logfile`: 將詳細日誌輸出到指定檔案。
  * `--csv`: 將統計數據以 CSV 格式輸出。

### 4.2 測試範例

**範例 1：執行純粹查詢的負載測試**
(模擬 2000 個使用者，持續 30 分鐘)

```bash
locust -f query_test.py --headless --users 2000 --spawn-rate 100 -t 30m --logfile reports/query_load.log --csv reports/query_load
```

**範例 2：執行搶券的壓力測試**
(模擬 3000 個使用者，持續 30 分鐘)

```bash
locust -f coupon_test.py --headless --users 3000 --spawn-rate 100 -t 30m --logfile reports/coupon_stress.log --csv reports/coupon_stress
```

## 5\. 核心測試結論摘要

本次壓測的核心發現如下：

  * **中小型數據規模 (10萬-50萬筆)：** M10 叢集**效能卓越**，所有指標均遠超 KPI 標準，證明其足以應對當前及可預見的業務增長。

  * **大型數據規模 (100萬筆)：** M10 叢集遭遇**災難性效能瓶頸**。

      * **查詢效能崩潰：** 複雜查詢因資料庫 RAM 不足而癱瘓，回應時間從毫秒級惡化至分鐘級。
      * **寫入效能下降：** 高並發寫入因索引過大導致 I/O 壓力劇增，效能顯著衰退。

  * **根本原因：** M10 的 **8GB RAM** 是限制系統擴展性的**唯一瓶頸**。

  * **核心建議：** 為了支撐百萬級數據負載，**必須將叢集升級至 M30 (16GB RAM) 或更高規格**。

<!-- end list -->

```
```
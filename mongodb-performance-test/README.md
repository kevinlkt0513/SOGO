# 遠東 SOGO 百貨「點換金」專案 - MongoDB 效能壓力測試

## 1. 專案簡介

本專案旨在對遠東 SOGO 百貨「點換金」系統從 PostgreSQL 遷移至 MongoDB Atlas 後的效能進行全面的負載與壓力測試。

測試核心目標是驗證 **MongoDB Atlas M10 叢集** 在不同數據規模與並發負載下的效能表現、擴展性及穩定性，並為未來的硬體規格選型提供量化的數據支援。

---

## 2. 專案結構

```text
.
├── reports/            # 存放所有 Locust 測試報告 (日誌與 CSV 檔案)
├── coupon_test.py      # 測試場景：高競爭更新 (搶券)
├── insert_test.py      # 測試場景：純粹批量寫入
├── mixed_test.py       # 測試場景：混合讀寫
├── query_test.py       # 測試場景：純粹讀取查詢
├── update_test.py      # 測試場景：純粹更新
├── .env.uat.example    # 環境變數設定檔範本
└── requirements.txt    # Python 依賴套件
```

---

## 3. 環境設定

### 3.1 前置需求

- Python 3.8+
- pip

### 3.2 安裝步驟

1. **Clone 專案庫**

   ```bash
   git clone [您的專案庫 URL]
   cd sogo-mongodb-performance-test
   ```

2. **建立並啟用虛擬環境** (建議)

   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows 用 `venv\Scripts\activate`
   ```

3. **安裝依賴套件**

   ```bash
   pip install -r requirements.txt
   ```

4. **設定環境變數**

   複製範本檔案，並填入您的 MongoDB Atlas 連線資訊：

   ```bash
   cp .env.uat.example .env.uat
   ```

   接著編輯 `.env.uat` 檔案。  
   **建議使用「標準連線字串 (Standard Connection String)」**，以確保在 Locust 測試下的相容性。

   ```ini
   # 從 Atlas UI → Connect → Drivers → Python
   # 複製標準連線字串 (會列出所有主機)
   MONGO_URI="mongodb://mongoConsultant:your_password@pl-0-ap-northeast-1.tafudp.mongodb.net:1024,pl-0-ap-northeast-1.tafudp.mongodb.net:1025,pl-0-ap-northeast-1.tafudp.mongodb.net:1026/?replicaSet=atlas-gd585r-shard-0&authSource=admin&retryWrites=true&w=majority"
   DB_NAME="your_database_name"
   ```

---

## 4. 如何執行測試

本專案使用 **Locust** 進行壓力測試。  
所有測試指令均應在專案根目錄下執行。

### 4.1 指令語法模板

```bash
locust -f [腳本檔案] \
       --headless \
       --users [並發使用者數] \
       --spawn-rate [每秒啟動使用者數] \
       -t [持續時間] \
       --logfile [日誌路徑] \
       --csv [CSV報告路徑]
```

參數說明：

- `-f`: 指定要執行的測試腳本  
- `--headless`: 無 UI 模式  
- `--users`: 總模擬使用者數量  
- `--spawn-rate`: 每秒啟動多少個使用者  
- `-t`: 測試持續時間 (如 `30m`, `1h`)  
- `--logfile`: 輸出日誌檔案  
- `--csv`: 將統計數據輸出為 CSV  

### 4.2 測試範例

**範例 1：純查詢負載測試**  
模擬 2000 個使用者，持續 30 分鐘：

```bash
locust -f query_test.py --headless --users 2000 --spawn-rate 100 -t 30m --logfile reports/query_load.log --csv reports/query_load
```

**範例 2：搶券壓力測試**  
模擬 3000 個使用者，持續 30 分鐘：

```bash
locust -f coupon_test.py --headless --users 3000 --spawn-rate 100 -t 30m --logfile reports/coupon_stress.log --csv reports/coupon_stress
```

---

## 5. 核心測試結論摘要

- **中小型數據規模 (10萬–50萬筆)：**  
  M10 叢集效能優秀，所有指標均遠超 KPI，能應付當前及近期業務需求。

- **大型數據規模 (100萬筆)：**  
  M10 出現災難性瓶頸：  
  - **查詢效能崩潰：** RAM 不足 → 複雜查詢從毫秒級惡化至分鐘級。  
  - **寫入效能下降：** 索引過大 → I/O 壓力劇增，效能明顯衰退。  

- **根本原因：** M10 的 **8GB RAM** 是系統擴展的唯一瓶頸。  

- **建議：**  
  若需支撐百萬級數據，**必須升級至 M30 (16GB RAM) 或更高規格**。  

---

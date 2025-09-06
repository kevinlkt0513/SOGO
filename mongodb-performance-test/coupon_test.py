import os
import random
import sys
import time
from datetime import datetime
from locust import User, task, between
from pymongo import MongoClient
from dotenv import load_dotenv

# ------------------------------------------------------------------------------------
# 步驟 1: 全域設定與資源初始化
# ------------------------------------------------------------------------------------

# 從 .env.uat 檔案載入環境變數
load_dotenv('.env.uat')

# MongoDB 連線資訊
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

# --- 全域共用 MongoClient ---
# 針對 2000 並發超載測試的特別設定
# maxPoolSize 與使用者數量一致，以模擬所有使用者同時嘗試連線。
# 增加 connectTimeoutMS 和 serverSelectionTimeoutMS 以確保失敗資訊來源於伺服器飽和，而非客戶端過早超時。
try:
    print("--- 正在建立共用的 MongoClient (為 2000 並發搶券測試設定)... ---")
    SHARED_MONGO_CLIENT = MongoClient(
        MONGO_URI,
        maxPoolSize=2000, 
        minPoolSize=100,
        connectTimeoutMS=10000, # 連線超時10秒
        serverSelectionTimeoutMS=10000 # 伺服器選擇超時10秒
    )
    SHARED_MONGO_CLIENT.server_info()
    print("--- 共用 MongoClient 建立並連線成功 ---")
except Exception as e:
    print(f"建立共用 MongoClient 時發生致命錯誤: {e}")
    sys.exit(1)

# 基於共用的 client，建立共用的資料庫和集合物件，以提高效率
DB = SHARED_MONGO_CLIENT[DB_NAME]
EVENTS_COLLECTION = DB['events']

# 共用的測試資料
EVENT_NOS = []

def setup_shared_data():
    """
    此函數在壓力測試腳本啟動時只會被執行一次，用於載入所有使用者共用的活動編號。
    """
    print("--- 正在載入搶券活動編號共用資料... ---")
    try:
        global EVENT_NOS
        print("正在從 'events' 集合載入所有 eventNo...")
        EVENT_NOS = [doc['eventNo'] for doc in EVENTS_COLLECTION.find({}, {'eventNo': 1})]
        
        if not EVENT_NOS:
            print("錯誤：未載入任何活動編號！請檢查資料庫中是否有資料。")
            sys.exit(1)
        else:
            print(f"--- 成功載入 {len(EVENT_NOS)} 筆活動編號 ---")
            
    except Exception as e:
        print(f"載入共用資料時發生致命錯誤: {e}")
        sys.exit(1)

# 在 Locust 腳本被解析時，立即執行一次資料載入
setup_shared_data()


# ------------------------------------------------------------------------------------
# 步驟 2: User Class (使用全域共用資源)
# ------------------------------------------------------------------------------------

class MongoDBUser(User):
    # on_start 現在極其輕量，因為所有資源都是全域共用的
    def on_start(self):
        pass

    @task
    def redeem_coupon(self):
        # 從全域共用的清單中隨機選取一個 event_no
        event_no = random.choice(EVENT_NOS)
        
        start_time = time.time()
        request_name = "寫入:搶券"
        
        try:
            # 任務直接使用全域的集合物件
            # 使用原子操作 $inc 來安全地扣減數量，此操作必定在 Primary 節點執行
            result = EVENTS_COLLECTION.update_one(
                {'eventNo': event_no, 'available': {'$gt': 0}},
                {'$inc': {'available': -1}}
            )
            
            total_time = int((time.time() - start_time) * 1000)
            
            # modified_count 為 1 表示搶券成功，為 0 表示券已搶完
            success = result.modified_count == 1
            context = {"eventNo": event_no, "success": success}
            
            # 無論操作是否成功修改了文件（搶券成功或失敗），資料庫操作本身是成功的，因此回報為成功事件
            self.environment.events.request.fire(
                request_type="mongodb",
                name=request_name,
                response_time=total_time,
                response_length=result.modified_count, # 可以記錄修改的文件數 (1 或 0)
                exception=None,
                context=context
            )
            
        except Exception as e:
            # 只有當資料庫連線或指令執行發生異常時，才回報為失敗
            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(
                request_type="mongodb",
                name=request_name,
                response_time=total_time,
                response_length=0,
                exception=e,
                context={"eventNo": event_no}
            )

    # 設置一個較短的等待時間以模擬密集的搶券場景
    wait_time = between(0.1, 1)

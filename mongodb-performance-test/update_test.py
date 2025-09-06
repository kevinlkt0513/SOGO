import os
import random
import sys
import time
from datetime import datetime
from locust import User, task, between
from pymongo import MongoClient
from dotenv import load_dotenv

# ------------------------------------------------------------------------------------
# 全域設定與資源初始化
# ------------------------------------------------------------------------------------
load_dotenv('.env.uat')
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

# --- 全域共用 MongoClient ---
# 針對 2000 並發超載測試的特別設定
# maxPoolSize 與使用者數量一致，以模擬所有使用者同時嘗試連線。
# 增加 connectTimeoutMS 和 serverSelectionTimeoutMS 以確保失敗資訊來源於伺服器飽和，而非客戶端過早超時。
try:
    print("--- 正在建立共用的 MongoClient (為 2000 並發更新測試設定)... ---")
    SHARED_MONGO_CLIENT = MongoClient(
        MONGO_URI,
        maxPoolSize=2000,
        minPoolSize=100,
        connectTimeoutMS=10000,  # 連線超時10秒
        serverSelectionTimeoutMS=10000  # 伺服器選擇超時10秒
    )
    SHARED_MONGO_CLIENT.server_info()
    print("--- 共用 MongoClient 建立並連線成功 ---")
except Exception as e:
    print(f"建立共用 MongoClient 時發生致命錯誤: {e}")
    sys.exit(1)

# 共用的資料庫和集合物件
DB = SHARED_MONGO_CLIENT[DB_NAME]
EVENTS_COLLECTION = DB['events']

# 共用的測試資料 (活動編號)
EVENT_NOS = []

def setup_shared_data():
    """載入所有活動編號，用於隨機選取更新目標。"""
    print("--- 正在載入活動編號共用資料... ---")
    try:
        global EVENT_NOS
        EVENT_NOS = [doc['eventNo'] for doc in EVENTS_COLLECTION.find({}, {'eventNo': 1})]
        if not EVENT_NOS:
            print("錯誤：未載入任何活動編號！")
            sys.exit(1)
        else:
            print(f"--- 成功載入 {len(EVENT_NOS)} 筆活動編號 ---")
    except Exception as e:
        print(f"載入共用資料時發生致命錯誤: {e}")
        sys.exit(1)

setup_shared_data()

# ------------------------------------------------------------------------------------
# Locust User Class
# ------------------------------------------------------------------------------------
class MongoDBUser(User):
    # 純粹更新測試，等待時間可以短一些以增加壓力
    wait_time = between(0.1, 0.5)

    def on_start(self):
        pass

    @task
    def pure_update_event(self):
        # 隨機選取一個 event_no 作為更新目標
        target_event_no = random.choice(EVENT_NOS)

        start_time = time.time()
        request_name = "寫入:更新活動文件"
        
        try:
            # 此更新操作必定在 Primary 節點執行
            result = EVENTS_COLLECTION.update_one(
                {"eventNo": target_event_no},
                {
                    "$set": {"mod_date": datetime.utcnow()},
                    "$inc": {"version": 1} # 模擬版本號或計數器遞增
                }
            )
            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(
                request_type="mongodb",
                name=request_name,
                response_time=total_time,
                response_length=result.modified_count, # 記錄被修改的文件數 (通常為 1 或 0)
                exception=None,
                context={"eventNo": target_event_no}
            )
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(
                request_type="mongodb",
                name=request_name,
                response_time=total_time,
                response_length=0,
                exception=e,
                context={"eventNo": target_event_no}
            )
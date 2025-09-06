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
    print("--- 正在建立共用的 MongoClient (為 2000 並發混合測試設定)... ---")
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

# 基於共用的 client，建立共用的資料庫和集合物件
DB = SHARED_MONGO_CLIENT[DB_NAME]
EVENTS_COLLECTION = DB['events']
ATTENDEES_COLLECTION = DB['event_attendees']

# 共用的測試資料
# 警告：如果 events 集合非常大，在啟動時將其全部載入記憶體可能會消耗大量資源並減慢啟動速度。
ALL_EVENTS = []
ALL_EVENT_NOS = []
ALL_APP_IDS = []

def setup_shared_data():
    """
    在壓測啟動時載入所有使用者共用的測試資料。
    """
    print("--- 正在載入混合測試共用資料... ---")
    try:
        global ALL_EVENTS, ALL_EVENT_NOS, ALL_APP_IDS
        
        print("正在從 'events' 集合載入資料...")
        ALL_EVENTS = list(EVENTS_COLLECTION.find({}, {'eventNo': 1, 'hccEventType': 1}))
        ALL_EVENT_NOS = [e['eventNo'] for e in ALL_EVENTS]
        
        attendee_count = int(os.getenv("ATTENDEE_COUNT", 1000000))
        sample_size = min(attendee_count // 10, 100000)
        if sample_size == 0: sample_size = 1
        
        print(f"資料總量 {attendee_count}，將隨機取樣 {sample_size} 個 App ID...")
        pipeline = [{'$sample': {'size': sample_size}}, {'$project': {'_id': 0, 'appId': 1}}]
        results = ATTENDEES_COLLECTION.aggregate(pipeline)
        ALL_APP_IDS = [doc['appId'] for doc in results]
        
        if not ALL_EVENTS or not ALL_APP_IDS:
            print("錯誤：共用資料為空！請檢查資料庫和集合中是否有資料。")
            sys.exit(1)
        else:
            print(f"--- 成功載入 {len(ALL_EVENTS)} 筆活動和 {len(ALL_APP_IDS)} 個 App ID 樣本 ---")
            
    except Exception as e:
        print(f"載入共用資料時發生致命錯誤: {e}")
        sys.exit(1)

setup_shared_data()


# ------------------------------------------------------------------------------------
# 步驟 2: User Class (混合讀寫任務)
# ------------------------------------------------------------------------------------

class MongoDBUser(User):
    def on_start(self):
        self.member_types = ["SOGO_VIP", "SOGO_VVIP"]

    # 讀取任務 (權重 3)
    @task(3)
    def read_operations(self):
        # 從共用資料中隨機選取測試資料
        event_info = random.choice(ALL_EVENTS)
        app_id = random.choice(ALL_APP_IDS)
        now = datetime.now()
        
        # 隨機執行兩種不同的讀取查詢之一
        if random.random() < 0.5:
            # 查詢類型 1: 判斷使用者是否符合任一活動資格 (高效)
            request_name = "讀取:檢查活動資格"
            pipeline = [
                {'$match': {'eventStatus': "USING", 'onlyApp': True, 'startDate': {'$lte': now}, 'endDate': {'$gte': now}, '$or': [{'hccEventType': {'$ne': "CLUB_DEDUCT_POINT"}}, {'memberTypes': {'$in': self.member_types}}]}},
                {'$lookup': {'from': "event_attendees", 'let': {'event_no': "$eventNo"}, 'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ["$eventNo", "$$event_no"]}, {'$eq': ["$appId", app_id]}]}}}, {'$project': {'_id': 1}}], 'as': "participation"}},
                {'$match': {'$or': [{'allMember': True}, {'participation': {'$ne': []}}]}},
                {'$limit': 1},
                {'$project': {'_id': 1}}
            ]
        else:
            # 查詢類型 2: 查詢使用者符合資格的所有分館 (優化版)
            request_name = "讀取:查詢分館資訊"
            pipeline = [
                {'$match': {'eventStatus': "USING", 'onlyApp': True, 'startDate': {'$lte': now}, 'endDate': {'$gte': now}, '$or': [{'hccEventType': {'$ne': "CLUB_DEDUCT_POINT"}}, {'memberTypes': {'$in': self.member_types}}]}},
                {'$lookup': {'from': "event_attendees", 'let': {'event_no': "$eventNo"}, 'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ["$eventNo", "$$event_no"]}, {'$eq': ["$appId", app_id]}]}}}], 'as': "participation"}},
                {'$match': {'$or': [{'allMember': True}, {'participation': {'$ne': []}}]}},
                {'$unwind': "$usingBranchNames"},
                {'$group': {'_id': None, 'uniqueBranches': {'$addToSet': "$usingBranchNames"}}},
                {'$project': {'_id': 0, 'branches': '$uniqueBranches'}}
            ]

        start_time = time.time()
        try:
            result_list = list(EVENTS_COLLECTION.aggregate(pipeline))
            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(request_type="mongodb", name=request_name, response_time=total_time, response_length=len(result_list))
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(request_type="mongodb", name=request_name, response_time=total_time, response_length=0, exception=e)

    # 寫入任務 (權重 1)
    @task(1)
    def redeem_coupon(self):
        # 從共用列表中隨機選取一個 event_no
        event_no = random.choice(ALL_EVENT_NOS)
        
        start_time = time.time()
        request_name = "寫入:搶券"
        
        try:
            # 使用原子操作 $inc 來安全地扣減數量，此操作必定在 Primary 節點執行
            result = EVENTS_COLLECTION.update_one(
                {'eventNo': event_no, 'available': {'$gt': 0}},
                {'$inc': {'available': -1}}
            )
            
            total_time = int((time.time() - start_time) * 1000)
            success = result.modified_count == 1
            
            self.environment.events.request.fire(
                request_type="mongodb",
                name=request_name,
                response_time=total_time,
                response_length=result.modified_count,
                context={"success": success}
            )
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(
                request_type="mongodb",
                name=request_name,
                response_time=total_time,
                response_length=0,
                exception=e
            )

    wait_time = between(1, 5)

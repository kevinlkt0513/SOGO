import os
import random
import sys
import time
from datetime import datetime
from locust import User, task, between
from pymongo import MongoClient
from dotenv import load_dotenv

# ------------------------------------------------------------------------------------
# 步骤 1: 全域设定与资源初始化
# ------------------------------------------------------------------------------------

load_dotenv('.env.uat')
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

# --- 全域共享 MongoClient ---
# 针对 2000 并发超载测试的特别配置
# maxPoolSize 与用户数一致，以模拟所有用户同时尝试连接。
# 增加 connectTimeoutMS 和 serverSelectionTimeoutMS 以确保失败信息来源于服务器饱和，而非客户端过早超时。
try:
    print("--- 正在建立共享的 MongoClient (为 2000 并发超载测试配置)... ---")
    SHARED_MONGO_CLIENT = MongoClient(
        MONGO_URI,
        maxPoolSize=2000, 
        minPoolSize=100,
        connectTimeoutMS=10000, # 连接超时10秒
        serverSelectionTimeoutMS=10000 # 服务器选择超时10秒
    )
    SHARED_MONGO_CLIENT.server_info()
    print("--- 共享 MongoClient 建立并连线成功 ---")
except Exception as e:
    print(f"建立共享 MongoClient 时发生致命错误: {e}")
    sys.exit(1)

DB = SHARED_MONGO_CLIENT[DB_NAME]
EVENTS_COLLECTION = DB['events']
ATTENDEES_COLLECTION = DB['event_attendees']

ALL_EVENT_NOS = []
ALL_APP_IDS = []

def setup_shared_data():
    """
    在压测启动时载入所有用户共享的测试数据。
    """
    print("--- 正在载入共享测试数据... ---")
    try:
        global ALL_EVENT_NOS, ALL_APP_IDS
        
        print("正在从 'events' 集合载入 eventNo...")
        ALL_EVENT_NOS = [doc['eventNo'] for doc in EVENTS_COLLECTION.find({}, {'eventNo': 1})]
        
        attendee_count = int(os.getenv("ATTENDEE_COUNT", 1000000))
        sample_size = min(attendee_count // 10, 100000)
        if sample_size == 0: sample_size = 1
        
        print(f"数据总量 {attendee_count}，将随机取样 {sample_size} 个 App ID...")
        pipeline = [{'$sample': {'size': sample_size}}, {'$project': {'_id': 0, 'appId': 1}}]
        results = ATTENDEES_COLLECTION.aggregate(pipeline)
        ALL_APP_IDS = [doc['appId'] for doc in results]
        
        if not ALL_EVENT_NOS or not ALL_APP_IDS:
            print("错误：共享数据为空！")
            sys.exit(1)
        else:
            print(f"--- 成功载入 {len(ALL_EVENT_NOS)} 笔活动编号和 {len(ALL_APP_IDS)} 个 App ID 样本 ---")
    except Exception as e:
        print(f"载入共享数据时发生致命错误: {e}")
        sys.exit(1)

setup_shared_data()

# ------------------------------------------------------------------------------------
# 步骤 2: User Class (采用最高效的聚合查询逻辑)
# ------------------------------------------------------------------------------------

class MongoDBUser(User):
    def on_start(self):
        self.member_types = ["SOGO_VIP", "SOGO_VVIP"]

    # 🎯 功能 1 (聚合优化版): 判断使用者是否符合任一活动资格
    @task(3)
    def has_hcc_event_aggregated(self):
        app_id = random.choice(ALL_APP_IDS)
        now = datetime.now()
        
        start_time = time.time()
        request_name = "agg:has_hcc_event"
        is_qualified = False
        
        try:
            pipeline = [
                {'$match': {'eventStatus': "USING", 'onlyApp': True, 'startDate': {'$lte': now}, 'endDate': {'$gte': now}, '$or': [{'hccEventType': {'$ne': "CLUB_DEDUCT_POINT"}}, {'memberTypes': {'$in': self.member_types}}]}},
                {'$lookup': {'from': 'event_attendees', 'let': {'event_no': '$eventNo'}, 'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$eventNo', '$$event_no']}, {'$eq': ['$appId', app_id]}]}}}, {'$project': {'_id': 1}}], 'as': 'participation'}},
                {'$match': {'$or': [{'allMember': True}, {'participation': {'$ne': []}}]}},
                {'$limit': 1},
                {'$project': {'_id': 1}}
            ]
            result = list(EVENTS_COLLECTION.aggregate(pipeline))
            if result:
                is_qualified = True
            
            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(request_type="mongodb", name=request_name, response_time=total_time, response_length=1, exception=None)
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(request_type="mongodb", name=request_name, response_time=total_time, response_length=0, exception=e)

    # 🎯 功能 2 (聚合优化版): 查询使用者符合活动资格的分馆清单
    @task(3)
    def find_hcc_event_branch_info_aggregated(self):
        app_id = random.choice(ALL_APP_IDS)
        now = datetime.now()
        
        start_time = time.time()
        request_name = "agg:find_branch_info"
        
        try:
            pipeline = [
                {'$match': {'appId': app_id}},
                {'$lookup': {'from': 'events', 'localField': 'eventNo', 'foreignField': 'eventNo', 'as': 'eventDetails'}},
                {'$unwind': '$eventDetails'},
                {'$match': {'eventDetails.eventStatus': 'USING', 'eventDetails.onlyApp': True, 'eventDetails.startDate': {'$lte': now}, 'eventDetails.endDate': {'$gte': now}, '$or': [{'eventDetails.hccEventType': {'$ne': "CLUB_DEDUCT_POINT"}}, {'eventDetails.memberTypes': {'$in': self.member_types}}]}},
                {'$unwind': '$eventDetails.usingBranchNames'},
                {'$group': {'_id': '$eventDetails.usingBranchNames'}}
            ]
            specific_branches = {doc['_id'] for doc in ATTENDEES_COLLECTION.aggregate(pipeline)}
            
            all_member_filter = {'eventStatus': "USING", 'onlyApp': True, 'allMember': True, 'startDate': {'$lte': now}, 'endDate': {'$gte': now}, '$or': [{'hccEventType': {'$ne': "CLUB_DEDUCT_POINT"}}, {'memberTypes': {'$in': self.member_types}}]}
            all_member_branches_cursor = EVENTS_COLLECTION.find(all_member_filter, {'usingBranchNames': 1, '_id': 0})
            all_member_branches = set()
            for doc in all_member_branches_cursor:
                if doc.get('usingBranchNames'):
                    all_member_branches.update(doc['usingBranchNames'])
            
            final_branches = list(specific_branches.union(all_member_branches))

            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(request_type="mongodb", name=request_name, response_time=total_time, response_length=len(final_branches), exception=None)
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(request_type="mongodb", name=request_name, response_time=total_time, response_length=0, exception=e)

    # 🎯 功能 3 (聚合优化版): 查询使用者参加指定活动详情
    @task(1)
    def find_hcc_event_info_aggregated(self):
        event_no = random.choice(ALL_EVENT_NOS)
        app_id = random.choice(ALL_APP_IDS)
        now = datetime.now()
        
        start_time = time.time()
        request_name = "agg:find_event_info"
        event_doc = None

        try:
            pipeline = [
                {'$match': {'eventNo': event_no, 'eventStatus': "USING", 'onlyApp': True, 'startDate': {'$lte': now}, 'endDate': {'$gte': now}, '$or': [{'hccEventType': {'$ne': "CLUB_DEDUCT_POINT"}}, {'memberTypes': {'$in': self.member_types}}]}},
                {'$lookup': {'from': 'event_attendees', 'let': {'event_no': '$eventNo'}, 'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$eventNo', '$$event_no']}, {'$eq': ['$appId', app_id]}]}}}, {'$project': {'_id': 1}}], 'as': 'participation'}},
                {'$match': {'$or': [{'allMember': True}, {'participation': {'$ne': []}}]}},
                {'$project': {'participation': 0}}
            ]
            
            result = list(EVENTS_COLLECTION.aggregate(pipeline))
            if result:
                event_doc = result[0]

            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(request_type="mongodb", name=request_name, response_time=total_time, response_length=1 if event_doc else 0, exception=None)
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            self.environment.events.request.fire(request_type="mongodb", name=request_name, response_time=total_time, response_length=0, exception=e)

    wait_time = between(1, 5)
import os
import random
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING
from bson.decimal128 import Decimal128
from dotenv import load_dotenv

# 從 .env.uat 文件加載環境變數
load_dotenv('.env.uat')

# MongoDB 連線資訊
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

# 數據量基準 (可根據壓測需求調整)
EVENT_COUNT = int(os.getenv("EVENT_COUNT", 1000))
MEMBER_COUNT = int(os.getenv("MEMBER_COUNT", 1000000))
ATTENDEE_COUNT = int(os.getenv("ATTENDEE_COUNT", 5000000))

# 分館對照表 (來自客戶提供的參考資料)
BRANCH_INFO = {
    'BRANCH_TAIWAN': '全台', 'BRANCH_TAIPEI': '台北店', 'BRANCH_JS': '忠孝館',
    'BRANCH_FS': '復興館', 'BRANCH_DH': '敦化館', 'BRANCH_TM': '天母店',
    'BRANCH_JL': '中壢店', 'BRANCH_BC': '新竹店', 'BRANCH_GS': '高雄店',
    'BRANCH_GCA': 'Garden City-A區', 'BRANCH_GCB': 'Garden City-B區',
    'BRANCH_GCC': 'Garden City-C區', 'BRANCH_GCD': 'Garden City-D區',
}
BRANCH_IDS = list(BRANCH_INFO.keys())
MEMBER_TYPES = ["SOGO_VIP", "SOGO_VVIP", "NORMAL", "BRONZE"]

def create_indexes(db):
    """
    在生成數據前創建所有必要的索引
    """
    print("開始創建索引...")
    events_collection = db['events']
    attendees_collection = db['event_attendees']

    try:
        print("正在删除旧的 event_query_index...")
        events_collection.drop_index("event_query_index")
    except Exception as e:
        print(f"删除索引 event_query_index 失败 (可能是因为它不存在): {e}")

     # 1. 核心查询优化索引：将等值匹配字段放在前面
    print("正在创建核心查询索引: event_query_index...")
    events_collection.create_index([
        ("eventStatus", ASCENDING),
        ("onlyApp", ASCENDING),
        ("startDate", ASCENDING),
        ("endDate", ASCENDING),
        ("allMember", ASCENDING), # 将 allMember 也加入索引，提高特定查询效率
        ("hccEventType", ASCENDING)
    ], name="event_query_index")

    # 2. memberTypes 索引
    events_collection.create_index([
        ("memberTypes", ASCENDING)
    ], name="member_types_index")

    # 3.eventNo 索引 (用於快速查找)
    events_collection.create_index([
        ("eventNo", ASCENDING)
    ], name="eventNo_index")
    
    # 4.event_attendees 參與者集合索引 (複合唯一索引)
    attendees_collection.create_index(
        [("eventNo", ASCENDING), ("appId", ASCENDING)],
        unique=True,
        name="event_attendees_unique_index"
    )

    # 5. attendees 集合需要一个基于 appId 的索引以加速聚合查询
    print("正在为 event_attendees 创建 appId 索引...")
    attendees_collection.create_index([
        ("appId", ASCENDING)
    ], name="attendees_appId_index")

    print("索引創建完成。")

def create_events_data(db):
    """
    生成 events 集合的測試數據
    """
    events_collection = db['events']
    events = []
    
    for i in range(EVENT_COUNT):
        event_no = f"EVN{random.randint(10000, 99999):08}"
        all_member = (i % 2 == 0) # 50% allMember = true
        start_date = datetime.now() - timedelta(days=random.randint(10, 30))
        end_date = start_date + timedelta(days=random.randint(30, 90))
        
        using_branch_ids = random.sample(BRANCH_IDS, random.randint(1, 3))
        using_branch_names = [BRANCH_INFO[bid] for bid in using_branch_ids]

        doc = {
            "eventNo": event_no,
            "name": f"測試活動-{i}",
            "hccEventType": "NORMAL_DEDUCT_POINT" if random.random() < 0.8 else "CLUB_DEDUCT_POINT",
            "eventStatus": "USING",
            "onlyApp": True,
            "startDate": start_date,
            "endDate": end_date,
            "allMember": all_member,
            "eventBranchId": random.choice(BRANCH_IDS),
            "usingBranchIds": [{"id": Decimal128(str(j)), "branch": bid} for j, bid in enumerate(using_branch_ids)],
            "usingBranchNames": using_branch_names,
            "prizeCouponJson": [{"uuid": f"UUID{j}", "exchangeName": f"優惠券{j}", "amt": 1000} for j in range(random.randint(1, 5))],
            "available": 1000, # for coupon_test
            "memberTypes": random.sample(MEMBER_TYPES, random.randint(1, 2)) if not all_member and "CLUB" in event_no else [],
            # 參考數據中其他欄位
            "add_cost_center": "100116330",
            "add_date": datetime.now(),
            "add_id": "006350",
            "add_name": "施力瑋",
            "app_exchange": "Y",
            "auto_distribution": "N",
            "carrier_type": "NONE",
            "co_sponsor": None,
            "disabled": "N",
            "disabled_date": None,
            "exchange_times": "ONE_OF_DAY" if random.random() < 0.5 else None,
            "freebie_exchange": None,
            "invoice_calcu_period": "DAY",
            "invoice_check_carrier": "N",
            "lock_counter": "N",
            "lock_customer": "N",
            "lock_user": None,
            "max_amt": Decimal128('0'),
            "min_amt": Decimal128('0'),
            "mod_date": datetime.now(),
            "mod_id": "004502",
            "mod_name": "游欣逸",
            "multiple_exchange": "Y",
            "number_of_invoices": "MULTIPLE",
            "number_of_stages": Decimal128('0'),
            "pay_types": "_01,_02",
            "version": Decimal128(str(random.randint(1, 100))),
            "web_url": "https://www.sogo.com.tw/event"
        }
        events.append(doc)
    
    if events:
        events_collection.insert_many(events)
    print(f"成功創建 {len(events)} 筆 events 集合數據。")
    return events

def create_attendees_data(db, events):
    """
    修正後的函數：生成 event_attendees 集合的測試數據，確保唯一性
    """
    attendees_collection = db['event_attendees']
    attendees = []
    
    qualified_events = [e for e in events if not e["allMember"]]
    
    if not qualified_events:
        print("沒有 allMember = false 的活動，跳過 attendees 數據生成。")
        return
        
    # 使用 set 來存儲已生成的 (eventNo, appId) 組合，確保唯一性
    unique_attendees = set()
    generated_count = 0

    while generated_count < ATTENDEE_COUNT:
        event = random.choice(qualified_events)
        app_id = f"APP{random.randint(100000, 1000000):07}"
        
        key = (event["eventNo"], app_id)
        
        # 檢查組合是否已存在
        if key not in unique_attendees:
            unique_attendees.add(key)
            
            doc = {
                "eventNo": event["eventNo"],
                "appId": app_id,
                # 參考數據中其他欄位
                "add_cost_center": "100116330",
                "add_date": datetime.now(),
                "add_id": "007396",
                "add_name": "徐貫鈞",
                "disabled": "N",
                "disabled_date": None,
                "id": Decimal128(str(generated_count)),
                "mod_date": datetime.now(),
                "mod_id": "007396",
                "mod_name": "徐貫鈞",
                "version": Decimal128('1')
            }
            attendees.append(doc)
            generated_count += 1

            if len(attendees) >= 10000: # 每 10000 筆批量插入一次
                try:
                    attendees_collection.insert_many(attendees, ordered=False)
                    attendees.clear()
                    print(f"已成功插入 {generated_count} 筆 attendees 數據。")
                except Exception as e:
                    print(f"批量插入失敗: {e}")
                    attendees.clear()
    
    # 插入最後一批剩餘的數據
    if attendees:
        try:
            attendees_collection.insert_many(attendees, ordered=False)
            print(f"成功插入最後 {len(attendees)} 筆 attendees 數據。")
        except Exception as e:
            print(f"批量插入失敗: {e}")


def clean_all_data(db):
    """
    清空所有測試數據
    """
    print("開始清空測試數據...")
    db['events'].delete_many({})
    db['event_attendees'].delete_many({})
    print("events 和 event_attendees 集合已清空。")

def main():
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        # 清空舊數據
        clean_all_data(db)
        
        # 創建索引
        create_indexes(db)
        
        # 創建新數據
        events = create_events_data(db)
        create_attendees_data(db, events)
        
    except Exception as e:
        print(f"發生錯誤：{e}")
    finally:
        if 'client' in locals() and client:
            client.close()

if __name__ == "__main__":
    main()

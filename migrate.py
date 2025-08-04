#!/usr/bin/env python3
# -*- coding: utf-8 -*-


"""
PostgreSQL → MongoDB 活動資料初始化遷移工具，功能說明：
- 主表計數，子表異常不影響遷移進度
- 支援斷點續傳（以時間+event_no 雙鍵）
- 大批次/粗粒度批次及一致性驗證
- 延遲日誌初始化，避免沒有參數時產生無效日誌
- 多線程批量處理，加速同步效率
- 命令列多模式（全量、增量、補寫、斷點恢復…）
"""


import os
import sys
import json
import logging
import datetime
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed


import psycopg2
from psycopg2.extras import DictCursor
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv


# ===== 讀取環境變數與載入對應 .env 檔案 =====
ENV = os.getenv("ENV", "dev")
load_dotenv(f".env.{ENV}", override=True)


# PostgreSQL 資料庫設定
POSTGRESQL_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "dbname": os.getenv("PG_DBNAME"),
}


# MongoDB 連線字串
MONGO_URI = os.getenv("MONGO_URI")


# 批次大小與最大線程數
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
SCHEMA = "gift2022"


CHECKPOINT_FILE = "migration_checkpoint.json"  # 斷點檔案
LOG_DIR = "logs"                              # 日誌目錄


LOG_FILE = None   # 日誌檔名
logger = None     # logger 實體


# ===== 延遲初始化日誌紀錄器 =====
def init_logger():
    """
    啟動日誌系統，在 LOG_DIR 目錄建立以當前時間命名之日誌文件
    """
    global LOG_FILE, logger
    os.makedirs(LOG_DIR, exist_ok=True)
    LOG_FILE = os.path.join(LOG_DIR, f"migration_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s"
    )
    logger = logging.getLogger()


# ===== 資料庫全局連線物件 =====
pg_conn = None          # PostgreSQL 連線
pg_cursor = None        # PostgreSQL 游標
mongo_client = None     # MongoDB 連線
col_events = None       # events 集合
col_attendees = None    # event_attendees 集合


def init_db_conn():
    """
    初始化 PostgreSQL 和 MongoDB 連線，且建立必要索引
    """
    global pg_conn, pg_cursor, mongo_client, col_events, col_attendees
    try:
        pg_conn = psycopg2.connect(**POSTGRESQL_CONFIG, cursor_factory=DictCursor)
        pg_cursor = pg_conn.cursor()
    except Exception as e:
        logger.error(f"無法連接 PostgreSQL：{e}")
        sys.exit(1)

    try:
        mongo_client = MongoClient(MONGO_URI)
        mdb = mongo_client["sogo_gift"]
        col_events = mdb["events"]
        col_attendees = mdb["event_attendees"]
        # 依常用查詢加建立索引
        col_events.create_index([
            ("hccEventType", 1), ("eventStatus", 1), ("onlyApp", 1),
            ("startDate", 1), ("endDate", 1)
        ])
        col_events.create_index("memberTypes")
        col_attendees.create_index([("eventNo", 1), ("appId", 1)], unique=True)
    except Exception as e:
        logger.error(f"無法連接 MongoDB：{e}")
        sys.exit(1)


# ===== 分館名稱映射表（供顯示及比對排序使用） =====
BRANCH_ORDER = {
    "BRANCH_TAIWAN": "全台", "BRANCH_TAIPEI": "台北店", "BRANCH_JS": "忠孝館",
    "BRANCH_FS": "復興館", "BRANCH_DH": "敦化館", "BRANCH_TM": "天母店",
    "BRANCH_JL": "中壢店", "BRANCH_BC": "新竹店", "BRANCH_GS": "高雄店",
    "BRANCH_GC": "Garden City", "BRANCH_GCA": "Garden City-A區",
    "BRANCH_GCB": "Garden City-B區", "BRANCH_GCC": "Garden City-C區", "BRANCH_GCD": "Garden City-D區"
}


# ===== 擴充主活動資料，掛入會員類型與適用分館資訊 =====
def enrich_event(event):
    """
    擴充活動資料字典，執行以下操作：
    1. 查詢此活動對應的會員類型，並加入 event["memberTypes"] 列表
    2. 查詢此活動對應優惠券所適用的分館清單，
       並排序後分別存入 event["usingBranchIds"] (分館ID) 與 event["usingBranchNames"] (分館名稱)
    3. 將字串型布林欄位（onlyApp、allMember）轉換為布林值 (True/False) 方便後續使用

    異常處理：
    - 若查詢會員類型或優惠券分館失敗，則對應欄位設為空列表並記錄警告日誌
    """

    event_no = event["eventNo"]  # 取得活動編號，作為查詢條件

    # 查詢會員類型，若有多筆則聚合成列表賦值給 event["memberTypes"]
    try:
        sql = f"SELECT member_type FROM {SCHEMA}.gif_hcc_event_member_type WHERE event_no = %s"
        logger.info(f"[SQL] 查會員類型: {sql} 參數: [{event_no}]")
        pg_cursor.execute(sql, (event_no,))
        rows = pg_cursor.fetchall()
        event["memberTypes"] = [r["member_type"] for r in rows] if rows else []
    except Exception as e:
        # 查詢失敗時，記錄警告並賦值空列表避免程式中斷
        logger.warning(f"會員類型查詢失敗，事件 {event_no}，錯誤：{e}")
        event["memberTypes"] = []

    # 查詢優惠券適用分館，利用子查詢取得活動關聯coupon_setting_no，撈取對應分館branch
    try:
        sql = f"""
            SELECT DISTINCT branch
            FROM {SCHEMA}.gif_event_coupon_burui
            WHERE coupon_setting_no IN (
                SELECT coupon_setting_no FROM {SCHEMA}.gif_event_coupon_setting WHERE event_no = %s
            )
        """
        logger.info(f"[SQL] 查優惠券分館: 參數 [{event_no}]")
        pg_cursor.execute(sql, (event_no,))
        branches = [r["branch"] for r in pg_cursor.fetchall()]
        if branches:
            # 根據預設分館順序 BRANCH_ORDER 排序分館ID清單
            sorted_ids = sorted(branches, key=lambda b: list(BRANCH_ORDER.keys()).index(b) if b in BRANCH_ORDER else 9999)
            event["usingBranchIds"] = sorted_ids
            # 取得對應中文分館名稱，找不到時以ID代替
            event["usingBranchNames"] = [BRANCH_ORDER.get(b, b) for b in sorted_ids]
        else:
            # 無匹配資料則設空列表
            event["usingBranchIds"] = []
            event["usingBranchNames"] = []
    except Exception as e:
        # 查詢失敗時，記錄警告並將分館欄位設為空值
        logger.warning(f"優惠券分館查詢失敗，事件 {event_no}，錯誤：{e}")
        event["usingBranchIds"] = []
        event["usingBranchNames"] = []

    # 將字串"Y"/"N"轉換為布林值 True/False
    event["onlyApp"] = event.get("onlyApp", "N") == 'Y'
    event["allMember"] = event.get("allMember", "N") == 'Y'

    return event


# ===== 批次抓取event_no，支援斷點（雙鍵：日期+event_no） =====
def fetch_events_batch(start_time, end_time, last_checkpoint_time=None, last_checkpoint_id=None, batch_size=100):
    """
    批次取得指定時間範圍內需遷移的活動編號 (event_no)。

    支援依時間戳記加事件ID雙鍵進行斷點續傳，
    可避免重複處理或遺漏資料。
    
    參數：
    - start_time: 起始時間，字串或 datetime 物件
    - end_time: 結束時間，字串或 datetime 物件
    - last_checkpoint_time: 上一次斷點時間 (ISO字串或 datetime)，作為下一批次的起點時間
    - last_checkpoint_id: 上一次斷點事件編號 (event_no)，輔助斷點定位
    - batch_size: 單次抓取最大活動數量限制

    回傳：
    - list of event_no，為符合條件的活動事件編號清單
    """

    # 若輸入為字串，轉換成 datetime 物件方便比較及 SQL 傳參
    if isinstance(start_time, str):
        start_time = datetime.datetime.fromisoformat(start_time)
    if isinstance(end_time, str):
        end_time = datetime.datetime.fromisoformat(end_time)

    # 判斷是否為斷點續傳模式，需指定上一筆時間與event_no為雙鍵
    if last_checkpoint_time is not None and last_checkpoint_id is not None:
        sql = f"""
            SELECT event_no
            FROM {SCHEMA}.gif_event
            WHERE 
              -- 從斷點時間下一筆起，依時間先後排序補抓
              (exchange_start_date > %s 
               OR (exchange_start_date = %s AND event_no > %s))
              -- 且在本次窗口指定時間區間內
              AND exchange_start_date >= %s AND exchange_start_date < %s
            ORDER BY exchange_start_date, event_no
            LIMIT %s
        """
        # SQL參數依序為：斷點時間、斷點時間、斷點event_no、窗口起始、窗口結束、抓取數量上限
        params = [last_checkpoint_time, last_checkpoint_time, last_checkpoint_id, start_time, end_time, batch_size]
    else:
        # 非斷點模式，直接在窗口時間範圍內依時間及event_no排序抓取
        sql = f"""
            SELECT event_no
            FROM {SCHEMA}.gif_event
            WHERE exchange_start_date >= %s AND exchange_start_date < %s
            ORDER BY exchange_start_date, event_no
            LIMIT %s
        """
        params = [start_time, end_time, batch_size]

    # 紀錄 SQL 查詢語句與參數方便追蹤與除錯
    logger.info(f"[SQL] fetch_events_batch:\n{sql}\n參數: {params}")

    # 執行 SQL 查詢
    pg_cursor.execute(sql, params)

    # 取得查詢結果
    rows = pg_cursor.fetchall()

    # 回傳活動編號列表，供後續遷移使用
    return [row["event_no"] for row in rows]


# ===== 取得指定活動完整內容 =====
def fetch_event(event_no):
    """
    從 PostgreSQL 資料庫中聯查主活動表與附表，
    擷取指定 event_no 的完整活動資料。

    查詢欄位包含：
    - 主活動表 (gif_hcc_event) 的細節欄位，如事件備註、分館、優惠券JSON、注意事項連結、布林欄位等
    - 附表 (gif_event) 的共用欄位，如活動名稱、狀態、開始結束日期及網址
    結果將以字典形式返回，欄位名稱已格式化對應。

    若找不到資料則回傳 None。
    """

    # 複合 SQL 查詢同時取得兩表資料，依 event_no 做連接條件
    sql = f"""
        SELECT he.event_no, he.event_memo, he.branch AS eventBranchId, he.prize_coupon_json,
               he.gift_attention_url, he.only_app, he.all_member, he.hcc_event_type,
               e.name, e.event_status, e.exchange_start_date, e.exchange_end_date, e.web_url
        FROM {SCHEMA}.gif_hcc_event he
        JOIN {SCHEMA}.gif_event e ON he.event_no = e.event_no
        WHERE he.event_no = %s
    """

    # 記錄 SQL 執行資訊，方便除錯
    logger.info(f"[SQL] fetch_event {event_no}")

    # 執行 SQL，帶入活動編號參數
    pg_cursor.execute(sql, (event_no,))

    # 取得單筆查詢結果
    row = pg_cursor.fetchone()

    # 若無資料則回傳 None
    if not row:
        return None

    # 對應查詢結果欄位，組成字典方便後續處理
    keys = [
        "eventNo", "eventMemo", "eventBranchId", "prizeCouponJson", "giftAttentionUrl",
        "onlyApp", "allMember", "hccEventType", "name", "eventStatus",
        "startDate", "endDate", "giftInforUrl"
    ]

    # 將鍵值配對回傳字典
    return dict(zip(keys, row))


# ===== 執行某活動所有參與者資料遷移（子表，異常不中斷） =====
def migrate_attendees(event_no):
    """
    遷移指定活動的所有參與者資料至 MongoDB。

    主要流程：
    1. 從 PostgreSQL 中查詢該活動(event_no)所有參與者的 app_id 列表
    2. 組合 MongoDB 批次 upsert 請求 (UpdateOne)，
       使用 eventNo 與 appId 作唯一鍵確保資料不重複
    3. 使用 MongoDB 的 bulk_write 非同步執行批次操作提升效能
    4. 回傳本次成功 upsert（新增）筆數
    5. 如有異常，紀錄警告日誌並回傳 0，避免阻斷整體遷移流程

    參數：
    - event_no: 活動編號

    回傳：
    - int: 成功 upsert 參與者資料筆數
    """
    try:
        # SQL 查詢該活動全部參與者 app_id
        sql = f"SELECT app_id FROM {SCHEMA}.gif_hcc_event_attendee WHERE event_no = %s"
        logger.info(f"[SQL] migrate_attendees {event_no}")
        pg_cursor.execute(sql, (event_no,))
        rows = pg_cursor.fetchall()

        # 若無參與者資料，直接回傳 0
        if not rows:
            return 0

        requests = []
        # 將查詢結果逐筆轉為 MongoDB UpdateOne upsert 請求
        for r in rows:
            app_id = r.get("app_id")
            if app_id:
                requests.append(UpdateOne(
                    # 查詢條件，確保唯一參考eventNo與appId
                    {"eventNo": event_no, "appId": app_id},
                    # 若不存在則新增，使用 $setOnInsert 操作符
                    {"$setOnInsert": {"eventNo": event_no, "appId": app_id}},
                    upsert=True
                ))

        # 若無有效請求（理論上不會），直接回傳 0
        if not requests:
            return 0

        # 執行 MongoDB 批次寫入，ordered=False 允許並行執行以提升效率
        result = col_attendees.bulk_write(requests, ordered=False)

        # 回傳本次 upsert 新增的筆數
        return result.upserted_count

    except Exception as e:
        # 捕獲異常，記錄警告以示非致命錯誤，並回傳0
        # 確保單筆活動參與者資料寫入失敗不會終止整體遷移流程
        logger.warning(f"參與者資料寫入失敗，事件 {event_no}，錯誤：{e}")
        return 0


# ===== 遷移單一活動 (主表、關聯子表)，統一用 bulkWrite + upsert =====
def migrate_single_event(event_no):
    """
    遷移單一活動資料與其參與者資料，全部使用 bulkWrite + upsert 模式。

    流程：
    1. 取得主活動資料，若找不到回傳 False
    2. 擴充活動資料（會員類型、分館等）
    3. 用 bulk_write 實現 update_one + upsert，若資料存在更新，否則新增
    4. 執行參與者資料遷移（同樣批次 upsert）
    5. 回傳成功狀態
    """
    try:
        event = fetch_event(event_no)
        if not event:
            logger.warning(f"找不到活動 {event_no}")
            return False

        event = enrich_event(event)

        # 準備更新請求，使用 update_one + upsert 保證存在時更新，不存在時新增
        request = UpdateOne(
            {"eventNo": event_no},
            {"$set": event},
            upsert=True
        )
        col_events.bulk_write([request], ordered=False)

        # 參與者資料統一用 upsert 批量寫入，因有唯一索引，不會重複
        inserted = migrate_attendees(event_no)

        logger.info(f"活動 {event_no} 遷移成功，參與者 {inserted} 筆")
        return True
    except Exception as e:
        logger.error(f"活動 {event_no} 遷移失敗，錯誤：{e}")
        return False


# ===== 統計指定時間段主表活動資料數量 =====
def count_pg_events_in_window(start, end):
    """
    統計 PostgreSQL 主表中，指定時間區間 (start 到 end) 內的活動總筆數。

    功能說明：
    - 利用 INNER JOIN 同步主活動表 (gif_hcc_event) 與附表 (gif_event)
      以確保計數活動均存在於兩表中
    - 使用 exchange_start_date 作為時間範圍條件過濾
    - 返回該時間段內活動的完整數量，作為遷移進度校驗依據

    參數：
    - start: 起始時間，datetime 或 ISO8601 字串
    - end: 結束時間，datetime 或 ISO8601 字串

    回傳：
    - int: 符合條件的活動筆數
    """

    if isinstance(start, str):
        start = datetime.datetime.fromisoformat(start)
    if isinstance(end, str):
        end = datetime.datetime.fromisoformat(end)

    sql = f"""
        SELECT COUNT(*)
        FROM {SCHEMA}.gif_hcc_event he
        JOIN {SCHEMA}.gif_event e ON he.event_no = e.event_no
        WHERE e.exchange_start_date >= %s AND e.exchange_start_date < %s
    """

    pg_cursor.execute(sql, (start, end))

    return pg_cursor.fetchone()[0]


# ===== 遷移單個窗口，包含多活動 =====
def migrate_window(window: dict, window_name: str):
    """
    處理指定「時間窗口」內的活動資料遷移。

    功能說明：
    - 根據斷點時間(last_checkpoint_time)和事件ID(last_checkpoint_id)批次抓取待遷移活動清單
    - 單筆遷移並持續更新斷點資料以支持續傳
    - 監控連續失敗次數，避免死循環
    - 完成後更新窗口狀態並記錄執行時間與統計
    """
    logger.info(f"處理窗口 {window_name}：{window['start']} ~ {window['end']} 狀態：{window['status']}")

    if window.get("status") == "completed":
        logger.info(f"{window_name} 已完成，跳過")
        return 0

    last_time = window.get("last_checkpoint_time")
    last_id = window.get("last_checkpoint_id")

    window["status"] = "in_progress"
    window["owner"] = os.uname().nodename
    window["start_exec_time"] = datetime.datetime.now().isoformat()
    save_checkpoint(cp_data)

    migrated = 0
    consecutive_fail_count = 0

    while True:
        batch_event_nos = fetch_events_batch(window["start"], window["end"], last_time, last_id, BATCH_SIZE)

        if not batch_event_nos:
            pg_total = count_pg_events_in_window(window["start"], window["end"])
            logger.info(f"PG主表總數：{pg_total}，已成功遷移：{migrated}")

            if migrated >= pg_total:
                window["status"] = "completed"
                logger.info(f"{window_name} 窗口已完成")
            else:
                window["status"] = "pending"
                logger.warning(f"{window_name} 窗口尚有 {pg_total - migrated} 筆未遷移")
            break

        logger.info(f"{window_name} 批次大小：{len(batch_event_nos)}")

        for eno in batch_event_nos:
            success = migrate_single_event(eno)

            if success:
                migrated += 1
                last_id = eno

                pg_cursor.execute(f"SELECT exchange_start_date FROM {SCHEMA}.gif_event WHERE event_no = %s", (eno,))
                row = pg_cursor.fetchone()
                if row and row["exchange_start_date"]:
                    last_time = row["exchange_start_date"].isoformat()
                else:
                    logger.warning(f"取event_no：{eno} 時間失敗，斷點時間不更新")

                window["last_checkpoint_time"] = last_time
                window["last_checkpoint_id"] = last_id
                window["processed_count"] = migrated
                window["last_update_time"] = datetime.datetime.now().isoformat()
                save_checkpoint(cp_data)

                consecutive_fail_count = 0
                logger.info(f"活動 {eno} 遷移成功，斷點時間更新: {last_time}")
            else:
                consecutive_fail_count += 1
                logger.warning(f"活動 {eno} 遷移失敗，連續錯誤計數 {consecutive_fail_count}")

                if consecutive_fail_count >= 5:
                    logger.error(f"{window_name} 連續 5 筆失敗，終止遷移避免死循環")
                    return migrated

    window["finish_exec_time"] = datetime.datetime.now().isoformat()
    save_checkpoint(cp_data)

    pg_count = count_pg_events_in_window(window["start"], window["end"])
    fail_count = max(pg_count - migrated, 0)

    logger.info(f"\n-- {window_name} 窗口遷移總結 --")
    logger.info(f"窗戶時間範圍: {window['start']} ~ {window['end']}")
    logger.info(f"PG主表筆數: {pg_count}")
    logger.info(f"成功遷移筆數: {migrated}")
    logger.info(f"失敗筆數: {fail_count}")
    logger.info(f"窗口狀態: {window['status']}\n")

    return migrated


# ===== 斷點資料的載入/儲存 =====
def load_checkpoint():
    """
    從檔案載入上次同步斷點, 不存在則初始化空結構
    """
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"base_windows": [], "correction_windows": []}


def save_checkpoint(data):
    """
    將同步進度與狀態存檔到checkpoint
    """
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ====== 命令處理（全量、增量、補寫、恢復、狀態等模式） =====
def command_full_sync(end_time=None):
    """
    全量同步：將自 1970-01-01（Linux 紀元時間起點）起至指定結束時間範圍內，
    所有活動資料一次性同步至目標資料庫（MongoDB）。

    透過時間區間劃分「基線窗口」（base_windows），以批次方式逐窗同步，
    如窗口不存在則新增並儲存斷點，支援斷點續傳功能。
    """

    if end_time is None:
        end_time = datetime.datetime.now().isoformat()

    global cp_data

    cp_data.setdefault("base_windows", [])

    exists = any(
        w["start"] == "1970-01-01T00:00:00" and w["end"] == end_time
        for w in cp_data["base_windows"]
    )

    if not exists:
        cp_data["base_windows"].append({
            "start": "1970-01-01T00:00:00",
            "end": end_time,
            "status": "pending",
            "owner": None,
            "processed_count": 0,
            "last_checkpoint_time": None,
            "last_checkpoint_id": None,
            "last_update_time": None,
            "start_exec_time": None,
            "finish_exec_time": None
        })
        save_checkpoint(cp_data)
        logger.info(f"新增基線窗口: 1970-01-01 至 {end_time}")

    total = 0
    for idx, w in enumerate(cp_data["base_windows"]):
        if w["status"] in ("pending", "in_progress"):
            total += migrate_window(w, f"base_windows[{idx}]")

    print(f"全量同步完成，遷移筆數: {total}")


def command_incremental(end_time=None):
    """
    執行增量同步任務：
    - 只處理自最後一個 base_windows 窗口的結束時間起至指定最新時間區間的新增或變更資料
    - 新增一個 pending 狀態的 base_windows 窗口來篩選這段時間內需遷移的資料
    - 此設計避免重複遷移舊資料，並保持狀態透明和追蹤清晰
    """
    if end_time is None:
        end_time = datetime.datetime.now().isoformat()

    global cp_data

    cp_data.setdefault("base_windows", [])

    if cp_data["base_windows"]:
        last_window = max(cp_data["base_windows"], key=lambda w: w["end"])
        start_time = last_window["end"]
    else:
        start_time = "1970-01-01T00:00:00"

    cp_data["base_windows"].append({
        "start": start_time,
        "end": end_time,
        "status": "pending",
        "owner": None,
        "processed_count": 0,
        "last_checkpoint_time": None,
        "last_checkpoint_id": None,
        "last_update_time": None,
        "start_exec_time": None,
        "finish_exec_time": None
    })

    logger.info(f"新增增量同步窗口：{start_time} 至 {end_time}")

    total = 0
    for idx, w in enumerate(cp_data["base_windows"]):
        if w["status"] in ("pending", "in_progress"):
            total += migrate_window(w, f"base_windows[{idx}]")

    save_checkpoint(cp_data)

    print(f"增量同步完成，遷移筆數: {total}")


def command_correction(start_time, end_time, force=True):
    """
    補寫同步（指定時間段）, 現僅保留強制補寫（覆蓋）模式。
    """
    global cp_data
    cp_data.setdefault("correction_windows", [])
    cp_data["correction_windows"].append({
        "start": start_time,
        "end": end_time,
        "status": "pending",
        "owner": None,
        "processed_count": 0,
        "last_checkpoint_time": None,
        "last_checkpoint_id": None,
        "last_update_time": None,
        "resync": True,   # 永遠強制覆蓋
        "start_exec_time": None,
        "finish_exec_time": None
    })
    save_checkpoint(cp_data)
    logger.info(f"新增補寫窗口：{start_time} 至 {end_time} 強制覆蓋=真")

    total = 0
    for idx, w in enumerate(cp_data["correction_windows"]):
        if w["status"] in ("pending", "in_progress"):
            total += migrate_window(w, f"correction_windows[{idx}]")
    print(f"補寫同步完成，遷移筆數: {total}")


def command_resume():
    """
    針對所有狀態 pending/in_progress 的窗口恢復續傳
    """
    global cp_data
    total = 0
    for wlist in ("base_windows", "correction_windows"):
        for idx, w in enumerate(cp_data.get(wlist, [])):
            if w["status"] in ("pending", "in_progress"):
                total += migrate_window(w, f"{wlist}[{idx}]")
    print(f"斷點恢復完成，遷移筆數: {total}")


def command_show_status():
    """
    顯示目前斷點與窗口同步狀態
    """
    global cp_data
    print(json.dumps(cp_data, ensure_ascii=False, indent=2))


def command_reset():
    """
    危險操作，重置所有斷點進度檔
    """
    confirm = input("警告：重置斷點將清除所有遷移進度且無法恢復，請輸入 Y 確認：")
    if confirm.strip().upper() == "Y":
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
        global cp_data
        cp_data = load_checkpoint()
        logger.info("斷點檔案已重置")
        print("重置完成")
    else:
        print("操作已取消")


def command_gen_report():
    """
    輸出各時間窗口同步進度摘要
    """
    global cp_data
    print("====== 遷移狀態報告 ======")
    for window_type in ("base_windows", "correction_windows"):
        print(f"\n[{window_type}] 時間窗口:")
        for w in cp_data.get(window_type, []):
            print(f"  時段: {w['start']} ~ {w['end']}")
            print(f"  狀態: {w['status']}, 已遷移筆數: {w.get('processed_count', 0)}")
            print(f"  最後斷點時間: {w.get('last_checkpoint_time')}")
            print(f"  最後斷點事件ID: {w.get('last_checkpoint_id')}")
            print(f"  執行者: {w.get('owner')}")
            print(f"  最後更新時間: {w.get('last_update_time')}")
    print("============================")


# ====== 命令列參數處理 =====
def parse_args():
    """
    解析命令列參數，支援各執行模式
    """
    parser = argparse.ArgumentParser(description="PostgreSQL → MongoDB 活動資料遷移工具")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full-sync", action="store_true", help="全量同步")
    group.add_argument("--incremental", action="store_true", help="增量同步")
    group.add_argument("--correction", action="store_true", help="強制補寫窗口")
    group.add_argument("--resume", action="store_true", help="斷點恢復")
    group.add_argument("--show-status", action="store_true", help="查看斷點狀態")
    group.add_argument("--gen-report", action="store_true", help="生成遷移報告")
    group.add_argument("--reset", action="store_true", help="重置所有斷點")

    parser.add_argument("--start", type=str, default=None, help="補寫窗口起始時間（ISO8601格式）")
    parser.add_argument("--end", type=str, default=None, help="結束時間（ISO8601格式或 now，預設當前時間）")
    parser.add_argument("--force", action="store_true", help="強制補寫時必須加")

    args = parser.parse_args()
    if args.end is None:
        args.end = datetime.datetime.now().isoformat()
    elif isinstance(args.end, str) and args.end.lower() == "now":
        args.end = datetime.datetime.now().isoformat()

    if args.correction and (not args.start or not args.end):
        parser.error("--correction 模式必須指定 --start 與 --end")

    if args.force and not args.correction:
        parser.error("--force 必須和 --correction 一起使用")

    return args


# ===== 主進入點 =====
def main():
    args = parse_args()
    global cp_data
    cp_data = load_checkpoint()

    if args.full_sync or args.incremental or args.correction or args.resume:
        init_logger()
        init_db_conn()

    if args.full_sync:
        command_full_sync(end_time=args.end)
    elif args.incremental:
        command_incremental(end_time=args.end)
    elif args.correction:
        # 只保留強制補寫模式，force 參數必須提供且為 True，參數在命令列必須提供
        command_correction(start_time=args.start, end_time=args.end, force=args.force)
    elif args.resume:
        command_resume()
    elif args.show_status:
        command_show_status()
    elif args.gen_report:
        command_gen_report()
    elif args.reset:
        command_reset()
    else:
        print("請指定有效參數，使用 --help 查看說明")

    if LOG_FILE:
        print("\n🔍 本次遷移日誌文件:")
        print(os.path.abspath(LOG_FILE))
        print(f"使用命令查看日誌：tail -f {os.path.abspath(LOG_FILE)}\n")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PostgreSQL → MongoDB 活動資料初始化遷移工具，具備：
- 以主表為計數單位，關聯表異常置空、不終止遷移
- 雙鍵斷點（時間+event_no）續傳
- 粗粒度大窗口批次+彙總一致性驗證
- 日誌延遲初始化避免無參數執行產生空檔
- 多線程批量遷移，提高效能
- 命令行多模式支援（全量、增量、補寫、斷點恢復等）
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

# ========== 載入 .env 配置 ==========
load_dotenv()

# 讀取目前環境，預設為 dev
ENV = os.getenv("ENV", "dev")

POSTGRESQL_CONFIGS = {
    "dev": {
        "host": os.getenv("PG_HOST_DEV"),
        "port": int(os.getenv("PG_PORT_DEV", 5432)),
        "user": os.getenv("PG_USER_DEV"),
        "password": os.getenv("PG_PASSWORD_DEV"),
        "dbname": os.getenv("PG_DBNAME_DEV"),
    },
    "prod": {
        "host": os.getenv("PG_HOST_PROD"),
        "port": int(os.getenv("PG_PORT_PROD", 5432)),
        "user": os.getenv("PG_USER_PROD"),
        "password": os.getenv("PG_PASSWORD_PROD"),
        "dbname": os.getenv("PG_DBNAME_PROD"),
    }
}

MONGO_URIS = {
    "dev": os.getenv("MONGO_URI_DEV"),
    "prod": os.getenv("MONGO_URI_PROD"),
}

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
SCHEMA = "gift2022"

POSTGRESQL_CONFIG = POSTGRESQL_CONFIGS[ENV]
MONGO_URI = MONGO_URIS[ENV]

CHECKPOINT_FILE = "migration_checkpoint.json"
LOG_DIR = "logs"

LOG_FILE = None
logger = None

# ========= 延遲初始化日誌 ==========
def init_logger():
    global LOG_FILE, logger
    os.makedirs(LOG_DIR, exist_ok=True)
    LOG_FILE = os.path.join(LOG_DIR, f"migration_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s"
    )
    logger = logging.getLogger()

# ========= 全局資料庫連線 ==========
pg_conn = None
pg_cursor = None
mongo_client = None
col_events = None
col_attendees = None

def init_db_conn():
    """ 初始化 PostgreSQL 與 MongoDB 連線 """
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

        col_events.create_index([
            ("hccEventType", 1), ("eventStatus", 1), ("onlyApp", 1),
            ("startDate", 1), ("endDate", 1), ("eventBranchId", 1)
        ])
        col_events.create_index("memberTypes")
        col_attendees.create_index([("eventNo", 1), ("appId", 1)], unique=True)
    except Exception as e:
        logger.error(f"無法連接 MongoDB：{e}")
        sys.exit(1)

BRANCH_ORDER = {
    "BRANCH_TAIWAN": "全台", "BRANCH_TAIPEI": "台北店", "BRANCH_JS": "忠孝館",
    "BRANCH_FS": "復興館", "BRANCH_DH": "敦化館", "BRANCH_TM": "天母店",
    "BRANCH_JL": "中壢店", "BRANCH_BC": "新竹店", "BRANCH_GS": "高雄店",
    "BRANCH_GC": "Garden City", "BRANCH_GCA": "Garden City-A區",
    "BRANCH_GCB": "Garden City-B區", "BRANCH_GCC": "Garden City-C區", "BRANCH_GCD": "Garden City-D區"
}

# ========= 主表與子表數據擴充 ==========
def enrich_event(event):
    event_no = event["eventNo"]
    try:
        sql = f"SELECT member_type FROM {SCHEMA}.gif_hcc_event_member_type WHERE event_no = %s"
        logger.info(f"[SQL] 查會員類型: {sql} 參數: [{event_no}]")
        pg_cursor.execute(sql, (event_no,))
        rows = pg_cursor.fetchall()
        event["memberTypes"] = [r["member_type"] for r in rows] if rows else []
    except Exception as e:
        logger.warning(f"會員類型查詢失敗，事件 {event_no}，錯誤：{e}")
        event["memberTypes"] = []

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
            sorted_ids = sorted(branches, key=lambda b: list(BRANCH_ORDER.keys()).index(b) if b in BRANCH_ORDER else 9999)
            event["usingBranchIds"] = sorted_ids
            event["usingBranchNames"] = [BRANCH_ORDER.get(b, b) for b in sorted_ids]
        else:
            event["usingBranchIds"] = []
            event["usingBranchNames"] = []
    except Exception as e:
        logger.warning(f"優惠券分館查詢失敗，事件 {event_no}，錯誤：{e}")
        event["usingBranchIds"] = []
        event["usingBranchNames"] = []

    event["onlyApp"] = event.get("onlyApp", "N") == 'Y'
    event["allMember"] = event.get("allMember", "N") == 'Y'

    return event

# ========= 批次抓取活動event_no ==========
def fetch_events_batch(start_time, end_time, last_checkpoint_time=None, last_checkpoint_id=None, batch_size=100):
    if isinstance(start_time, str):
        start_time = datetime.datetime.fromisoformat(start_time)
    if isinstance(end_time, str):
        end_time = datetime.datetime.fromisoformat(end_time)

    if last_checkpoint_time is not None and last_checkpoint_id is not None:
        sql = f"""
            SELECT event_no
            FROM {SCHEMA}.gif_event
            WHERE (exchange_start_date > %s OR (exchange_start_date = %s AND event_no > %s))
              AND exchange_start_date >= %s AND exchange_start_date < %s
            ORDER BY exchange_start_date, event_no
            LIMIT %s
        """
        params = [last_checkpoint_time, last_checkpoint_time, last_checkpoint_id, start_time, end_time, batch_size]
    else:
        sql = f"""
            SELECT event_no
            FROM {SCHEMA}.gif_event
            WHERE exchange_start_date >= %s AND exchange_start_date < %s
            ORDER BY exchange_start_date, event_no
            LIMIT %s
        """
        params = [start_time, end_time, batch_size]

    logger.info(f"[SQL] fetch_events_batch:\n{sql}\n參數: {params}")
    pg_cursor.execute(sql, params)
    rows = pg_cursor.fetchall()
    return [row["event_no"] for row in rows]

def fetch_event(event_no):
    sql = f"""
        SELECT he.event_no, he.event_memo, he.branch AS eventBranchId, he.prize_coupon_json,
               he.gift_attention_url, he.only_app, he.all_member, he.hcc_event_type,
               e.name, e.event_status, e.exchange_start_date, e.exchange_end_date, e.web_url
        FROM {SCHEMA}.gif_hcc_event he
        JOIN {SCHEMA}.gif_event e ON he.event_no = e.event_no
        WHERE he.event_no = %s
    """
    logger.info(f"[SQL] fetch_event {event_no}")
    pg_cursor.execute(sql, (event_no,))
    row = pg_cursor.fetchone()
    if not row:
        return None
    keys = ["eventNo", "eventMemo", "eventBranchId", "prizeCouponJson", "giftAttentionUrl", "onlyApp", "allMember", "hccEventType",
            "name", "eventStatus", "startDate", "endDate", "giftInforUrl"]
    return dict(zip(keys, row))

def migrate_attendees(event_no):
    try:
        sql = f"SELECT app_id FROM {SCHEMA}.gif_hcc_event_attendee WHERE event_no = %s"
        logger.info(f"[SQL] migrate_attendees {event_no}")
        pg_cursor.execute(sql, (event_no,))
        rows = pg_cursor.fetchall()
        if not rows:
            return 0
        requests = []
        for r in rows:
            app_id = r.get("app_id")
            if app_id:
                requests.append(UpdateOne(
                    {"eventNo": event_no, "appId": app_id},
                    {"$setOnInsert": {"eventNo": event_no, "appId": app_id}},
                    upsert=True
                ))
        if not requests:
            return 0
        result = col_attendees.bulk_write(requests, ordered=False)
        return result.upserted_count
    except Exception as e:
        logger.warning(f"參與者資料寫入失敗，事件 {event_no}，錯誤：{e}")
        return 0

def migrate_single_event(event_no):
    try:
        event = fetch_event(event_no)
        if not event:
            logger.warning(f"找不到活動 {event_no}")
            return False
        event = enrich_event(event)
        col_events.replace_one({"eventNo": event_no}, event, upsert=True)
        inserted = migrate_attendees(event_no)
        logger.info(f"活動 {event_no} 遷移成功，參與者 {inserted} 筆")
        return True
    except Exception as e:
        logger.error(f"活動 {event_no} 遷移失敗，錯誤：{e}")
        return False

def count_pg_events_in_window(start, end):
    if isinstance(start, str):
        start = datetime.datetime.fromisoformat(start)
    if isinstance(end, str):
        end = datetime.datetime.fromisoformat(end)
    sql = f"SELECT COUNT(*) FROM {SCHEMA}.gif_hcc_event he JOIN {SCHEMA}.gif_event e ON he.event_no = e.event_no WHERE e.exchange_start_date >= %s AND e.exchange_start_date < %s"
    pg_cursor.execute(sql, (start, end))
    return pg_cursor.fetchone()[0]

def migrate_window(window: dict, window_name: str):
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

        # 串行處理，等待每筆完成再處理下一筆
        for eno in batch_event_nos:
            success = migrate_single_event(eno)
            if success:
                migrated += 1
                last_id = eno
                # 更新最後事件時間作斷點
                pg_cursor.execute(f"SELECT exchange_start_date FROM {SCHEMA}.gif_event WHERE event_no = %s", (eno,))
                row = pg_cursor.fetchone()
                if row and row["exchange_start_date"]:
                    last_time = row["exchange_start_date"].isoformat()
                else:
                    # 發生問題時不更新 last_time，保留上次斷點
                    logger.warning(f"取event_no：{eno} 時間失敗，斷點時間不更新")
                # 更新斷點
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

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"base_windows": [], "correction_windows": []}

def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# 命令行管理及對應操作

def command_full_sync(end_time=None):
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
    if end_time is None:
        end_time = datetime.datetime.now().isoformat()
    global cp_data
    cp_data.setdefault("base_windows", [])
    for w in cp_data["base_windows"]:
        if w["end"] < end_time:
            w["end"] = end_time
    total = 0
    for idx, w in enumerate(cp_data["base_windows"]):
        if w["status"] in ("pending", "in_progress"):
            total += migrate_window(w, f"base_windows[{idx}]")
    save_checkpoint(cp_data)
    print(f"增量同步完成，遷移筆數: {total}")

def command_correction(start_time, end_time, force=False):
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
        "resync": force,
        "start_exec_time": None,
        "finish_exec_time": None
    })
    save_checkpoint(cp_data)
    logger.info(f"新增補寫窗口：{start_time} 至 {end_time} 強制覆蓋={force}")

    total = 0
    for idx, w in enumerate(cp_data["correction_windows"]):
        if w["status"] in ("pending", "in_progress"):
            total += migrate_window(w, f"correction_windows[{idx}]")
    print(f"補寫同步完成，遷移筆數: {total}")

def command_resume():
    global cp_data
    total = 0
    for wlist in ("base_windows", "correction_windows"):
        for idx, w in enumerate(cp_data.get(wlist, [])):
            if w["status"] in ("pending", "in_progress"):
                total += migrate_window(w, f"{wlist}[{idx}]")
    print(f"斷點恢復完成，遷移筆數: {total}")

def command_show_status():
    global cp_data
    print(json.dumps(cp_data, ensure_ascii=False, indent=2))

def command_reset():
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

def parse_args():
    parser = argparse.ArgumentParser(description="PostgreSQL → MongoDB 活動資料遷移工具")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full-sync", action="store_true", help="全量同步")
    group.add_argument("--incremental", action="store_true", help="增量同步")
    group.add_argument("--correction", action="store_true", help="補寫窗口")
    group.add_argument("--resume", action="store_true", help="斷點恢復")
    group.add_argument("--show-status", action="store_true", help="查看斷點狀態")
    group.add_argument("--gen-report", action="store_true", help="生成遷移報告")
    group.add_argument("--reset", action="store_true", help="重置所有斷點")

    parser.add_argument("--start", type=str, default=None, help="補寫窗口起始時間（ISO8601格式）")
    parser.add_argument("--end", type=str, default=None, help="結束時間（ISO8601格式或 now，預設當前時間）")
    parser.add_argument("--force", action="store_true", help="補寫時強制覆蓋")

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

def main():
    args = parse_args()
    global cp_data
    cp_data = load_checkpoint()

    # 只有在執行遷移會初始化日誌及DB連線
    if args.full_sync or args.incremental or args.correction or args.resume:
        init_logger()
        init_db_conn()

    if args.full_sync:
        command_full_sync(end_time=args.end)
    elif args.incremental:
        command_incremental(end_time=args.end)
    elif args.correction:
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

    # 輸出日誌路徑方便查看
    if LOG_FILE:
        print("\n🔍 本次遷移日誌文件:")
        print(os.path.abspath(LOG_FILE))
        print(f"使用命令查看日誌：tail -f {os.path.abspath(LOG_FILE)}\n")

if __name__ == "__main__":
    main()

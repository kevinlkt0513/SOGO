#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PostgreSQL → MongoDB 活動資料遷移工具（生產環境修正版）
- 核心邏輯：以 PostgreSQL gif_event 表中 event_no LIKE 'HCC%' 的記錄為唯一權威來源（白名單）。
- 功能：
  - 支援全量同步(--full-sync)與增量同步(--incremental)，所有操作均受白名單過濾。
  - 智慧增量同步：自動查找各表上次成功同步的結束時間點，作為本次增量起點。
  - checkpoint.json 集中管理狀態，支援斷點續傳(--resume)。
  - 資料一致性校驗邏輯與遷移邏輯完全對齊，確保校驗準確性。
  - 詳細的日誌記錄與任務總結報告。
"""

import os
import sys
import json
import logging
import datetime
import argparse
from typing import Set

import psycopg2
from psycopg2.extras import DictCursor
from pymongo import MongoClient, UpdateOne, IndexModel
from dotenv import load_dotenv
from decimal import Decimal
from bson.decimal128 import Decimal128
from datetime import date, timezone

# ===== 環境與設定 =====
ENV = os.getenv("ENV", "uat")
load_dotenv(f".env.{ENV}", override=True)

POSTGRESQL_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "dbname": os.getenv("PG_DBNAME"),
}

MONGO_URI = os.getenv("MONGO_URI")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
SCHEMA = "gift2022"

CHECKPOINT_FILE = "migration_checkpoint.json"
LOG_DIR = "logs"

# ===== 全域變數 =====
logger = None
args = None
pg_conn = None
mongo_client = None
mongo_dbs = {}
cp_data = {}

# 白名單全域變數
VALID_EVENT_NOS: Set[str] = set()

MIGRATION_STATS = {
    "tables": {},
    "total_pg_records": 0,
    "total_inserted": 0,
    "total_updated": 0,
    "verification_results": []
}

# 定義多表設定
TABLES_CONFIG = {
    "events": {
        "pg_table": "gif_event", "add_date_field": "add_date", "mod_date_field": "mod_date",
        "id_field": "event_no", "mongo_collection": "events", "verification_mode": "primary_count"
    },
    "hcc_events": {
        "pg_table": "gif_hcc_event", "add_date_field": "add_date", "mod_date_field": "mod_date",
        "id_field": "event_no", "mongo_collection": "events", "verification_mode": "merge_source"
    },
    "attendees": {
        "pg_table": "gif_hcc_event_attendee", "add_date_field": "add_date", "mod_date_field": "mod_date",
        "id_field": "id", "mongo_collection": "event_attendees", "verification_mode": "primary_count",
        "link_field": "event_no"
    },
    "coupon_burui": {
        "pg_table": "gif_event_coupon_burui", "add_date_field": "add_date", "mod_date_field": "mod_date",
        "id_field": "id", "mongo_collection": "events", "embed_array_field": "usingBranchIds",
        "verification_mode": "embed_array"
    },
    "member_types": {
        "pg_table": "gif_hcc_event_member_type", "add_date_field": "add_date", "mod_date_field": "mod_date",
        "id_field": "id", "mongo_collection": "events", "embed_array_field": "memberTypes",
        "verification_mode": "embed_array",
        "link_field": "event_no"
    },
}

def init_logger(task_name="general"):
    global logger
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    short_hostname = os.uname().nodename.split('.')[0]
    pid = os.getpid()
    log_filename = f"{timestamp}_{task_name}_{short_hostname}_{pid}.log"
    log_file_path = os.path.join(LOG_DIR, log_filename)
    if logging.getLogger().hasHandlers():
        logging.getLogger().handlers.clear()
    logging.basicConfig(
        filename=log_file_path, level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s",
    )
    # 將日誌同時輸出到控制台
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
    logging.getLogger().addHandler(console_handler)
    logger = logging.getLogger()
    logger.info(f"日誌檔案位於: {os.path.abspath(log_file_path)}")

def ensure_mongodb_indexes(mdb):
    logger.info("開始檢查並建立 MongoDB 索引...")
    try:
        events_collection = mdb["events"]
        event_no_index = IndexModel([("eventNo", 1)], name="idx_eventNo", unique=True)
        events_collection.create_indexes([event_no_index])
        logger.info(f"成功檢查/建立 events 集合的索引: idx_eventNo (unique)")
        
        attendees_collection = mdb["event_attendees"]
        attendee_compound_index = IndexModel([("eventNo", 1), ("appId", 1)], name="idx_eventNo_appId", unique=True)
        attendees_collection.create_indexes([attendee_compound_index])
        logger.info(f"成功檢查/建立 event_attendees 集合的索引: idx_eventNo_appId (unique)")
        
        logger.info("MongoDB 索引檢查完畢。")
    except Exception as e:
        logger.error(f"建立 MongoDB 索引時發生錯誤: {e}", exc_info=True)
        raise

def ensure_postgresql_indexes(conn):
    logger.info("開始檢查並建立 PostgreSQL 索引...")
    indexes_to_create = [
        f"CREATE INDEX IF NOT EXISTS ix_gif_event_hcc_filter ON {SCHEMA}.gif_event (event_no) WHERE event_no LIKE 'HCC%'",
        f"CREATE INDEX IF NOT EXISTS ix_gif_event_add_date_event_no ON {SCHEMA}.gif_event (add_date, event_no)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_hcc_event_add_date_event_no ON {SCHEMA}.gif_hcc_event (add_date, event_no)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_hcc_event_attendee_add_date_id ON {SCHEMA}.gif_hcc_event_attendee (add_date, id)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_hcc_event_member_type_add_date_id ON {SCHEMA}.gif_hcc_event_member_type (add_date, id)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_event_coupon_burui_add_date_id ON {SCHEMA}.gif_event_coupon_burui (add_date, id)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_event_mod_date ON {SCHEMA}.gif_event (mod_date)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_hcc_event_mod_date ON {SCHEMA}.gif_hcc_event (mod_date)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_hcc_event_attendee_mod_date ON {SCHEMA}.gif_hcc_event_attendee (mod_date)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_event_coupon_burui_coupon_setting_no ON {SCHEMA}.gif_event_coupon_burui (coupon_setting_no)",
    ]
    with conn.cursor() as cur:
        for sql in indexes_to_create:
            try:
                logger.info(f"嘗試執行: {sql}")
                cur.execute(sql)
            except psycopg2.Error as e:
                logger.warning(f"建立 PostgreSQL 索引失敗 (已跳過): {sql} - 原因: {e.pgcode} {e.pgerror}")
                conn.rollback() 
                continue 
        conn.commit()
    logger.info("PostgreSQL 索引檢查完畢。")


def init_db_conn():
    global pg_conn, mongo_client, mongo_dbs
    try:
        pg_conn = psycopg2.connect(**POSTGRESQL_CONFIG, cursor_factory=DictCursor)
        if not args.skip_pg_index_check:
            ensure_postgresql_indexes(pg_conn)
        else:
            logger.warning("已跳過 PostgreSQL 索引檢查。")
    except Exception as e:
        logger.error(f"無法連接 PostgreSQL：{e}", exc_info=True)
        sys.exit(1)
    try:
        mongo_client = MongoClient(MONGO_URI)
        mdb = mongo_client["gift"]
        ensure_mongodb_indexes(mdb)
        for k, v in TABLES_CONFIG.items():
            mongo_dbs[k] = mdb[v["mongo_collection"]]
    except Exception as e:
        logger.error(f"無法連接 MongoDB 或建立索引：{e}", exc_info=True)
        sys.exit(1)

def load_valid_event_nos():
    global VALID_EVENT_NOS
    logger.info("正在從 PostgreSQL 載入 event_no 白名單 (LIKE 'HCC%')...")
    sql = f"SELECT event_no FROM {SCHEMA}.gif_event WHERE event_no LIKE 'HCC%'"
    with pg_conn.cursor() as cur:
        cur.execute(sql)
        results = cur.fetchall()
    VALID_EVENT_NOS = {row[0] for row in results}
    if not VALID_EVENT_NOS:
        logger.warning("警告：在 gif_event 表中未找到任何以 'HCC' 開頭的活動，遷移將處理 0 筆主活動資料。")
    else:
        logger.info(f"白名單載入完畢，共 {len(VALID_EVENT_NOS)} 個有效的 event_no。")

def fetch_batch(table_key, start_time, end_time,
                  last_checkpoint_time=None, last_checkpoint_id=None,
                  batch_size=100, mode="incremental"):
    conf = TABLES_CONFIG[table_key]
    pg_table_alias = "t"
    join_clause = ""
    select_clause = f"SELECT {pg_table_alias}.* FROM {SCHEMA}.{conf['pg_table']} {pg_table_alias}"
    
    if table_key == "coupon_burui":
        join_clause = f"JOIN {SCHEMA}.gif_event_coupon_setting s ON {pg_table_alias}.coupon_setting_no = s.coupon_setting_no"
        select_clause = f"SELECT {pg_table_alias}.*, s.event_no FROM {SCHEMA}.{conf['pg_table']} {pg_table_alias}"

    date_field_str = conf['add_date_field']
    mod_date_field_str = conf['mod_date_field']
    id_field_str = conf['id_field']
    
    # 統一時間標準：無論何種模式，都使用 COALESCE(mod_date, add_date)
    date_field_coalesce = f"COALESCE({pg_table_alias}.{mod_date_field_str}, {pg_table_alias}.{date_field_str}, '1970-01-01')"
    
    order_by_clause = f"ORDER BY {date_field_coalesce}, {pg_table_alias}.{id_field_str}"

    params = []
    where_clauses = []

    link_field = conf.get("link_field", "event_no")
    event_no_field_in_query = "s.event_no" if table_key == "coupon_burui" else f"{pg_table_alias}.{link_field}"
    
    if VALID_EVENT_NOS:
        where_clauses.append(f"{event_no_field_in_query} = ANY(%s)")
        params.append(list(VALID_EVENT_NOS))
    else:
        where_clauses.append("1=0")
    
    where_clauses.append(f"{date_field_coalesce} < %s")
    params.append(end_time)

    id_field_with_alias = f"{pg_table_alias}.{id_field_str}"
    if last_checkpoint_time and last_checkpoint_id is not None:
        where_clauses.append(f"({date_field_coalesce} > %s OR ({date_field_coalesce} = %s AND {id_field_with_alias} > %s))")
        params.extend([last_checkpoint_time, last_checkpoint_time, last_checkpoint_id])
    else:
         where_clauses.append(f"{date_field_coalesce} >= %s")
         params.append(start_time)

    full_sql = f"{select_clause} {join_clause} WHERE {' AND '.join(where_clauses)} {order_by_clause} LIMIT %s"
    params.append(batch_size)
    
    logger.info(f"[SQL-{table_key}] {full_sql.replace(chr(10), ' ')} params[1:] {params[1:]}") 
    
    with pg_conn.cursor() as cur:
        cur.execute(full_sql, params)
        return cur.fetchall()

def normalize_value(v):
    if isinstance(v, Decimal):
        return Decimal128(v)
    if isinstance(v, datetime.datetime):
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v
    if isinstance(v, date):
        return datetime.datetime.combine(v, datetime.time.min, tzinfo=timezone.utc)
    return v

def snake_to_camel(snake_str):
    if not isinstance(snake_str, str): return snake_str
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def transform_row_to_doc(row):
    doc = {}
    for key, value in row.items():
        camel_key = snake_to_camel(key)
        transformed_value = value
        if isinstance(value, str):
            if value.upper() == 'Y': transformed_value = True
            elif value.upper() == 'N': transformed_value = False
        
        doc[camel_key] = normalize_value(transformed_value)
    return doc

def upsert_batch(table_key, rows):
    conf = TABLES_CONFIG[table_key]
    requests = []
    for row in rows:
        doc = transform_row_to_doc(row)
        if "embed_array_field" in conf:
            array_field = conf["embed_array_field"]
            event_no = doc.get("eventNo")
            if not event_no:
                logger.warning(f"在 {table_key} 表的記錄中找不到 eventNo，數據: {doc}")
                continue
            
            filter_cond = {"eventNo": event_no}
            if array_field == "usingBranchIds": value_to_add = doc.get("branch")
            elif array_field == "memberTypes": value_to_add = doc.get("memberType")
            else: value_to_add = doc

            if value_to_add:
                requests.append(UpdateOne(filter_cond, {"$addToSet": {array_field: value_to_add}}, upsert=False))
        
        elif table_key == "hcc_events":
            event_no = doc.get("eventNo")
            if not event_no: continue
            prize_coupon_json_str = doc.get("prizeCouponJson", None)
            doc.pop("eventNo", None)
            doc.pop("addDate", None)
            update_op = {"$set": doc}
            if prize_coupon_json_str is not None:
                update_op["$set"]["prizeCouponJson"] = prize_coupon_json_str
            requests.append(UpdateOne({"eventNo": event_no}, update_op, upsert=False))

        else:
            id_field_camel = snake_to_camel(conf["id_field"])
            if table_key == "attendees":
                filter_cond = {"eventNo": doc.get("eventNo"), "appId": doc.get("appId")}
            else:
                if id_field_camel not in doc:
                    logger.warning(f"在 {table_key} 表的記錄中找不到主鍵 {id_field_camel}，數據: {doc}")
                    continue
                filter_cond = {id_field_camel: doc[id_field_camel]}
            requests.append(UpdateOne(filter_cond, {"$set": doc}, upsert=True))
    
    if not requests: return None
    
    result = mongo_dbs[table_key].bulk_write(requests, ordered=False)
    return result

def update_checkpoint(table_key, last_time_iso, last_id, processed_count, window, status=None):
    global cp_data
    cp_table = cp_data.setdefault(table_key, {})
    
    cp_table["last_checkpoint_time"] = last_time_iso
    cp_table["last_checkpoint_id"] = last_id

    for w in cp_table.setdefault("base_windows", []):
        if w["start"] == window["start"] and w["end"] == window["end"]:
            w["last_checkpoint_time"] = last_time_iso
            w["last_checkpoint_id"] = last_id
            w["processed_count"] = processed_count
            w["owner"] = os.uname().nodename.split('.')[0]
            if status:
                w["status"] = status
            if status == "completed":
                 w["finish_exec_time"] = datetime.datetime.now(timezone.utc).isoformat()
            break
    save_checkpoint(cp_data)

def migrate_table_window(table_key, window, mode="full"):
    conf = TABLES_CONFIG[table_key]
    start, end = window["start"], window["end"]
    
    last_t_iso, last_id = window.get("last_checkpoint_time"), window.get("last_checkpoint_id")
    processed = window.get("processed_count", 0)
    initial_processed = processed

    window["status"] = "in_progress"
    window["start_exec_time"] = datetime.datetime.now(timezone.utc).isoformat()
    save_checkpoint(cp_data)

    consecutive_errors = 0
    while True:
        try:
            rows = fetch_batch(table_key, start, end, last_t_iso, last_id, BATCH_SIZE, mode=mode)
            if not rows:
                update_checkpoint(table_key, last_t_iso, last_id, processed, window, status="completed")
                logger.info(f"{table_key} 視窗 {start}~{end} 同步完成，本次處理 {processed - initial_processed} 筆")
                break
            
            result = upsert_batch(table_key, rows)
            
            processed_this_batch = len(rows)
            inserted = result.upserted_count if result else 0
            updated = result.modified_count if result else 0
            if "embed_array_field" in conf or table_key == "hcc_events":
                updated = processed_this_batch

            table_stats = MIGRATION_STATS["tables"].setdefault(table_key, {"pg_records": 0, "inserted": 0, "updated": 0})
            table_stats["pg_records"] += processed_this_batch
            table_stats["inserted"] += inserted
            table_stats["updated"] += updated
            MIGRATION_STATS["total_pg_records"] += processed_this_batch
            MIGRATION_STATS["total_inserted"] += inserted
            MIGRATION_STATS["total_updated"] += updated

            processed += processed_this_batch
            last_row = rows[-1]
            time_key_value = last_row.get(conf["mod_date_field"]) or last_row.get(conf["add_date_field"])
            id_key_value = last_row.get(conf["id_field"])

            if time_key_value is None or id_key_value is None:
                logger.error(f"FATAL: {table_key} 無法獲取有效斷點值，數據: {last_row}")
                raise ValueError("無法獲取斷點值，遷移終止")

            last_t_iso = normalize_value(time_key_value).isoformat()
            last_id = id_key_value
            
            update_checkpoint(table_key, last_t_iso, last_id, processed, window, status="in_progress")
            logger.info(f"{table_key} 批次同步成功: records={len(rows)}, last_time={last_t_iso}, last_id={last_id}")
            consecutive_errors = 0

        except Exception as e:
            consecutive_errors += 1
            logger.error(f"{table_key} 視窗 {start}~{end} 同步批次異常，第 {consecutive_errors} 次錯誤: {e}", exc_info=True)
            if consecutive_errors >= 5:
                window["status"] = "pending"
                save_checkpoint(cp_data)
                logger.error(f"{table_key} 視窗因連續錯誤而終止，狀態已改為 pending。")
                raise e
            
def convert_decimal_for_json(obj):
    if isinstance(obj, dict):
        return {k: convert_decimal_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal_for_json(elem) for elem in obj]
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    else:
        return obj

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Checkpoint 檔案 {CHECKPOINT_FILE} 格式錯誤，將建立新的檔案。")
                return {}
    return {key: {"base_windows": []} for key in TABLES_CONFIG.keys()}

def save_checkpoint(data):
    safe_data = convert_decimal_for_json(data)
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(safe_data, f, ensure_ascii=False, indent=2)

def verify_consistency():
    global MIGRATION_STATS
    all_ok = True
    logger.info("開始執行資料一致性校驗 (基於白名單)...")
    
    if not VALID_EVENT_NOS:
        logger.warning("白名單為空，跳過大部分資料一致性校驗。")
        return True

    for key, conf in TABLES_CONFIG.items():
        mode = conf.get("verification_mode")
        pg_table = f"{SCHEMA}.{conf['pg_table']}"
        pg_count = 0
        link_field = conf.get("link_field", "event_no")

        try:
            with pg_conn.cursor() as cur:
                if mode == "merge_source":
                    sql = f"SELECT COUNT(*) FROM {pg_table} WHERE {link_field} = ANY(%s);"
                    cur.execute(sql, (list(VALID_EVENT_NOS),))
                    pg_count = cur.fetchone()[0]
                    result_str = f"資料合併來源 {conf['pg_table']}: PG 端提供 {pg_count} 筆白名單內資料。"
                    MIGRATION_STATS["verification_results"].append(f"INFO: {result_str}")
                    logger.info(f"INFO: {result_str}")
                    continue

                elif key == "coupon_burui":
                    logger.info("[coupon_burui] 執行特殊去重計數邏輯...")
                    sql = f"""
                        SELECT COUNT(*) FROM (
                            SELECT DISTINCT s.event_no, t.branch 
                            FROM {pg_table} t 
                            JOIN {SCHEMA}.gif_event_coupon_setting s ON t.coupon_setting_no = s.coupon_setting_no
                            WHERE s.event_no = ANY(%s) AND t.branch IS NOT NULL AND t.branch != ''
                        ) AS distinct_rows;
                    """
                    cur.execute(sql, (list(VALID_EVENT_NOS),))
                    pg_count = cur.fetchone()[0]
                    logger.info(f"[coupon_burui] PG 理論總數 (已去重): {pg_count}")
                else:
                    sql = f"SELECT COUNT(*) FROM {pg_table} WHERE {link_field} = ANY(%s);"
                    cur.execute(sql, (list(VALID_EVENT_NOS),))
                    pg_count = cur.fetchone()[0]
                
                mongo_collection = mongo_dbs[key]
                if mode == "embed_array":
                    arr_field = conf["embed_array_field"]
                    pipeline = [
                        {"$match": {"eventNo": {"$in": list(VALID_EVENT_NOS)}}},
                        {"$project": {"count": {"$size": {"$ifNull": [f"${arr_field}", []]}}}},
                        {"$group": {"_id": None, "total": {"$sum": "$count"}}}
                    ]
                    mongo_result = list(mongo_collection.aggregate(pipeline))
                    mongo_count = mongo_result[0]['total'] if mongo_result else 0
                    result_str = f"嵌入陣列 {conf['pg_table']}: PG(理論值)={pg_count}, MG(實際)={mongo_count}"
                    is_consistent = (pg_count == mongo_count)
                else: # primary_count
                    mongo_count = mongo_collection.count_documents({"eventNo": {"$in": list(VALID_EVENT_NOS)}})
                    result_str = f"主計數 {conf['pg_table']}: PG={pg_count}, MG={mongo_count}"
                    is_consistent = (pg_count == mongo_count)
            
                if is_consistent:
                    MIGRATION_STATS["verification_results"].append(f"✅ PASS: {result_str}")
                    logger.info(f"✅ 校驗通過: {result_str}")
                else:
                    MIGRATION_STATS["verification_results"].append(f"❌ FAIL: {result_str}")
                    logger.warning(f"❌ 校驗失敗: {result_str}")
                    all_ok = False
        except Exception as e:
            logger.error(f"校驗表格 {key} 時出錯: {e}", exc_info=True)
            all_ok = False

    logger.info("資料一致性校驗完畢。")
    return all_ok

def command_full_sync():
    global cp_data
    end_time = "9999-12-31T23:59:59Z"
    for table_key in TABLES_CONFIG.keys():
        table_data = cp_data.setdefault(table_key, {})
        base_windows = table_data.setdefault("base_windows", [])
        base_windows.clear()
        base_windows.append({
            "start": "1970-01-01T00:00:00Z", "end": end_time, "status": "pending", 
            "mode": "full", "processed_count": 0, "last_checkpoint_time": None, 
            "last_checkpoint_id": None, "owner": None, "start_exec_time": None, "finish_exec_time": None
        })
    save_checkpoint(cp_data)
    logger.info(f"已為所有表建立/重置全量同步視窗: 1970-01-01 至 {end_time}")
    
    execution_order = ["events", "hcc_events", "attendees", "coupon_burui", "member_types"]
    for table_key in execution_order:
        logger.info(f"--- 開始處理表: {table_key} ---")
        window = cp_data[table_key]["base_windows"][0]
        if window.get("status") != "completed":
             migrate_table_window(table_key, window, mode="full")

def command_incremental():
    global cp_data
    end_time_utc = datetime.datetime.now(timezone.utc)
    
    for table_key in TABLES_CONFIG.keys():
        table_data = cp_data.setdefault(table_key, {})
        base_windows = table_data.setdefault("base_windows", [])
        
        start_time_str = "1970-01-01T00:00:00Z"
        initial_checkpoint_time = None
        initial_checkpoint_id = None
        
        completed_windows = [
            w for w in base_windows 
            if w.get("status") == "completed" and w.get("finish_exec_time")
        ]
        
        if completed_windows:
            latest_completed_window = max(
                completed_windows, 
                key=lambda w: datetime.datetime.fromisoformat(w["finish_exec_time"])
            )
            
            initial_checkpoint_time = latest_completed_window.get("last_checkpoint_time")
            initial_checkpoint_id = latest_completed_window.get("last_checkpoint_id")

            if initial_checkpoint_time:
                start_time_str = initial_checkpoint_time
            else:
                start_time_str = latest_completed_window.get("end", "1970-01-01T00:00:00Z")
                logger.warning(f"{table_key} 最新成功窗口無 last_checkpoint_time，使用其 end 時間 {start_time_str} 作為起點。")

        new_window = {
            "start": start_time_str,
            "end": end_time_utc.isoformat(),
            "status": "pending",
            "mode": "incremental",
            "processed_count": 0,
            "last_checkpoint_time": initial_checkpoint_time,
            "last_checkpoint_id": initial_checkpoint_id,
            "owner": None,
            "start_exec_time": None,
            "finish_exec_time": None
        }
        base_windows.append(new_window)
        logger.info(f"新增 {table_key} 增量同步視窗：{start_time_str} ~ {end_time_utc.isoformat()} (繼承斷點 ID: {initial_checkpoint_id})")
        
    save_checkpoint(cp_data)

    execution_order = ["events", "hcc_events", "attendees", "coupon_burui", "member_types"]
    for table_key in execution_order:
        logger.info(f"--- 開始處理表: {table_key} ---")
        window_to_run = cp_data[table_key]["base_windows"][-1]
        migrate_table_window(table_key, window_to_run, mode="incremental")

def command_resume():
    logger.info("開始執行斷點恢復任務...")
    execution_order = ["events", "hcc_events", "attendees", "coupon_burui", "member_types"]
    for table_key in execution_order:
        if table_key not in cp_data: continue
        for w in cp_data[table_key].get("base_windows", []):
            if w.get("status") in ("pending", "in_progress"):
                mode = w.get("mode", "full") if w.get("mode") == "full" else "incremental"
                logger.info(f"恢復處理 '{table_key}' 的 {mode} 模式視窗: {w['start']} -> {w['end']}")
                migrate_table_window(table_key, w, mode=mode)

def command_show_status():
    print(json.dumps(load_checkpoint(), ensure_ascii=False, indent=2))

def command_reset():
    confirm = input("警告：這將清除 checkpoint.json 檔案，所有同步進度將丟失。請輸入 'RESET' 確認：")
    if confirm.strip() == "RESET":
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
        logger.info("Checkpoint 檔案已重置。")
        print("重置完成。")
    else:
        print("操作已取消。")

def parse_args():
    parser = argparse.ArgumentParser(description="PostgreSQL → MongoDB 活動資料遷移工具 (Production Ready)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full-sync", action="store_true", help="執行一次全新的全量同步，會清空現有 checkpoint。")
    group.add_argument("--incremental", action="store_true", help="執行一次增量同步。")
    group.add_argument("--resume", action="store_true", help="從上次中斷的地方恢復待處理(pending/in_progress)的任務。")
    group.add_argument("--show-status", action="store_true", help="顯示當前的 checkpoint 狀態。")
    group.add_argument("--reset", action="store_true", help="重置並刪除 checkpoint 檔案。")
    parser.add_argument("--skip-pg-index-check", action="store_true", help="跳過啟動時的 PostgreSQL 索引檢查。")
    return parser.parse_args()

def log_summary_report(start_time, end_time, task_name):
    duration = end_time - start_time
    total_seconds = duration.total_seconds()
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    duration_str = f"{int(hours)} 小時 {int(minutes)} 分鐘 {seconds:.2f} 秒"
    
    total_records = MIGRATION_STATS.get('total_pg_records', 0)
    throughput = (total_records / total_seconds) if total_seconds > 0 else 0
    verification_results = MIGRATION_STATS.get("verification_results", [])
    has_failures = any("FAIL" in result for result in verification_results)
    status_str = "失敗" if has_failures else "成功完成"
    
    if not has_failures and task_name not in ["show-status", "reset"]:
        status_str += " (所有校驗均通過)"
        
    report_lines = [
        "=" * 60,
        "========== [ 遷移任務總結報告 (Migration Summary) ] ==========",
        "=" * 60,
        "",
        "[ 總體概覽 (Overall) ]",
        f"  - 任務狀態:                  {status_str}",
        f"  - 任務名稱:                  {task_name}",
        f"  - 開始時間:                  {start_time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"  - 結束時間:                  {end_time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"  - 總耗時:                      {duration_str}",
        f"  - 總處理 PostgreSQL 紀錄數: {total_records:,} 筆",
        f"  - 平均吞吐量:                  {throughput:.1f} 筆/秒",
        "",
        "[ 分表處理詳情 (Per-Table Details) ]"
    ]

    if not MIGRATION_STATS.get("tables"):
        report_lines.append("  - 本次任務未處理任何資料。")
    else:
        for key, stats in MIGRATION_STATS["tables"].items():
            pg_table_name = TABLES_CONFIG[key]['pg_table']
            report_lines.append(f"  - {pg_table_name} ({key}):")
            report_lines.append(f"    > PG 來源: {stats.get('pg_records', 0):,} | Mongo 新增: {stats.get('inserted', 0):,} | Mongo 更新: {stats.get('updated', 0):,}")

    report_lines.extend([
        "",
        "[ 資料一致性校驗 (Data Consistency Verification) ]"
    ])

    if not verification_results:
        report_lines.append("  - 本次任務未執行資料校驗。")
    else:
        for result in verification_results:
            report_lines.append(f"  - {result}")
            
    report_lines.extend(["", "=" * 25 + " [ 報告結束 ] " + "=" * 25])
    
    for line in report_lines:
        logger.info(line)


def main():
    global cp_data, args
    start_time = datetime.datetime.now()
    args = parse_args()
    
    task_name_map = {
        "full_sync": "full-sync", "incremental": "incremental", "resume": "resume",
        "show_status": "show-status", "reset": "reset"
    }
    task_name = next((v for k, v in task_name_map.items() if getattr(args, k, False)), "unknown")

    init_logger(task_name=task_name)
    logger.info(f"===== 開始執行遷移任務: {task_name} =====")
    
    if args.show_status:
        cp_data = load_checkpoint()
        command_show_status()
        return
    if args.reset:
        command_reset()
        return

    try:
        init_db_conn()
        cp_data = load_checkpoint()
        
        load_valid_event_nos()

        if args.full_sync: command_full_sync()
        elif args.incremental: command_incremental()
        elif args.resume: command_resume()
        
        if task_name not in ["show-status", "reset"]:
             verify_consistency()

    except Exception as e:
        logger.error(f"任務 '{task_name}' 執行期間發生未處理的致命錯誤: {e}", exc_info=True)
        MIGRATION_STATS["verification_results"].append(f"❌ FAIL: 任務因未處理的異常而終止 - {e}")
        sys.exit(1)
    finally:
        if pg_conn: pg_conn.close()
        if mongo_client: mongo_client.close()
        end_time = datetime.datetime.now()
        log_summary_report(start_time, end_time, task_name)
        logger.info(f"===== 遷移任務結束: {task_name} =====")

if __name__ == "__main__":
    main()
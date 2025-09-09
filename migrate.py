#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PostgreSQL → MongoDB 活動資料初始化遷移工具，支援多表雙鍵斷點管理，多模式命令列控制
功能：
- 主表及子表分表管理各自斷點(add_date + id / mod_date + id組合雙鍵)
- 支援全量同步(add_date)、增量同步(mod_date)、補寫、斷點恢復
- 狀態集中於 base_windows & correction_windows，支援批次級錯誤重試
- 日誌依同步類型與視窗分檔記錄詳細成功與異常
- 全量一致性統計，包含嵌入主表文件的陣列欄位比對
- 配合 checkpoint.json 多表獨立管理，保持運維友好
"""

import os
import sys
import json
import logging
import datetime
import argparse
import re

import psycopg2
from psycopg2.extras import DictCursor
from pymongo import MongoClient, UpdateOne, IndexModel
from dotenv import load_dotenv
from decimal import Decimal
from bson.decimal128 import Decimal128
from datetime import date

# ===== 環境與設定 =====
ENV = os.getenv("ENV", "dev")
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
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))
SCHEMA = "gift2022"

CHECKPOINT_FILE = "migration_checkpoint.json"
LOG_DIR = "logs"
LOG_FILE = None
logger = None
args = None # 全域變數，用於儲存命令列參數

# --- 新增這個全域統計變數 ---
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
        "id_field": "id", "mongo_collection": "event_attendees", "verification_mode": "primary_count"
    },
    "coupon_burui": {
        "pg_table": "gif_event_coupon_burui", "add_date_field": "add_date", "mod_date_field": "mod_date",
        "id_field": "id", "mongo_collection": "events", "embed_array_field": "usingBranchIds",
        "verification_mode": "embed_array"
    },
    "coupon_setting": {
        "pg_table": "gif_event_coupon_setting", "add_date_field": "add_date", "mod_date_field": "mod_date",
        "id_field": "coupon_setting_no", "mongo_collection": "events", "embed_array_field": "prizeCouponJson",
        "verification_mode": "embed_array"
    },
    "member_types": {
        "pg_table": "gif_hcc_event_member_type", "add_date_field": "add_date", "mod_date_field": "mod_date",
        "id_field": "id", "mongo_collection": "events", "embed_array_field": "memberTypes",
        "verification_mode": "embed_array"
    },
}

def init_logger(task_name="general"):
    global LOG_FILE, logger
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    short_hostname = os.uname().nodename.split('.')[0]
    pid = os.getpid()
    log_filename = f"{timestamp}_{task_name}_{short_hostname}_{pid}.log"
    LOG_FILE = os.path.join(LOG_DIR, log_filename)
    if logging.getLogger().hasHandlers():
        logging.getLogger().handlers.clear()
    logging.basicConfig(
        filename=LOG_FILE, level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s",
    )
    logger = logging.getLogger()

pg_conn = None
pg_cursor = None
mongo_client = None
mongo_dbs = {}

def ensure_mongodb_indexes(mdb):
    logger.info("開始檢查並建立 MongoDB 索引...")
    try:
        events_collection = mdb["events"]
        event_no_index = IndexModel([("eventNo", 1)], name="idx_eventNo")
        events_collection.create_indexes([event_no_index])
        logger.info(f"成功檢查/建立 events 集合的索引: idx_eventNo")
        attendees_collection = mdb["event_attendees"]
        attendee_compound_index = IndexModel([("eventNo", 1), ("appId", 1)], name="idx_eventNo_appId")
        attendees_collection.create_indexes([attendee_compound_index])
        logger.info(f"成功檢查/建立 event_attendees 集合的索引: idx_eventNo_appId")
        logger.info("MongoDB 索引檢查完畢。")
    except Exception as e:
        logger.error(f"建立 MongoDB 索引時發生錯誤: {e}")

def ensure_postgresql_indexes(conn):
    logger.info("開始檢查並建立 PostgreSQL 索引...")
    indexes_to_create = [
        f"CREATE INDEX IF NOT EXISTS ix_gif_event_add_date_event_no ON {SCHEMA}.gif_event (add_date, event_no)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_hcc_event_add_date_event_no ON {SCHEMA}.gif_hcc_event (add_date, event_no)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_hcc_event_attendee_add_date_id ON {SCHEMA}.gif_hcc_event_attendee (add_date, id)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_hcc_event_member_type_add_date_id ON {SCHEMA}.gif_hcc_event_member_type (add_date, id)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_event_coupon_burui_add_date_id ON {SCHEMA}.gif_event_coupon_burui (add_date, id)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_event_coupon_setting_add_date_coupon_setting_no ON {SCHEMA}.gif_event_coupon_setting (add_date, coupon_setting_no)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_event_mod_date ON {SCHEMA}.gif_event (mod_date)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_hcc_event_mod_date ON {SCHEMA}.gif_hcc_event (mod_date)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_hcc_event_attendee_mod_date ON {SCHEMA}.gif_hcc_event_attendee (mod_date)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_event_coupon_burui_coupon_setting_no ON {SCHEMA}.gif_event_coupon_burui (coupon_setting_no)",
        f"CREATE INDEX IF NOT EXISTS ix_gif_event_coupon_setting_coupon_setting_no ON {SCHEMA}.gif_event_coupon_setting (coupon_setting_no)"
    ]
    with conn.cursor() as cur:
        for sql in indexes_to_create:
            try:
                logger.info(f"執行: {sql}")
                cur.execute(sql)
            except Exception as e:
                logger.error(f"建立 PostgreSQL 索引失敗: {sql} - 錯誤: {e}")
                conn.rollback()
                return
        conn.commit()
    logger.info("PostgreSQL 索引檢查完畢。")

def init_db_conn():
    global pg_conn, pg_cursor, mongo_client, mongo_dbs
    try:
        pg_conn = psycopg2.connect(**POSTGRESQL_CONFIG, cursor_factory=DictCursor)
        pg_cursor = pg_conn.cursor()
        if not args.skip_pg_index_check:
            ensure_postgresql_indexes(pg_conn)
        else:
            logger.warning("已跳過 PostgreSQL 索引檢查。請確保資料庫端索引已手動建立以保證查詢效能。")
    except Exception as e:
        logger.error(f"無法連接 PostgreSQL 或建立索引：{e}")
        sys.exit(1)
    try:
        mongo_client = MongoClient(MONGO_URI)
        mdb = mongo_client["gift"]
        ensure_mongodb_indexes(mdb)
        for k, v in TABLES_CONFIG.items():
            mongo_dbs[k] = mdb[v["mongo_collection"]]
    except Exception as e:
        logger.error(f"無法連接 MongoDB 或建立索引：{e}")
        sys.exit(1)

def fetch_batch(table_key, start_time, end_time,
                  last_checkpoint_time=None, last_checkpoint_id=None,
                  batch_size=100, mode="incremental"):
    conf = TABLES_CONFIG[table_key]
    join_clause = ""
    base_sql_select = f"SELECT t.* FROM {SCHEMA}.{conf['pg_table']} t"
    if table_key == "coupon_burui":
        join_clause = f"JOIN {SCHEMA}.gif_event_coupon_setting s ON t.coupon_setting_no = s.coupon_setting_no"
        base_sql_select = f"SELECT t.*, s.event_no FROM {SCHEMA}.{conf['pg_table']} t"
    date_field_str = conf['add_date_field']
    mod_date_field_str = conf['mod_date_field']
    id_field_str = conf['id_field']
    if mode == "full":
        date_field = f"COALESCE(t.{date_field_str}, '1970-01-01')"
    else:
        date_field = f"COALESCE(t.{mod_date_field_str}, t.{date_field_str}, '1970-01-01')"
    order_by_clause = f"ORDER BY {date_field}, t.{id_field_str}"
    base_sql = f"{base_sql_select} {join_clause}"
    params = []
    where_clauses = []
    if last_checkpoint_time is None or last_checkpoint_id is None:
        where_clauses.append(f"{date_field} >= %s AND {date_field} < %s")
        params.extend([start_time, end_time])
    else:
        id_field_with_alias = f"t.{id_field_str}"
        where_clauses.append(f"({date_field} > %s OR ({date_field} = %s AND {id_field_with_alias} > %s))")
        params.extend([last_checkpoint_time, last_checkpoint_time, last_checkpoint_id])
        where_clauses.append(f"{date_field} >= %s AND {date_field} < %s")
        params.extend([start_time, end_time])
    sql = f"{base_sql} WHERE {' AND '.join(where_clauses)} {order_by_clause} LIMIT %s"
    params.append(batch_size)
    logger.info(f"[SQL-{table_key}] {sql.replace(chr(10), ' ')} params {params}")
    pg_cursor.execute(sql, params)
    return pg_cursor.fetchall()

def normalize_value(v):
    if isinstance(v, Decimal):
        try:
            return Decimal128(v)
        except (TypeError, ValueError, OverflowError):
            return float(v)
    if isinstance(v, datetime.datetime):
        return v
    if isinstance(v, date):
        return datetime.datetime.combine(v, datetime.datetime.min.time())
    return v

def snake_to_camel(snake_str):
    if not isinstance(snake_str, str):
        return snake_str
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def transform_row_to_doc(row):
    doc = {}
    for key, value in row.items():
        camel_key = snake_to_camel(key)
        transformed_value = value
        if isinstance(value, str):
            if value.upper() == 'Y':
                transformed_value = True
            elif value.upper() == 'N':
                transformed_value = False
        if camel_key == 'prizeCouponJson' and isinstance(transformed_value, str):
            if not transformed_value:
                transformed_value = []
            else:
                try:
                    parsed_data = json.loads(transformed_value)
                    if isinstance(parsed_data, list):
                        transformed_value = parsed_data
                    else:
                        transformed_value = [parsed_data]
                except json.JSONDecodeError:
                    print(f"警告：在 eventNo={row.get('event_no')} 中發現無效的 prizeCouponJson 字符串，將其設置為空陣列。內容: {transformed_value[:100]}")
                    transformed_value = []
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
            if array_field == "usingBranchIds":
                branch_id = doc.get("branch")
                if branch_id:
                    requests.append(UpdateOne(filter_cond, {"$addToSet": {array_field: branch_id}}, upsert=True))
            elif array_field == "memberTypes":
                member_type = doc.get("memberType")
                if member_type:
                    requests.append(UpdateOne(filter_cond, {"$addToSet": {array_field: member_type}}, upsert=True))
            else:
                requests.append(UpdateOne(filter_cond, {"$addToSet": {array_field: doc}}, upsert=True))
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
    if not requests:
        class EmptyResult:
            upserted_count = 0
            modified_count = 0
        return EmptyResult()
    result = mongo_dbs[table_key].bulk_write(requests, ordered=False)
    return result

def update_checkpoint(table_key, last_time, last_id, processed_count, window=None, status=None):
    global cp_data
    cp_table = cp_data.setdefault(table_key, {})
    cp_table["last_checkpoint_time"] = last_time
    cp_table["last_checkpoint_id"] = last_id
    if window:
        for w in cp_table.setdefault("base_windows", []):
            if w["start"] == window["start"] and w["end"] == window["end"]:
                w["last_checkpoint_time"] = last_time
                w["last_checkpoint_id"] = last_id
                w["processed_count"] = processed_count
                if status:
                    w["status"] = status
                w["last_mod_date"] = datetime.datetime.now().isoformat()
                break
    save_checkpoint(cp_data)

def migrate_table_window(table_key, window, mode="incremental"):
    global cp_data
    conf = TABLES_CONFIG[table_key]
    start = window["start"]
    end = window["end"]
    last_t = window.get("last_checkpoint_time")
    last_id = window.get("last_checkpoint_id")
    processed = 0
    window["status"] = "in_progress"
    window["start_exec_time"] = datetime.datetime.now().isoformat()
    save_checkpoint(cp_data)
    consecutive_errors = 0
    while True:
        rows = fetch_batch(table_key, start, end, last_t, last_id, BATCH_SIZE, mode=mode)
        if not rows:
            window["status"] = "completed"
            window["finish_exec_time"] = datetime.datetime.now().isoformat()
            save_checkpoint(cp_data)
            logger.info(f"{table_key} 視窗 {start}~{end} 同步完成，總計 {processed} 筆")
            break
        try:
            result = upsert_batch(table_key, rows)
            table_stats = MIGRATION_STATS["tables"].setdefault(table_key, {"pg_records": 0, "inserted": 0, "updated": 0})
            processed_this_batch = len(rows)
            inserted_this_batch = result.upserted_count
            updated_this_batch = result.modified_count if "embed_array_field" not in conf else processed_this_batch
            table_stats["pg_records"] += processed_this_batch
            table_stats["inserted"] += inserted_this_batch
            table_stats["updated"] += updated_this_batch
            MIGRATION_STATS["total_pg_records"] += processed_this_batch
            MIGRATION_STATS["total_inserted"] += inserted_this_batch
            MIGRATION_STATS["total_updated"] += updated_this_batch
            processed += len(rows)
            last_row = rows[-1]
            if mode == "incremental":
                time_key_value = last_row.get(conf["mod_date_field"]) or last_row.get(conf["add_date_field"])
            else:
                time_key_value = last_row.get(conf["add_date_field"])
            id_key_value = last_row.get(conf["id_field"])
            if time_key_value is None or id_key_value is None:
                logger.error(f"{table_key} 無法獲取有效的斷點值，時間: {time_key_value}, ID: {id_key_value}，最後一行數據: {last_row}")
                continue
            last_t = time_key_value.isoformat()
            last_id = id_key_value
            update_checkpoint(table_key, last_t, last_id, processed, window, status="in_progress")
            consecutive_errors = 0
            logger.info(f"{table_key} 同步批次成功，records={len(rows)}, last_time={last_t}, last_id={last_id}")
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"{table_key} 視窗 {start}~{end} 同步批次異常，第 {consecutive_errors} 次錯誤: {e}", exc_info=True)
            if consecutive_errors >= 5:
                window["status"] = "pending"
                save_checkpoint(cp_data)
                logger.error(f"{table_key} 視窗多次失敗終止，狀態改為 pending")
                break
    return processed

def convert_decimal_for_json(obj):
    if isinstance(obj, dict):
        return {k: convert_decimal_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal_for_json(elem) for elem in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {table_key: {"base_windows": [], "correction_windows": [], "last_checkpoint_time": None, "last_checkpoint_id": None} for table_key in TABLES_CONFIG.keys()}

def save_checkpoint(data):
    safe_data = convert_decimal_for_json(data)
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(safe_data, f, ensure_ascii=False, indent=2)

def verify_consistency():
    global MIGRATION_STATS
    all_ok = True
    logger.info("開始執行資料一致性校驗...")
    for key, conf in TABLES_CONFIG.items():
        mode = conf.get("verification_mode", "primary_count")
        pg_table_name = conf['pg_table']
        pg_count = 0
        if key == "coupon_setting":
            logger.info(f"[{key}] 執行 coupon_setting 的特殊合併計數邏輯...")
            sql_a = f"SELECT SUM(json_array_length(prize_coupon_json::json)) FROM {SCHEMA}.gif_hcc_event WHERE prize_coupon_json IS NOT NULL AND prize_coupon_json LIKE '[%]';"
            pg_cursor.execute(sql_a)
            count_a = pg_cursor.fetchone()[0] or 0
            sql_b = f"SELECT COUNT(*) FROM {SCHEMA}.{pg_table_name};"
            pg_cursor.execute(sql_b)
            count_b = pg_cursor.fetchone()[0] or 0
            pg_count = int(count_a) + int(count_b)
            logger.info(f"[{key}] PG 理論總數: (來自 gif_hcc_event.prize_coupon_json 的 {count_a}) + (來自 {pg_table_name} 的 {count_b}) = {pg_count}")
        elif key == "coupon_burui":
            logger.info(f"[{key}] 執行 coupon_burui 的特殊去重計數邏輯...")
            sql = f"SELECT COUNT(*) FROM (SELECT DISTINCT s.event_no, t.branch FROM {SCHEMA}.{pg_table_name} t INNER JOIN {SCHEMA}.gif_event_coupon_setting s ON t.coupon_setting_no = s.coupon_setting_no WHERE t.branch IS NOT NULL AND t.branch != '') AS distinct_rows;"
            pg_cursor.execute(sql)
            pg_count = pg_cursor.fetchone()[0] or 0
            logger.info(f"[{key}] PG 理論總數 (已去重): {pg_count}")
        else:
            pg_cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{pg_table_name}")
            pg_count = pg_cursor.fetchone()[0] or 0

        if mode == "merge_source":
            result_str = f"資料合併來源 {pg_table_name}: PG 端提供 {pg_count} 筆資料。"
            MIGRATION_STATS["verification_results"].append(f"INFO: {result_str}")
            logger.info(f"INFO: {result_str}")
            continue
        elif mode == "embed_array":
            arr_field = conf["embed_array_field"]
            mongo_collection_name = conf["mongo_collection"]
            mongo_collection = mongo_client["gift"][mongo_collection_name]
            pipeline = [{"$project": {"count": {"$size": {"$ifNull": [f"${arr_field}", []]}}}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
            mongo_result = list(mongo_collection.aggregate(pipeline))
            mongo_count = mongo_result[0]['total'] if mongo_result else 0
            result_str = f"嵌入陣列 {pg_table_name}: PG(理論值)={pg_count}, MG(實際)={mongo_count}"
            is_consistent = (pg_count == mongo_count)
        elif mode == "primary_count":
            mongo_count = mongo_dbs[key].count_documents({})
            result_str = f"主計數 {pg_table_name}: PG={pg_count}, MG={mongo_count}"
            is_consistent = (pg_count == mongo_count)

        if is_consistent:
            MIGRATION_STATS["verification_results"].append(f"✅ PASS: {result_str}")
            logger.info(f"✅ 校驗通過: {result_str}")
        else:
            MIGRATION_STATS["verification_results"].append(f"❌ FAIL: {result_str}")
            logger.warning(f"❌ 校驗失敗: {result_str}")
            all_ok = False
    logger.info("資料一致性校驗完畢。")
    return all_ok

def command_full_sync(end_time=None):
    global cp_data
    end_time = "9999-12-31T23:59:59Z"
    for table_key in TABLES_CONFIG.keys():
        base_windows = cp_data.setdefault(table_key, {}).setdefault("base_windows", [])
        exist = any(w for w in base_windows if w["start"] == "1970-01-01T00:00:00" and w["end"] == end_time)
        if not exist:
            base_windows.append({"start": "1970-01-01T00:00:00", "end": end_time, "status": "pending", "mode": "full", "processed_count": 0, "last_checkpoint_time": None, "last_checkpoint_id": None, "last_mod_date": None, "owner": None, "start_exec_time": None, "finish_exec_time": None})
    save_checkpoint(cp_data)
    logger.info(f"新增所有表基線視窗: 1970-01-01 至 {end_time}")
    total = 0
    for table_key in TABLES_CONFIG.keys():
        for w in cp_data[table_key].get("base_windows", []):
            if w.get("status") in ("pending", "in_progress"):
                total += migrate_table_window(table_key, w, mode="full")
    print(f"全量同步完成，共同步 {total} 筆記錄")

def command_incremental(end_time=None):
    global cp_data
    if end_time is None:
        end_time = datetime.datetime.now().isoformat()
    total = 0
    for table_key in TABLES_CONFIG.keys():
        table_data = cp_data.setdefault(table_key, {})
        base_windows = table_data.setdefault("base_windows", [])
        last_completed_end_time = "1970-01-01T00:00:00"
        if base_windows:
            completed_windows = [w for w in base_windows if w.get("status") == "completed"]
            if completed_windows:
                last_completed_end_time = max(w["end"] for w in completed_windows)
        new_window = {"start": last_completed_end_time, "end": end_time, "status": "pending", "mode": "incremental", "processed_count": 0, "last_checkpoint_time": None, "last_checkpoint_id": None, "last_mod_date": None, "owner": None, "start_exec_time": None, "finish_exec_time": None}
        base_windows.append(new_window)
        logger.info(f"新增 {table_key} 增量同步視窗：{last_completed_end_time} ~ {end_time}")
        total += migrate_table_window(table_key, new_window, mode="incremental")
    save_checkpoint(cp_data)
    print(f"增量同步完成，共同步 {total} 筆記錄")

def command_correction(table_key, start_time, end_time):
    global cp_data
    corr = cp_data.setdefault(table_key, {}).setdefault("correction_windows", [])
    corr.append({"start": start_time, "end": end_time, "status": "pending", "mode": "incremental", "processed_count": 0, "last_checkpoint_time": None, "last_checkpoint_id": None, "last_mod_date": None, "owner": None, "start_exec_time": None, "finish_exec_time": None})
    save_checkpoint(cp_data)
    logger.info(f"新增 {table_key} 補寫同步視窗：{start_time} ~ {end_time}")
    total = 0
    for w in corr:
        if w["status"] in ("pending", "in_progress"):
            total += migrate_table_window(table_key, w, mode="incremental")
    print(f"{table_key} 補寫同步完成，共同步 {total} 筆記錄")

def command_resume():
    global cp_data
    total = 0
    for table_key in TABLES_CONFIG.keys():
        for window_type in ("base_windows", "correction_windows"):
            for w in cp_data[table_key].get(window_type, []):
                if w["status"] in ("pending", "in_progress"):
                    mode_to_run = w.get("mode", "incremental")
                    logger.info(f"Resuming window for '{table_key}' in '{mode_to_run}' mode. Window: {w['start']} -> {w['end']}")
                    total += migrate_table_window(table_key, w, mode=mode_to_run)
    print(f"斷點恢復完成，共同步 {total} 筆記錄")

def command_show_status():
    global cp_data
    print(json.dumps(cp_data, ensure_ascii=False, indent=2))

def command_reset():
    confirm = input("警告：重置斷點將清除所有同步進度且無法恢復，請輸入 Y 確認：")
    if confirm.strip().upper() == "Y":
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
        global cp_data
        cp_data = load_checkpoint()
        logger.info("斷點檔案已重置")
        print("重置完成")
    else:
        print("操作已取消")

def parse_args():
    parser = argparse.ArgumentParser(description="PostgreSQL → MongoDB 活動資料遷移工具")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full-sync", action="store_true", help="全量同步")
    group.add_argument("--incremental", action="store_true", help="增量同步")
    group.add_argument("--correction", nargs=3, metavar=('TABLE', 'START', 'END'), help="補寫同步 eg: --correction attendees 2025-08-01T00:00:00 2025-08-01T03:00:00")
    group.add_argument("--resume", action="store_true", help="斷點恢復")
    group.add_argument("--show-status", action="store_true", help="查看斷點狀態")
    group.add_argument("--reset", action="store_true", help="重置所有斷點")
    parser.add_argument("--skip-pg-index-check", action="store_true", help="【維護用】跳過 PostgreSQL 的索引檢查與建立")
    parser.add_argument("--end", type=str, default=None, help="截止時間 ISO格式或 now（增量/全量同步）")
    args = parser.parse_args()
    if args.end is None or (isinstance(args.end, str) and args.end.lower() == "now"):
        args.end = datetime.datetime.now().isoformat()
    return args

def log_summary_report(start_time, end_time, task_name):
    duration = end_time - start_time
    total_seconds = duration.total_seconds()
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    duration_str = f"{int(hours)} 小時 {int(minutes)} 分鐘 {seconds:.2f} 秒"
    total_records = MIGRATION_STATS['total_pg_records']
    throughput = (total_records / total_seconds) if total_seconds > 0 else 0
    has_failures = any("FAIL" in result for result in MIGRATION_STATS["verification_results"])
    status_str = "失敗" if has_failures else "成功完成"
    if not has_failures and task_name not in ["show-status", "reset"]:
        status_str += " (所有校驗均通過)"
    logger.info("=" * 60)
    logger.info("========== [ 遷移任務總結報告 (Migration Summary) ] ==========")
    logger.info("=" * 60)
    logger.info("")
    logger.info("[ 總體概覽 (Overall) ]")
    logger.info(f"  - 任務狀態:               {status_str}")
    logger.info(f"  - 任務名稱:               {task_name}")
    logger.info(f"  - 開始時間:               {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  - 結束時間:               {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  - 總耗時:                   {duration_str}")
    logger.info(f"  - 總處理 PostgreSQL 紀錄數: {total_records:,} 筆")
    logger.info(f"  - 平均吞吐量:               {throughput:.1f} 筆/秒")
    logger.info("")
    logger.info("[ 分表處理詳情 (Per-Table Details) ]")
    if not MIGRATION_STATS["tables"]:
        logger.info("  - 本次任務未處理任何資料。")
    else:
        for key, stats in MIGRATION_STATS["tables"].items():
            pg_table_name = TABLES_CONFIG[key]['pg_table']
            logger.info(f"  - {pg_table_name} ({key}):")
            logger.info(f"    > PG 來源: {stats['pg_records']:,} | Mongo 新增: {stats['inserted']:,} | Mongo 更新: {stats['updated']:,}")
    logger.info("")
    logger.info("[ 資料一致性校驗 (Data Consistency Verification) ]")
    if not MIGRATION_STATS["verification_results"]:
        logger.info("  - 本次任務未執行資料校驗。")
    else:
        for result in MIGRATION_STATS["verification_results"]:
            logger.info(f"  - {result}")
    logger.info("")
    logger.info("=" * 25 + " [ 報告結束 ] " + "=" * 25)

def main():
    global cp_data, args
    start_time = datetime.datetime.now()
    args = parse_args()
    task_name = "unknown"
    if args.full_sync: task_name = "full-sync"
    elif args.incremental: task_name = "incremental"
    elif args.correction: task_name = f"correction-{args.correction[0]}"
    elif args.resume: task_name = "resume"
    elif args.show_status: task_name = "show-status"
    elif args.reset: task_name = "reset"
    init_logger(task_name=task_name)
    logger.info(f"===== 開始執行遷移任務: {task_name} =====")
    init_db_conn()
    cp_data = load_checkpoint()
    if args.full_sync: command_full_sync(end_time=args.end)
    elif args.incremental: command_incremental(end_time=args.end)
    elif args.correction:
        table_key, start, end = args.correction
        if table_key not in TABLES_CONFIG:
            print(f"未知的 table: {table_key}")
            sys.exit(1)
        command_correction(table_key, start, end)
    elif args.resume: command_resume()
    elif args.show_status: command_show_status()
    elif args.reset: command_reset()
    else: print("請指定有效參數，使用 --help 查看說明")
    if task_name not in ["show-status", "reset"]:
        verify_consistency()
    end_time = datetime.datetime.now()
    log_summary_report(start_time, end_time, task_name)
    if LOG_FILE:
        print(f"\n日誌檔案：{os.path.abspath(LOG_FILE)}")

if __name__ == "__main__":
    main()
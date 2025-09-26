#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PostgreSQL → MongoDB 活動資料遷移工具（生产环境最终修正版）
- 核心邏輯：以 PostgreSQL gif_event 表中 event_no LIKE 'HCC%' 的記錄為唯一權威來源（白名單）。
- 功能：
  - 支援全量同步(--full-sync)與增量同步(--incremental)，所有操作均受白名單過濾。
  - 智慧增量同步：精確繼承上次成功任务的资料断点，作為本次增量起點，確保無遺漏。
  - checkpoint.json 集中管理狀態，支援斷點續傳(--resume)，並可透過 --show-status 和 --reset 進行管理。
  - 資料一致性校驗邏輯與遷移邏輯完全對齊，並在失敗時自動輸出差異ID。
  - 詳細的日誌記錄與任務總結報告。
"""

import os
import sys
import json
import logging
import datetime
import argparse
from typing import Set
from collections import defaultdict

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
    "host": os.getenv("PG_HOST"), "port": int(os.getenv("PG_PORT", 5432)),
    "user": os.getenv("PG_USER"), "password": os.getenv("PG_PASSWORD"),
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
pg_cursor = None
mongo_client = None
mongo_dbs = {}
cp_data = {}
VALID_EVENT_NOS: Set[str] = set()

MIGRATION_STATS = {
    "tables": {}, "total_pg_records": 0, "total_inserted": 0,
    "total_updated": 0, "verification_results": []
}

# 定義多表設定
TABLES_CONFIG = {
    "events": {
        "pg_table": "gif_event", "add_date_field": "add_date", "mod_date_field": "mod_date",
        "id_field": "event_no", "mongo_collection": "events", "verification_mode": "primary_count",
        "link_field": "event_no"
    },
    "hcc_events": {
        "pg_table": "gif_hcc_event", "add_date_field": "add_date", "mod_date_field": "mod_date",
        "id_field": "event_no", "mongo_collection": "events", "verification_mode": "merge_source",
        "link_field": "event_no"
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
    global pg_conn, pg_cursor, mongo_client, mongo_dbs
    try:
        pg_conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        pg_cursor = pg_conn.cursor(cursor_factory=DictCursor)
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
    pg_cursor.execute(sql)
    results = pg_cursor.fetchall()
    VALID_EVENT_NOS = {row[0] for row in results}
    if not VALID_EVENT_NOS:
        logger.warning("警告：在 gif_event 表中未找到任何以 'HCC' 開頭的活動，遷移將處理 0 筆主活動資料。")
    else:
        logger.info(f"白名單載入完畢，共 {len(VALID_EVENT_NOS)} 個有效的 event_no。")

def fetch_batch(table_key, end_time, last_checkpoint_time, last_checkpoint_id, batch_size=100):
    conf = TABLES_CONFIG[table_key]
    pg_table_alias = "t"
    join_clause = ""
    select_clause = f"SELECT {pg_table_alias}.* FROM {SCHEMA}.{conf['pg_table']} {pg_table_alias}"
    
    if table_key == "coupon_burui":
        join_clause = f"JOIN {SCHEMA}.gif_event_coupon_setting s ON {pg_table_alias}.coupon_setting_no = s.coupon_setting_no"
        select_clause = f"SELECT {pg_table_alias}.*, s.event_no FROM {SCHEMA}.{conf['pg_table']} {pg_table_alias}"

    effective_timestamp_field = f"COALESCE({pg_table_alias}.{conf['mod_date_field']}, {pg_table_alias}.{conf['add_date_field']}, '1970-01-01')"
    id_field_with_alias = f"{pg_table_alias}.{conf['id_field']}"
    
    order_by_clause = f"ORDER BY {effective_timestamp_field}, {id_field_with_alias}"

    params = []
    where_clauses = []

    link_field = conf.get("link_field", "event_no")
    event_no_field_in_query = "s.event_no" if table_key == "coupon_burui" else f"{pg_table_alias}.{link_field}"
    
    if VALID_EVENT_NOS:
        where_clauses.append(f"{event_no_field_in_query} = ANY(%s)")
        params.append(list(VALID_EVENT_NOS))
    else:
        where_clauses.append("1=0") 
    
    where_clauses.append(f"{effective_timestamp_field} < %s")
    params.append(end_time)
    
    if last_checkpoint_time and last_checkpoint_id is not None:
        where_clauses.append(f"({effective_timestamp_field} > %s OR ({effective_timestamp_field} = %s AND {id_field_with_alias} > %s))")
        params.extend([last_checkpoint_time, last_checkpoint_time, last_checkpoint_id])

    full_sql = f"{select_clause} {join_clause} WHERE {' AND '.join(where_clauses)} {order_by_clause} LIMIT %s"
    params.append(batch_size)
    
    # logger.info(f"[SQL-{table_key}] {full_sql.replace(chr(10), ' ')}")
    pg_cursor.execute(full_sql, params)
    return pg_cursor.fetchall()

def normalize_value(v):
    if isinstance(v, Decimal): return Decimal128(v)
    if isinstance(v, datetime.datetime): return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v
    if isinstance(v, date): return datetime.datetime.combine(v, datetime.time.min, tzinfo=timezone.utc)
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
            if not event_no: continue
            filter_cond = {"eventNo": event_no}
            if array_field == "usingBranchIds": value_to_add = doc.get("branch")
            elif array_field == "memberTypes": value_to_add = doc.get("memberType")
            else: value_to_add = doc
            if value_to_add:
                requests.append(UpdateOne(filter_cond, {"$addToSet": {array_field: value_to_add}}, upsert=False))
        
        elif table_key == "hcc_events":
            event_no = doc.get("eventNo")
            if not event_no: continue
            doc.pop("eventNo", None)
            doc.pop("addDate", None)
            update_op = {"$set": doc}
            requests.append(UpdateOne({"eventNo": event_no}, update_op, upsert=False))

        else: # 主 collection (events, attendees)
            id_field_camel = snake_to_camel(conf["id_field"])
            if table_key == "attendees":
                filter_cond = {"eventNo": doc.get("eventNo"), "appId": doc.get("appId")}
            else:
                if id_field_camel not in doc: continue
                filter_cond = {id_field_camel: doc[id_field_camel]}
            requests.append(UpdateOne(filter_cond, {"$set": doc}, upsert=True))
    
    if not requests: return None
    
    return mongo_dbs[table_key].bulk_write(requests, ordered=False)

def update_checkpoint_window(table_key, window_to_update, new_checkpoint_time, new_checkpoint_id, processed_count_delta, status):
    global cp_data
    cp_table = cp_data.setdefault(table_key, {})
    
    for w in cp_table.setdefault("base_windows", []):
        if w["start"] == window_to_update["start"] and w["end"] == window_to_update["end"] and w.get("mode") == window_to_update.get("mode"):
            w.update({
                "last_checkpoint_time": new_checkpoint_time,
                "last_checkpoint_id": str(new_checkpoint_id) if new_checkpoint_id is not None else None,
                "processed_count": w.get("processed_count", 0) + processed_count_delta,
                "owner": os.uname().nodename.split('.')[0],
                "status": status
            })
            if status == "completed": 
                w["finish_exec_time"] = datetime.datetime.now(timezone.utc).isoformat()
            break
    save_checkpoint(cp_data)

def migrate_table_window(table_key, window):
    conf = TABLES_CONFIG[table_key]
    
    last_t_iso = window.get("last_checkpoint_time")
    last_id = window.get("last_checkpoint_id")
    
    processed_since_resume = 0
    consecutive_errors = 0
    
    update_checkpoint_window(table_key, window, last_t_iso, last_id, 0, status="in_progress")
    
    while True:
        try:
            rows = fetch_batch(table_key, window["end"], last_t_iso, last_id, BATCH_SIZE)
            
            if not rows:
                update_checkpoint_window(table_key, window, last_t_iso, last_id, 0, status="completed")
                total_processed = window.get("processed_count", 0)
                logger.info(f"{table_key} 視窗 {window['start']}~{window['end']} 同步完成，本次執行處理了 {processed_since_resume} 筆，累計處理 {total_processed} 筆")
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

            last_row = rows[-1]
            time_key_value = last_row.get(conf["mod_date_field"]) or last_row.get(conf["add_date_field"])
            id_key_value = last_row.get(conf["id_field"])

            if time_key_value is None or id_key_value is None:
                raise ValueError(f"無法獲取有效斷點值，數據: {last_row}")

            last_t_iso = normalize_value(time_key_value).isoformat()
            last_id = id_key_value
            
            update_checkpoint_window(table_key, window, last_t_iso, last_id, processed_this_batch, status="in_progress")
            processed_since_resume += processed_this_batch
            logger.info(f"{table_key} 批次同步成功: records={len(rows)}, last_time={last_t_iso}, last_id={last_id}")
            consecutive_errors = 0
            
        except psycopg2.Error as pg_err:
            consecutive_errors += 1
            logger.error(f"PostgreSQL 發生錯誤，正在回滾事務: {pg_err}")
            pg_conn.rollback()
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"{table_key} 視窗同步批次異常，第 {consecutive_errors} 次錯誤: {e}", exc_info=True)
            if consecutive_errors >= 5:
                update_checkpoint_window(table_key, window, last_t_iso, last_id, 0, status="pending")
                logger.error(f"{table_key} 視窗因連續錯誤而終止，狀態已改為 pending。")
                raise e
            
def convert_decimal_for_json(obj):
    if isinstance(obj, dict): return {k: convert_decimal_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list): return [convert_decimal_for_json(elem) for elem in obj]
    elif isinstance(obj, Decimal): return str(obj)
    else: return obj

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            try: return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"{CHECKPOINT_FILE} 格式錯誤，將建立新檔案。")
                return {}
    return {}

def save_checkpoint(data):
    safe_data = convert_decimal_for_json(data)
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(safe_data, f, ensure_ascii=False, indent=2)

def log_discrepancy_details(key, conf):
    logger.warning(f"--- [ {key} ] 遺漏資料詳細清單 (最多顯示 20 筆) ---")
    pg_ids = set()
    mongo_ids = set()
    try:
        pg_table = f"{SCHEMA}.{conf['pg_table']}"
        link_field = conf.get("link_field", "event_no")
        with pg_conn.cursor() as cur:
            if key == "coupon_burui":
                sql = f"SELECT DISTINCT s.event_no, t.branch FROM {pg_table} t JOIN {SCHEMA}.gif_event_coupon_setting s ON t.coupon_setting_no = s.coupon_setting_no WHERE s.event_no = ANY(%s) AND t.branch IS NOT NULL AND t.branch != ''"
                cur.execute(sql, (list(VALID_EVENT_NOS),))
                pg_ids = {f"{row[0]}:{row[1]}" for row in cur.fetchall()}
            else:
                pk_field = conf['id_field']
                sql = f"SELECT {pk_field} FROM {pg_table} WHERE {link_field} = ANY(%s);"
                cur.execute(sql, (list(VALID_EVENT_NOS),))
                pg_ids = {str(row[0]) for row in cur.fetchall()}

        mongo_collection = mongo_dbs[key]
        if conf.get("verification_mode") == "embed_array":
            arr_field = conf["embed_array_field"]
            key_field = "branch" if key == "coupon_burui" else "memberType"
            pipeline = [
                {"$match": {"eventNo": {"$in": list(VALID_EVENT_NOS)}}},
                {"$unwind": f"${arr_field}"}
            ]
            if key == "coupon_burui":
                 pipeline.append({"$project": {"_id": 0, "combinedKey": {"$concat": ["$eventNo", ":", f"${arr_field}"]}}})
            else: 
                 pipeline.append({"$project": {"_id": 0, "combinedKey": {"$concat": ["$eventNo", ":", f"${arr_field}.{key_field}"]}}})
            mongo_results = list(mongo_collection.aggregate(pipeline))
            mongo_ids = {item['combinedKey'] for item in mongo_results}
        else:
            pk_field_camel = snake_to_camel(conf['id_field'])
            cursor = mongo_collection.find({"eventNo": {"$in": list(VALID_EVENT_NOS)}}, {pk_field_camel: 1, "_id": 0})
            mongo_ids = {str(doc[pk_field_camel]) for doc in cursor if pk_field_camel in doc}

        missing_from_mongo = sorted(list(pg_ids - mongo_ids))
        
        if missing_from_mongo:
            logger.warning(f"在 MongoDB 中遺漏了 {len(missing_from_mongo)} 筆資料。")
            for i, missing_id in enumerate(missing_from_mongo[:20]):
                logger.warning(f"  - 遺漏的 ID/組合鍵: {missing_id}")
        else:
            logger.warning("計數不一致，但未找到具體遺漏的 ID，可能是 MongoDB 中存在重複或額外的資料。")
    except Exception as e:
        logger.error(f"執行差異分析時出錯: {e}", exc_info=True)
    logger.warning("----------------------------------------------------")

def verify_consistency():
    global MIGRATION_STATS
    all_ok = True
    logger.info("開始執行資料一致性校驗 (基於白名單)...")
    if not VALID_EVENT_NOS:
        logger.warning("白名單為空，跳過資料一致性校驗。")
        return True
    
    with pg_conn.cursor() as cur:
        for key, conf in TABLES_CONFIG.items():
            mode = conf.get("verification_mode")
            pg_table = f"{SCHEMA}.{conf['pg_table']}"
            link_field = conf.get("link_field", "event_no")
            pg_count = 0
            try:
                if key == "coupon_burui":
                    sql = f"SELECT COUNT(*) FROM (SELECT DISTINCT s.event_no, t.branch FROM {pg_table} t JOIN {SCHEMA}.gif_event_coupon_setting s ON t.coupon_setting_no = s.coupon_setting_no WHERE s.event_no = ANY(%s) AND t.branch IS NOT NULL AND t.branch != '') AS distinct_rows;"
                else:
                    sql = f"SELECT COUNT(*) FROM {pg_table} WHERE {link_field} = ANY(%s);"
                
                cur.execute(sql, (list(VALID_EVENT_NOS),))
                pg_count = cur.fetchone()[0]

                result_str = ""
                is_consistent = False

                if mode == "merge_source":
                    result_str = f"資料合併來源 {conf['pg_table']}: PG={pg_count} (白名單內)"
                    is_consistent = True
                else:
                    mongo_collection = mongo_dbs[key]
                    mongo_count = 0
                    if mode == "embed_array":
                        arr_field = conf["embed_array_field"]
                        pipeline = [{"$match": {"eventNo": {"$in": list(VALID_EVENT_NOS)}}}, {"$project": {"count": {"$size": {"$ifNull": [f"${arr_field}", []]}}}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
                        mongo_result = list(mongo_collection.aggregate(pipeline))
                        mongo_count = mongo_result[0]['total'] if mongo_result else 0
                        result_str = f"嵌入陣列 {conf['pg_table']}: PG(理論)={pg_count}, MG(實際)={mongo_count}"
                    else: # primary_count
                        mongo_count = mongo_collection.count_documents({"eventNo": {"$in": list(VALID_EVENT_NOS)}})
                        result_str = f"主計數 {conf['pg_table']}: PG={pg_count}, MG={mongo_count}"
                    
                    is_consistent = (pg_count == mongo_count)
                
                if is_consistent:
                    MIGRATION_STATS["verification_results"].append(f"✅ PASS: {result_str}")
                    logger.info(f"✅ 校驗通過: {result_str}")
                else:
                    MIGRATION_STATS["verification_results"].append(f"❌ FAIL: {result_str}"); all_ok = False
                    logger.warning(f"❌ 校驗失敗: {result_str}")
                    if mode != "merge_source":
                        log_discrepancy_details(key, conf)
            
            except Exception as e:
                logger.error(f"校驗表格 {key} 時出錯: {e}", exc_info=True); all_ok = False
    logger.info("資料一致性校驗完畢。")
    return all_ok

def command_full_sync():
    global cp_data
    logger.warning("執行全量同步 (--full-sync) 將會清空現有的 checkpoint 進度。")
    cp_data = {}
    end_time = "9999-12-31T23:59:59Z"
    for table_key in TABLES_CONFIG.keys():
        window = {
            "start": "1970-01-01T00:00:00Z", 
            "end": end_time, 
            "status": "pending", 
            "mode": "full",
            "last_checkpoint_time": "1970-01-01T00:00:00Z",
            "last_checkpoint_id": None,
            "processed_count": 0
        }
        cp_data.setdefault(table_key, {}).setdefault("base_windows", []).append(window)

    save_checkpoint(cp_data)
    logger.info(f"已為所有表建立/重置全量同步視窗。")
    
    execution_order = ["events", "hcc_events", "attendees", "coupon_burui", "member_types"]
    for table_key in execution_order:
        logger.info(f"--- 開始處理表: {table_key} ---")
        window = cp_data[table_key]["base_windows"][0]
        migrate_table_window(table_key, window)

def get_latest_checkpoint_for_table(table_key):
    """
    從 checkpoint 檔案中，只查找【指定表】的已完成任務，並返回其最新的斷點。
    """
    latest_checkpoint_time = "1970-01-01T00:00:00Z"
    latest_checkpoint_id = None
    
    current_cp_data = load_checkpoint()
    
    table_windows = current_cp_data.get(table_key, {}).get("base_windows", [])
    completed_windows = [w for w in table_windows if w.get("status") == "completed"]

    if completed_windows:
        latest_window = max(completed_windows, key=lambda w: w.get("finish_exec_time", "1970-01-01T00:00:00Z"))
        latest_checkpoint_time = latest_window.get("last_checkpoint_time", "1970-01-01T00:00:00Z")
        latest_checkpoint_id = latest_window.get("last_checkpoint_id")
    
    return latest_checkpoint_time, latest_checkpoint_id

def command_incremental():
    global cp_data
    end_time_utc = datetime.datetime.now(timezone.utc)
    execution_order = ["events", "hcc_events", "attendees", "coupon_burui", "member_types"]

    # *** 最終核心邏輯修正 ***
    # 廢除「全局斷點」，改為在循環中為【每個表】獨立查找其自身的最新斷點。
    # 這確保了每個表的進度是獨立維護的，從根本上避免了數據類型污染問題。
    for table_key in execution_order:
        logger.info(f"--- 正在為表 '{table_key}' 準備增量任務 ---")
        
        # 1. 為當前表獨立查找其最新的斷點
        latest_checkpoint_time, latest_checkpoint_id = get_latest_checkpoint_for_table(table_key)
        
        # 2. 重新從磁碟加載 checkpoint 檔案，以防被其他進程修改
        cp_data = load_checkpoint()
        table_data = cp_data.setdefault(table_key, {})
        base_windows = table_data.setdefault("base_windows", [])
        
        # 3. 創建新的任務窗口，起點是【該表自己】的最新斷點
        new_window = {
            "start": latest_checkpoint_time, 
            "end": end_time_utc.isoformat(), 
            "status": "pending", 
            "mode": "incremental", 
            "last_checkpoint_time": latest_checkpoint_time, 
            "last_checkpoint_id": latest_checkpoint_id,
            "processed_count": 0
        }
        base_windows.append(new_window)
        logger.info(f"新增 {table_key} 增量同步視窗：{new_window['start']} ~ {new_window['end']} (繼承【自身】斷點)")
        save_checkpoint(cp_data)
        
        # 4. 執行同步
        migrate_table_window(table_key, new_window)

def command_resume():
    logger.info("開始執行斷點恢復任務...")
    execution_order = ["events", "hcc_events", "attendees", "coupon_burui", "member_types"]
    for table_key in execution_order:
        current_cp_data = load_checkpoint()
        if table_key not in current_cp_data: continue
        
        for w in current_cp_data[table_key].get("base_windows", []):
            if w.get("status") in ("pending", "in_progress"):
                logger.info(f"恢復處理 '{table_key}' 的 {w.get('mode', 'full')} 模式視窗: {w['start']} -> {w['end']}")
                global cp_data
                cp_data = current_cp_data
                migrate_table_window(table_key, w)

def command_show_status():
    """顯示當前 checkpoint 狀態"""
    logger.info("="*25 + " [ Checkpoint 狀態報告 ] " + "="*25)
    if not cp_data:
        logger.info("Checkpoint 檔案為空或不存在。")
        return

    summary = defaultdict(lambda: {"completed": 0, "in_progress": 0, "pending": 0})
    
    logger.info("[ 各表獨立最新斷點 (Latest Checkpoint per Table) ]")
    for table_key in TABLES_CONFIG.keys():
        latest_time, latest_id = get_latest_checkpoint_for_table(table_key)
        logger.info(f"  - {table_key}:")
        logger.info(f"    > 最新斷點時間: {latest_time or 'N/A'}")
        logger.info(f"    > 最新斷點ID:    {latest_id or 'N/A'}")

        table_data = cp_data.get(table_key, {})
        for w in table_data.get("base_windows", []):
            status = w.get("status", "unknown")
            summary[table_key][status] += 1
    
    logger.info("")
    logger.info("[ 各表任務窗口狀態 (Window Status per Table) ]")
    for table, statuses in summary.items():
        if statuses:
            logger.info(f"  - {table}:")
            for status, count in statuses.items():
                logger.info(f"    > {status}: {count} 個窗口")
    logger.info("="*67)


def command_reset():
    """重置 checkpoint 檔案"""
    logger.warning(f"警告：這個操作將會刪除 checkpoint 檔案 '{CHECKPOINT_FILE}'。")
    user_input = input("請輸入 'reset' 來確認執行此操作: ")
    if user_input == 'reset':
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            logger.info(f"Checkpoint 檔案 '{CHECKPOINT_FILE}' 已被成功刪除。")
        else:
            logger.info("Checkpoint 檔案不存在，無需操作。")
    else:
        logger.info("操作已取消。")

def parse_args():
    parser = argparse.ArgumentParser(description="PostgreSQL → MongoDB 活動資料遷移工具 (Production Ready)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full-sync", action="store_true", help="執行一次全新的全量同步，會清空進度。")
    group.add_argument("--incremental", action="store_true", help="從上次的斷點開始執行增量同步。")
    group.add_argument("--resume", action="store_true", help="恢復處理狀態為 'pending' 或 'in_progress' 的任務。")
    group.add_argument("--show-status", action="store_true", help="顯示當前 checkpoint 的狀態摘要。")
    group.add_argument("--reset", action="store_true", help="重置(刪除) checkpoint 檔案，以便從頭開始。")
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
        "=" * 60, "========== [ 遷移任務總結報告 (Migration Summary) ] ==========", "=" * 60, "",
        "[ 總體概覽 (Overall) ]",
        f"  - 任務狀態:               {status_str}",
        f"  - 任務名稱:               {task_name}",
        f"  - 開始時間:               {start_time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"  - 結束時間:               {end_time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"  - 總耗時:                   {duration_str}",
    ]
    if task_name not in ["show-status", "reset"]:
        report_lines.extend([
            f"  - 總處理 PostgreSQL 紀錄數: {total_records:,} 筆",
            f"  - 平均吞吐量:               {throughput:.1f} 筆/秒", ""
        ])
        report_lines.append("[ 分表處理詳情 (Per-Table Details) ]")
        if not MIGRATION_STATS.get("tables"):
            report_lines.append("  - 本次任務未處理任何資料。")
        else:
            for key, stats in MIGRATION_STATS["tables"].items():
                pg_table_name = TABLES_CONFIG[key]['pg_table']
                report_lines.append(f"  - {pg_table_name} ({key}):")
                report_lines.append(f"    > PG 來源: {stats.get('pg_records', 0):,} | Mongo 新增: {stats.get('inserted', 0):,} | Mongo 更新: {stats.get('updated', 0):,}")
        report_lines.extend(["", "[ 資料一致性校驗 (Data Consistency Verification) ]"])
        if not verification_results:
            report_lines.append("  - 本次任務未執行資料校驗。")
        else:
            for result in verification_results:
                report_lines.append(f"  - {result}")

    report_lines.extend(["", "=" * 25 + " [ 報告結束 ] " + "=" * 25])
    for line in report_lines: logger.info(line)

def main():
    global cp_data, args
    start_time = datetime.datetime.now()
    args = parse_args()
    task_name_map = {"full_sync": "full-sync", "incremental": "incremental", "resume": "resume", "show_status": "show-status", "reset": "reset"}
    task_name = next((v for k, v in task_name_map.items() if getattr(args, k, False)), "unknown")
    init_logger(task_name=task_name)
    logger.info(f"===== 開始執行遷移任務: {task_name} =====")
    
    try:
        if args.show_status or args.reset:
            cp_data = load_checkpoint()
            if args.show_status: command_show_status()
            elif args.reset: command_reset()
            return

        init_db_conn()
        cp_data = load_checkpoint()
        load_valid_event_nos()

        if args.full_sync: command_full_sync()
        elif args.incremental: command_incremental()
        elif args.resume: command_resume()
        
        if task_name in ["full-sync", "incremental", "resume"]:
            verify_consistency()

    except Exception as e:
        logger.error(f"任務 '{task_name}' 執行期間發生未處理的致命錯誤: {e}", exc_info=True)
        MIGRATION_STATS["verification_results"].append(f"❌ FAIL: 任務因異常而終止 - {e}")
        sys.exit(1)
    finally:
        if pg_conn: pg_conn.close()
        if mongo_client: mongo_client.close()
        end_time = datetime.datetime.now()
        
        if not (getattr(args, 'show_status', False) or getattr(args, 'reset', False)):
            log_summary_report(start_time, end_time, task_name)
        logger.info(f"===== 遷移任務結束: {task_name} =====")

if __name__ == "__main__":
    main()
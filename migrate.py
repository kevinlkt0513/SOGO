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

# 定義多表設定：PG資料表 | 全量時間欄位(add_date) | 斷點時間欄位(mod_date) | 主鍵欄位 | MongoDB 集合名稱 | 嵌入陣列欄位(可選)
TABLES_CONFIG = {
    "events": {
        "pg_table": "gif_event",
        "add_date_field": "add_date",
        "mod_date_field": "mod_date",
        "id_field": "event_no",
        "mongo_collection": "events",
    },
    "hcc_events": {
        "pg_table": "gif_hcc_event",
        "add_date_field": "add_date",
        "mod_date_field": "mod_date",
        "id_field": "event_no",
        "mongo_collection": "events",
    },
    "attendees": {
        "pg_table": "gif_hcc_event_attendee",
        "add_date_field": "add_date",
        "mod_date_field": "mod_date",
        "id_field": "id",
        "mongo_collection": "event_attendees",
    },
    "coupon_burui": {
        "pg_table": "gif_event_coupon_burui",
        "add_date_field": "add_date",
        "mod_date_field": "mod_date",
        "id_field": "id",
        "mongo_collection": "events",
        "embed_array_field": "usingBranchIds",
    },
    "coupon_setting": {
        "pg_table": "gif_event_coupon_setting",
        "add_date_field": "add_date",
        "mod_date_field": "mod_date",
        "id_field": "coupon_setting_no",
        "mongo_collection": "events",
        "embed_array_field": "prizeCouponJson",
    },
    "member_types": {
        "pg_table": "gif_hcc_event_member_type",
        "add_date_field": "add_date",
        "mod_date_field": "mod_date",
        "id_field": "id",
        "mongo_collection": "events",
        "embed_array_field": "memberTypes",
    },
}

# ===== 全域變數：儲存有效的 event_no 白名單 =====
VALID_EVENT_NOS = set()

def load_valid_event_nos(conn):
    """
    從主表 gif_hcc_event 載入所有有效的 event_no，作為遷移的白名單。
    """
    global VALID_EVENT_NOS
    logger.info("正在從主表 gif_hcc_event 載入有效的 event_no 白名單...")
    
    # 確保只執行一次
    if VALID_EVENT_NOS:
        logger.info("白名單已載入，跳過。")
        return

    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT event_no FROM {SCHEMA}.gif_hcc_event")
            rows = cur.fetchall()
            VALID_EVENT_NOS = {row['event_no'] for row in rows}
            logger.info(f"成功載入 {len(VALID_EVENT_NOS)} 個有效的 event_no 到白名單。")
    except Exception as e:
        logger.error(f"載入 event_no 白名單失敗: {e}")
        sys.exit(1)

def init_logger(task_name="general"):
    """
    初始化日誌系統，根據任務名稱和時間戳生成易於管理的日誌檔名。
    新格式: {YYYY-MM-DD}_{HHMMSS}_{任務名稱}_{簡短主機名}_{PID}.log
    """
    global LOG_FILE, logger
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # 獲取檔名組件
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    short_hostname = os.uname().nodename.split('.')[0]
    pid = os.getpid()
    
    # 組成新檔名
    log_filename = f"{timestamp}_{task_name}_{short_hostname}_{pid}.log"
    LOG_FILE = os.path.join(LOG_DIR, log_filename)
    
    # 移除舊的 handler，避免日誌重複輸出
    if logging.getLogger().hasHandlers():
        logging.getLogger().handlers.clear()
        
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
    )
    logger = logging.getLogger()

pg_conn = None
pg_cursor = None
mongo_client = None
mongo_dbs = {}

def ensure_mongodb_indexes(mdb):
    """
    確保 MongoDB 中存在必要的索引以提高 upsert 效能
    """
    logger.info("開始檢查並建立 MongoDB 索引...")
    try:
        # 1. events 集合的索引
        events_collection = mdb["events"]
        event_no_index = IndexModel([("event_no", 1)], name="idx_event_no")
        events_collection.create_indexes([event_no_index])
        logger.info(f"成功檢查/建立 events 集合的索引: idx_event_no")

        # 2. event_attendees 集合的索引 (最關鍵)
        attendees_collection = mdb["event_attendees"]
        attendee_compound_index = IndexModel([("eventNo", 1), ("appId", 1)], name="idx_eventNo_appId")
        attendees_collection.create_indexes([attendee_compound_index])
        logger.info(f"成功檢查/建立 event_attendees 集合的索引: idx_eventNo_appId")
        
        logger.info("MongoDB 索引檢查完畢。")
    except Exception as e:
        logger.error(f"建立 MongoDB 索引時發生錯誤: {e}")
        # 根據您的策略，這裡可以選擇 sys.exit(1) 或僅是警告

def ensure_postgresql_indexes(conn):
    """
    確保 PostgreSQL 中存在推薦的索引
    """
    logger.info("開始檢查並建立 PostgreSQL 索引...")
    # 使用 IF NOT EXISTS 避免重複建立時報錯 (需要 PG 9.5+)
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
                conn.rollback() # 發生錯誤時回滾
                return # 中斷後續操作
        conn.commit() # 所有索引指令成功後提交
    logger.info("PostgreSQL 索引檢查完畢。")

def init_db_conn():
    """
    初始化 PostgreSQL 與 MongoDB 連接，並確保索引存在
    """
    global pg_conn, pg_cursor, mongo_client, mongo_dbs
    try:
        pg_conn = psycopg2.connect(**POSTGRESQL_CONFIG, cursor_factory=DictCursor)
        pg_cursor = pg_conn.cursor()
        # 建立/檢查 PostgreSQL 索引
        ensure_postgresql_indexes(pg_conn)
    except Exception as e:
        logger.error(f"無法連接 PostgreSQL 或建立索引：{e}")
        sys.exit(1)
        
    try:
        mongo_client = MongoClient(MONGO_URI)
        mdb = mongo_client["gift"]  # 請根據實際庫名調整
        
        # 建立/檢查 MongoDB 索引
        ensure_mongodb_indexes(mdb)
        
        for k, v in TABLES_CONFIG.items():
            mongo_dbs[k] = mdb[v["mongo_collection"]]

    except Exception as e:
        logger.error(f"無法連接 MongoDB 或建立索引：{e}")
        sys.exit(1)

def fetch_batch(table_key, start_time, end_time,
                last_checkpoint_time=None, last_checkpoint_id=None,
                batch_size=100, mode="incremental"):
    """
    批次擷取指定表於時間視窗內之資料
    - ★★★ 新邏輯：所有查詢都必須過濾 event_no，確保其存在於 VALID_EVENT_NOS 白名單中 ★★★
    """
    conf = TABLES_CONFIG[table_key]
    
    valid_event_nos_list = list(VALID_EVENT_NOS)
    
    event_no_column_alias = "t.event_no"
    join_clause = ""
    
    if table_key == "coupon_burui":
        join_clause = f"JOIN {SCHEMA}.gif_event_coupon_setting s ON t.coupon_setting_no = s.coupon_setting_no"
        event_no_column_alias = "s.event_no"
    
    # --- 修正點：明確指定主表 't' 的欄位 ---
    date_field_str = conf['add_date_field']
    mod_date_field_str = conf['mod_date_field']
    id_field_str = conf['id_field']

    date_field = f"t.{date_field_str}" if mode == "full" else f"COALESCE(t.{mod_date_field_str}, t.{date_field_str})"
    order_by_clause = f"ORDER BY {date_field}, t.{id_field_str}"
    
    base_sql = f"SELECT t.* FROM {SCHEMA}.{conf['pg_table']} t {join_clause}"
    
    if table_key == "coupon_burui":
        base_sql = f"SELECT t.*, s.event_no FROM {SCHEMA}.{conf['pg_table']} t {join_clause}"
    
    params = []
    where_clauses = []

    if last_checkpoint_time is None or last_checkpoint_id is None:
        where_clauses.append(f"{date_field} >= %s AND {date_field} < %s")
        params.extend([start_time, end_time])
    else:
        # --- 修正點：明確指定 id 欄位來自 't' 表 ---
        id_field_with_alias = f"t.{id_field_str}"
        where_clauses.append(f"({date_field} > %s OR ({date_field} = %s AND {id_field_with_alias} > %s))")
        params.extend([last_checkpoint_time, last_checkpoint_time, last_checkpoint_id])
        where_clauses.append(f"{date_field} >= %s AND {date_field} < %s")
        params.extend([start_time, end_time])
        
    where_clauses.append(f"{event_no_column_alias} = ANY(%s)")
    params.append(valid_event_nos_list)

    sql = f"{base_sql} WHERE {' AND '.join(where_clauses)} {order_by_clause} LIMIT %s"
    params.append(batch_size)

    # 為了日誌清晰，從 params 複製一份來顯示，避免修改原始 params
    log_params = list(params)
    if valid_event_nos_list:
        log_params[log_params.index(valid_event_nos_list)] = f"[...{len(valid_event_nos_list)} event_nos...]"
        
    logger.info(f"[SQL-{table_key}] {sql.replace(chr(10), ' ')} params {log_params}")
    pg_cursor.execute(sql, params)
    return pg_cursor.fetchall()

def normalize_value(v):
    """
    將 Decimal, date, datetime 等不可直存 MongoDB 的類型轉換為可序列化原生類型
    """
    # 處理高精度十進位
    if isinstance(v, Decimal):
        try:
            return Decimal128(v)
        except (TypeError, ValueError, OverflowError):
            return float(v)
    # 處理 datetime.datetime
    if isinstance(v, datetime.datetime):
        return v
    # 處理 date
    if isinstance(v, date):
        return datetime.datetime.combine(v, datetime.datetime.min.time())
    # 其他類型直接返回
    return v

def normalize_row(row):
    """
    將 DictCursor 返回的 row 中所有值做 normalize
    """
    return {k: normalize_value(v) for k, v in row.items()}

def upsert_batch(table_key, rows):
    """
    批次 upsert 資料至 MongoDB，支援嵌入陣列欄位與單文件 upsert
    """
    conf = TABLES_CONFIG[table_key]
    requests = []
    for row in rows:
        # 先調用 normalize_row 轉換所有欄位
        doc = normalize_row(row)

        if "embed_array_field" in conf:
            # 嵌入陣列更新：使用 $addToSet 避免重複
            filter_cond = {"event_no": doc["event_no"]}
            requests.append(UpdateOne(
                filter_cond,
                {"$addToSet": {conf["embed_array_field"]: doc}},
                upsert=True
            ))
        else:
            # 單文件 upsert
            if table_key == "attendees":
                filter_cond = {"eventNo": doc.get("event_no"), "appId": doc.get("app_id")}
            else:
                filter_cond = {conf["id_field"]: doc[conf["id_field"]]}
            requests.append(UpdateOne(filter_cond, {"$set": doc}, upsert=True))

    if not requests:
        return 0
    result = mongo_dbs[table_key].bulk_write(requests, ordered=False)
    return result.upserted_count

def update_checkpoint(table_key, last_time, last_id, processed_count, window=None, status=None):
    """
    更新 checkpoint.json 中之斷點時間、ID 及視窗狀態
    """
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
            upsert_batch(table_key, rows)
            processed += len(rows)

            last_row = rows[-1]

            # === 关键修改：时间键 fallback ===
            if mode == "incremental":
                # 优先用 mod_date，没有就用 add_date
                time_key_value = last_row.get(conf["mod_date_field"]) or last_row.get(conf["add_date_field"])
            else:
                time_key_value = last_row.get(conf["add_date_field"])

            id_key_value = last_row.get(conf["id_field"])

            # 防御性检查
            if time_key_value is None or id_key_value is None:
                logger.error(
                    f"{table_key} 無法獲取有效的斷點值，時間: {time_key_value}, ID: {id_key_value}，最後一行數據: {last_row}"
                )
                # 不更新断点，继续下一批
                continue

            # 存储 ISO 格式
            last_t = time_key_value.isoformat()
            last_id = id_key_value

            update_checkpoint(
                table_key,
                last_t,
                last_id,
                processed,
                window,
                status="in_progress"
            )

            consecutive_errors = 0
            logger.info(
                f"{table_key} 同步批次成功，records={len(rows)}, "
                f"last_time={last_t}, last_id={last_id}"
            )

        except Exception as e:
            consecutive_errors += 1
            logger.error(
                f"{table_key} 視窗 {start}~{end} 同步批次異常，第 {consecutive_errors} 次錯誤: {e}"
            )
            if consecutive_errors >= 5:
                window["status"] = "pending"
                save_checkpoint(cp_data)
                logger.error(f"{table_key} 視窗多次失敗終止，狀態改為 pending")
                break

    return processed

def convert_decimal_for_json(obj):
    """
    遞歸轉換資料結構中所有 Decimal 為 float 或 str，避免 json.dump 失敗
    """
    if isinstance(obj, dict):
        return {k: convert_decimal_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal_for_json(elem) for elem in obj]
    elif isinstance(obj, Decimal):
        # 可選擇轉 float 或 str，根據場景決定精度需求
        return float(obj)
    else:
        return obj

def load_checkpoint():
    """
    載入或初始化 checkpoint.json
    """
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {table_key: {"base_windows": [], "correction_windows": [], "last_checkpoint_time": None, "last_checkpoint_id": None} for table_key in TABLES_CONFIG.keys()}

def save_checkpoint(data):
    """
    儲存 checkpoint.json，轉換 Decimal 類型，避免序列化異常
    """
    safe_data = convert_decimal_for_json(data)
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(safe_data, f, ensure_ascii=False, indent=2)

def verify_consistency():
    """
    對各表做全量一致性檢驗；
    ★★★ 新邏輯：PG 端計數時，必須過濾 event_no 白名單 ★★★
    """
    valid_event_nos_list = list(VALID_EVENT_NOS)

    for key, conf in TABLES_CONFIG.items():
        # PG 端計數 (加入白名單過濾)
        pg_count_sql = ""
        params = [valid_event_nos_list]
        
        # 根據不同表格結構，產生不同的計數 SQL
        if key == "coupon_burui":
            pg_count_sql = f"""
                SELECT COUNT(t.*) FROM {SCHEMA}.{conf['pg_table']} t
                JOIN {SCHEMA}.gif_event_coupon_setting s ON t.coupon_setting_no = s.coupon_setting_no
                WHERE s.event_no = ANY(%s)
            """
        elif key in ["events", "hcc_events", "attendees", "member_types", "coupon_setting"]:
            # 對於 gif_event，我們只計算那些 event_no 存在於主表 hcc_event 中的數量
            pg_count_sql = f"SELECT COUNT(*) FROM {SCHEMA}.{conf['pg_table']} WHERE event_no = ANY(%s)"
        else:
            logger.warning(f"一致性統計未針對 {key} 設定特別的 SQL，可能不準確。")
            continue

        pg_cursor.execute(pg_count_sql, params)
        pg_count = pg_cursor.fetchone()[0]

        # MG 端計數
        mongo_collection = mongo_dbs[key]
        if "embed_array_field" in conf:
            # 嵌入欄位的計數邏輯保持不變，因為寫入時已經過濾
            arr_field = conf["embed_array_field"]
            # 這裡的查詢也應該加上 event_no 的過濾
            mongo_count = sum(len(doc.get(arr_field, [])) for doc in mongo_dbs["events"].find(
                {"event_no": {"$in": valid_event_nos_list}},
                {arr_field: 1}
            ))
        else:
            # 對於 events 集合，其總數應該等於白名單的數量
            if conf["mongo_collection"] == "events":
                mongo_count = mongo_dbs[key].count_documents({"event_no": {"$in": valid_event_nos_list}})
            else:
                 mongo_count = mongo_dbs[key].count_documents({})


        logger.info(f"一致性統計 {conf['pg_table']}: PG={pg_count}, MG={mongo_count}")
        if pg_count != mongo_count:
            logger.warning(f"一致性不符 {conf['pg_table']} PG={pg_count} vs MG={mongo_count}，需重新同步")

def command_full_sync(end_time=None):
    """
    全量同步：從1970-01-01T00:00:00至 end_time 建立基線視窗並同步(add_date)
    """
    global cp_data
    if end_time is None:
        end_time = datetime.datetime.now().isoformat()
    for table_key in TABLES_CONFIG.keys():
        base_windows = cp_data.setdefault(table_key, {}).setdefault("base_windows", [])
        exist = any(w for w in base_windows if w["start"] == "1970-01-01T00:00:00" and w["end"] == end_time)
        if not exist:
            base_windows.append({
                "start": "1970-01-01T00:00:00",
                "end": end_time,
                "status": "pending",
                "mode": "full",  # <--- 修改點：明確記錄視窗模式
                "processed_count": 0,
                "last_checkpoint_time": None,
                "last_checkpoint_id": None,
                "last_mod_date": None,
                "owner": None,
                "start_exec_time": None,
                "finish_exec_time": None
            })
    save_checkpoint(cp_data)
    logger.info(f"新增所有表基線視窗: 1970-01-01 至 {end_time}")
    total = 0
    for table_key in TABLES_CONFIG.keys():
        for w in cp_data[table_key]["base_windows"]:
            if w["status"] in ("pending", "in_progress"):
                # <--- 這裡不需要改動，因為首次執行時模式就是正確的
                total += migrate_table_window(table_key, w, mode="full")
    print(f"全量同步完成，共同步 {total} 筆記錄")

def command_incremental(end_time=None):
    """
    增量同步：以上次斷點為起點至 end_time 同步(mod_date)
    """
    global cp_data
    if end_time is None:
        end_time = datetime.datetime.now().isoformat()
    for table_key in TABLES_CONFIG.keys():
        base_windows = cp_data.setdefault(table_key, {}).setdefault("base_windows", [])
        last_end = max((w["end"] for w in base_windows), default="1970-01-01T00:00:00")
        base_windows.append({
            "start": last_end,
            "end": end_time,
            "status": "pending",
            "mode": "incremental",  # <--- 修改點：明確記錄視窗模式
            "processed_count": 0,
            "last_checkpoint_time": None,
            "last_checkpoint_id": None,
            "last_mod_date": None,
            "owner": None,
            "start_exec_time": None,
            "finish_exec_time": None
        })
        logger.info(f"新增 {table_key} 增量同步視窗：{last_end} ~ {end_time}")
    save_checkpoint(cp_data)
    total = 0
    for table_key in TABLES_CONFIG.keys():
        for w in cp_data[table_key]["base_windows"]:
            if w["status"] in ("pending", "in_progress"):
                total += migrate_table_window(table_key, w, mode="incremental")
    print(f"增量同步完成，共同步 {total} 筆記錄")

def command_correction(table_key, start_time, end_time):
    """
    強制補寫：針對指定表與時間視窗重做同步(mod_date)
    """
    global cp_data
    corr = cp_data.setdefault(table_key, {}).setdefault("correction_windows", [])
    corr.append({
        "start": start_time,
        "end": end_time,
        "status": "pending",
        "mode": "incremental",  # <--- 修改點：明確記錄視窗模式
        "processed_count": 0,
        "last_checkpoint_time": None,
        "last_checkpoint_id": None,
        "last_mod_date": None,
        "owner": None,
        "start_exec_time": None,
        "finish_exec_time": None
    })
    save_checkpoint(cp_data)
    logger.info(f"新增 {table_key} 補寫同步視窗：{start_time} ~ {end_time}")
    total = 0
    for w in corr:
        if w["status"] in ("pending", "in_progress"):
            total += migrate_table_window(table_key, w, mode="incremental")
    print(f"{table_key} 補寫同步完成，共同步 {total} 筆記錄")

def command_resume():
    """
    斷點恢復：繼續所有未完成視窗之同步任務
    """
    global cp_data
    total = 0
    for table_key in TABLES_CONFIG.keys():
        for window_type in ("base_windows", "correction_windows"):
            for w in cp_data[table_key].get(window_type, []):
                if w["status"] in ("pending", "in_progress"):
                    # --- 從這裡開始是修改點 ---
                    # 1. 從視窗物件w中獲取其模式。
                    # 2. 使用 .get() 是為了相容舊的、沒有mode欄位的checkpoint檔案，預設其為 "incremental"。
                    mode_to_run = w.get("mode", "incremental")
                    logger.info(f"Resuming window for '{table_key}' in '{mode_to_run}' mode. Window: {w['start']} -> {w['end']}")
                    
                    # 3. 將獲取到的正確模式傳遞給遷移函式。
                    total += migrate_table_window(table_key, w, mode=mode_to_run)
                    # --- 修改點結束 ---
    print(f"斷點恢復完成，共同步 {total} 筆記錄")

def command_show_status():
    """
    顯示 checkpoint.json 目前狀態
    """
    global cp_data
    print(json.dumps(cp_data, ensure_ascii=False, indent=2))

def command_reset():
    """
    重置所有斷點，請慎用
    """
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
    """
    解析命令列參數
    """
    parser = argparse.ArgumentParser(description="PostgreSQL → MongoDB 活動資料遷移工具")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full-sync", action="store_true", help="全量同步")
    group.add_argument("--incremental", action="store_true", help="增量同步")
    group.add_argument("--correction", nargs=3, metavar=('TABLE', 'START', 'END'),
                       help="補寫同步 eg: --correction attendees 2025-08-01T00:00:00 2025-08-01T03:00:00")
    group.add_argument("--resume", action="store_true", help="斷點恢復")
    group.add_argument("--show-status", action="store_true", help="查看斷點狀態")
    group.add_argument("--reset", action="store_true", help="重置所有斷點")
    parser.add_argument("--end", type=str, default=None, help="截止時間 ISO格式或 now（增量/全量同步）")
    args = parser.parse_args()
    if args.end is None or (isinstance(args.end, str) and args.end.lower() == "now"):
        args.end = datetime.datetime.now().isoformat()
    return args

def main():
    global cp_data
    args = parse_args()

    # --- 核心修改點：根據參數確定任務名稱 ---
    task_name = "unknown"
    if args.full_sync:
        task_name = "full-sync"
    elif args.incremental:
        task_name = "incremental"
    elif args.correction:
        task_name = f"correction-{args.correction[0]}" # 例如 correction-attendees
    elif args.resume:
        task_name = "resume"
    elif args.show_status:
        task_name = "show-status"
    elif args.reset:
        task_name = "reset"

    # 使用新的任務名稱來初始化日誌
    init_logger(task_name=task_name)
    
    # 後續邏輯保持不變
    init_db_conn()
    load_valid_event_nos(pg_conn)
    cp_data = load_checkpoint()

    if args.full_sync:
        command_full_sync(end_time=args.end)
    elif args.incremental:
        command_incremental(end_time=args.end)
    elif args.correction:
        table_key, start, end = args.correction
        if table_key not in TABLES_CONFIG:
            print(f"未知的 table: {table_key}")
            sys.exit(1)
        command_correction(table_key, start, end)
    elif args.resume:
        command_resume()
    elif args.show_status:
        command_show_status()
    elif args.reset:
        command_reset()
    else:
        print("請指定有效參數，使用 --help 查看說明")

    # 對於不會進行資料比對的任務，可以跳過驗證
    if task_name not in ["show-status", "reset"]:
        verify_consistency()

    if LOG_FILE:
        print(f"\n日誌檔案：{os.path.abspath(LOG_FILE)}，可用 tail -f 查看")

if __name__ == "__main__":
    main()

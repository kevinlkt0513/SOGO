#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
清理遷移環境腳本
功能：
- 刪除 logs 目錄下所有日誌檔案
- 重置 migration_checkpoint.json 為空初始狀態
- 清空 MongoDB 相關集合中的資料並刪除索引
"""

import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv

# ========== 環境與設定 ==========
load_dotenv(".env.dev", override=True)  # 或指定其他環境檔

LOG_DIR = "logs"
CHECKPOINT_FILE = "migration_checkpoint.json"

# MongoDB 連線設定
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DBNAME", "gift")  # 與遷移工具中使用的資料庫名一致

# 需清空的集合名稱
COLLECTIONS = [
    "events",
    "event_attendees",
    # 嵌入陣列不獨立清空，刪除 events 即同步清空
]

def clear_logs():
    """刪除 logs 目錄下所有檔案"""
    if os.path.isdir(LOG_DIR):
        for fname in os.listdir(LOG_DIR):
            path = os.path.join(LOG_DIR, fname)
            if os.path.isfile(path):
                os.remove(path)
        print(f"已清空日誌目錄：{LOG_DIR}")
    else:
        print(f"日誌目錄不存在：{LOG_DIR}")

def reset_checkpoint():
    """重置 checkpoint.json 為所有表初始結構"""
    try:
        tables = ["events", "hcc_events", "attendees", "coupon_burui", "coupon_setting", "member_types"]
        data = {
            tbl: {
                "last_checkpoint_time": None,
                "last_checkpoint_id": None,
                "base_windows": [],
                "correction_windows": []
            }
            for tbl in tables
        }
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"已重置檔案：{CHECKPOINT_FILE}")
    except Exception as e:
        print(f"重置 checkpoint 時發生錯誤：{e}")

def clear_mongodb():
    """連線 MongoDB，清空集合並刪除索引"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        for coll_name in COLLECTIONS:
            coll = db[coll_name]
            # 刪除所有文檔
            result = coll.delete_many({})
            print(f"集合 {coll_name} 已刪除 {result.deleted_count} 筆文件")
            # 刪除除 _id 以外的索引
            index_info = coll.index_information()
            for index_name in list(index_info.keys()):
                if index_name != "_id_":
                    coll.drop_index(index_name)
                    print(f"集合 {coll_name} 已刪除索引 {index_name}")
        client.close()
    except Exception as e:
        print(f"清空 MongoDB 時發生錯誤：{e}")

if __name__ == "__main__":
    clear_logs()
    reset_checkpoint()
    clear_mongodb()
    print("環境清理完成。")

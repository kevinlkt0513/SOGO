import os
from locust import User, task, events
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv

# 從 .env.uat 文件加載環境變數
load_dotenv('.env.uat')

# 從環境變數讀取要測試的 URI
# 這樣我們就可以在不修改程式碼的情況下，切換測試目標
TEST_MONGO_URI = os.getenv("MONGO_URI")

class MongoDBConnectionTester(User):
    # 這個 User 不會執行迴圈任務，只在啟動時測試一次
    # 設置一個很大的 wait_time 讓它在 on_start 後保持運行狀態
    wait_time = 3600

    def on_start(self):
        if not TEST_MONGO_URI:
            print("錯誤：請設定 TEST_MONGO_URI 環境變數！")
            # 讓 Locust 程序失敗退出
            self.environment.runner.quit()
            return

        print(f"--- 正在嘗試使用以下 URI 進行連線 ---")
        print(TEST_MONGO_URI)
        
        try:
            # 建立 MongoClient，設定一個較短的超時時間方便快速看到結果
            client = MongoClient(TEST_MONGO_URI, serverSelectionTimeoutMS=10000) # 10秒超時
            
            # server_info() 是一個會強制建立連線並驗證的指令
            client.server_info()
            
            print("\n✅ ✅ ✅  連線成功！資料庫版本:", client.server_info()['version'])
            
        except ConnectionFailure as e:
            print(f"\n❌ ❌ ❌ 連線失敗！錯誤訊息: {e}")
        except Exception as e:
            print(f"\n❌ ❌ ❌ 發生未知錯誤: {e}")
        finally:
            # 測試結束後讓 Locust 程序自動退出
            self.environment.runner.quit()
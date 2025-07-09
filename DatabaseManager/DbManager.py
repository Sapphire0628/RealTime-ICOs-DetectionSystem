#==================================================================================
# 檔案名稱：DbManager.py
# 功能描述：數據庫管理類，提供與SQLite數據庫交互的各種操作，包括：
#          - 數據查詢與執行
#          - 表格創建、刪除與修改
#          - 加密貨幣相關表格（代幣、合約、推文等）的結構定義與創建
# 使用方式：實例化DatabaseManager類並提供數據庫路徑，然後調用相應方法
# 作者：ICO-DataHub團隊
#==================================================================================

import sqlite3  # 導入SQLite數據庫API模塊


class DatabaseManager:
    """
    數據庫管理類，提供對SQLite數據庫的各種操作接口。
    此類封裝了數據庫連接、查詢執行和表格管理等功能，
    專門用於處理加密貨幣相關數據。
    """
    
    def __init__(self, db_path):
        """
        初始化數據庫管理器。
        
        參數:
            db_path (str): SQLite數據庫文件的路徑。如果該文件不存在，將被自動創建。
        """
        self.db_path = db_path  # 保存數據庫路徑以供後續方法使用

    def get_query(self, query, parameters=None):
        """
        執行SELECT查詢並返回結果集。
        
        參數:
            query (str): 要執行的SQL查詢語句，通常是SELECT語句。
            parameters (tuple, optional): 參數化查詢的參數，用於防止SQL注入攻擊。
            
        返回:
            list: 查詢結果的列表，每個元素是一個元組，代表一行數據。
                 如果查詢失敗，返回None。
                 
        示例:
            # 獲取所有代幣
            tokens = db_manager.get_query("SELECT * FROM tokens")
            
            # 使用參數化查詢獲取特定地址的代幣
            token = db_manager.get_query("SELECT * FROM tokens WHERE ContractAddress = ?", 
                                        ("0x123abc...",))
        """
        try:
            # 連接到數據庫
            with sqlite3.connect(self.db_path) as connection:
                cursor = connection.cursor()
                # 如果提供了參數，使用參數執行查詢（防止SQL注入）
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                # 獲取查詢的所有結果
                results = cursor.fetchall()
                return results
        except sqlite3.Error as e:
            # 若發生錯誤，打印錯誤信息並返回None
            print(f"數據庫查詢錯誤: {e}")
            return None

        
    def execute_query(self, query, parameters=None):
        """
        執行SQL更新查詢（如INSERT、UPDATE、DELETE、CREATE TABLE等）。
        
        與get_query不同，此方法用於執行不需要返回結果集的SQL命令，
        而是用於修改數據庫結構或內容。
        
        參數:
            query (str): 要執行的SQL查詢語句。
            parameters (tuple, optional): 參數化查詢的參數，用於防止SQL注入攻擊。
            
        返回:
            無返回值，但會在出錯時打印錯誤信息。
            
        示例:
            # 插入新記錄
            db_manager.execute_query(
                "INSERT INTO tokens (ContractAddress, TokenName) VALUES (?, ?)",
                ("0x123abc...", "MyToken")
            )
            
            # 更新記錄
            db_manager.execute_query(
                "UPDATE tokens SET isScam = ? WHERE ContractAddress = ?",
                ("true", "0x123abc...")
            )
        """
        try:
            # 連接到數據庫
            with sqlite3.connect(self.db_path) as connection:
                cursor = connection.cursor()
                # 如果提供了參數，使用參數執行查詢（防止SQL注入）
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                # 提交事務，使更改永久生效
                connection.commit()
        except sqlite3.Error as e:
            # 若發生錯誤，打印錯誤信息
            print(f"數據庫執行錯誤: {e}")

    def drop_table(self, table_name):
        """
        從數據庫中刪除指定表格。
        
        如果表格不存在，此操作不會引發錯誤。
        
        參數:
            table_name (str): 要刪除的表格名稱。
            
        返回:
            無返回值，但會打印操作結果信息。
            
        警告:
            此操作會永久刪除表格及其所有數據，不可恢復。
        """
        self.execute_query(f"DROP TABLE IF EXISTS {table_name}")
        print(f"表格 '{table_name}' 已刪除（如果存在）。")

    def delete_records(self, table_name):
        """
        刪除表格中的所有記錄，但保留表結構。
        
        此方法執行的是DELETE操作，而非DROP，因此表格結構會保留。
        
        參數:
            table_name (str): 要清空記錄的表格名稱。
            
        返回:
            無返回值，但會打印操作結果信息。
            
        警告:
            此操作會永久刪除表格中的所有數據，但保留表結構。
        """
        self.execute_query(f"DELETE FROM {table_name}")
        print(f"已刪除表格 '{table_name}' 中的所有記錄。")

    def create_table(self, table_name, schema):
        """
        使用指定的結構定義創建新表格。
        
        如果表格已存在，此操作不會重新創建或修改已有表格。
        
        參數:
            table_name (str): 要創建的表格名稱。
            schema (str): 表格結構定義，包含列名、數據類型和約束。
                        例如: "id INTEGER PRIMARY KEY, name TEXT NOT NULL"
            
        返回:
            無返回值，但會打印操作結果信息。
            
        示例:
            db_manager.create_table(
                "new_tokens", 
                "address TEXT PRIMARY KEY, name TEXT NOT NULL, supply INTEGER"
            )
        """
        self.execute_query(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})")
        print(f"表格 '{table_name}' 已創建。")

    def create_tokens_table(self):
        """
        創建'tokens'表格，用於存儲加密代幣的基本信息和安全屬性。
        
        此表格包含代幣的基本屬性（合約地址、名稱、符號等）、
        社交媒體鏈接、安全相關屬性（是否詐騙、蜜罐等）、
        以及稅率信息等重要字段。
        
        返回:
            無返回值，但會通過create_table方法打印操作結果。
            
        表格結構:
            - 主鍵: ContractAddress (代幣合約地址)
            - 包含代幣基本信息、社交媒體、安全屬性等多種數據
        """
        schema = """
            ContractAddress TEXT PRIMARY KEY,   -- 代幣的唯一識別符（智能合約地址）
            pairAddress TEXT,          -- 代幣交易對地址（用於DEX流動性池）
            Owner TEXT ,                -- 代幣合約擁有者地址
            Creator TEXT,              -- 代幣合約創建者地址
            TokenName TEXT NOT NULL,            -- 代幣名稱
            Symbol TEXT NOT NULL,               -- 代幣符號（如ETH、BTC）
            TotalSupply INTEGER,       -- 代幣總供應量
            Decimal INTEGER,           -- 代幣小數位數
            CreatedBlock INTEGER,     -- 代幣創建時的區塊號
            TwitterUrl TEXT,                -- 代幣的Twitter（X）頁面URL
            WebsiteUrl TEXT,                -- 代幣的官方網站URL
            TwitterUser TEXT,              -- 代幣的Twitter用戶名
            TelegramUrl TEXT,               -- 代幣的Telegram頻道URL
            WhitepaperUrl TEXT,             -- 代幣白皮書URL
            isScam TEXT,                    -- 指示代幣是否為詐騙幣（true/false）
            isPotentialSpam TEXT,           -- 指示代幣是否為潛在垃圾幣（true/false）
            safetyLevel TEXT,               -- 代幣的安全級別評估
            spamCode INT,                   -- 垃圾幣類型的編碼
            attackTypes TEXT,               -- 與代幣相關的攻擊類型（多個類型以逗號分隔）
            is_open_source TEXT,            -- 指示代幣合約是否開源（true/false）
            is_honeypot TEXT,               -- 指示代幣是否為蜜罐（無法賣出的騙局）（true/false）
            is_mintable TEXT,                -- 指示代幣是否可鑄造（增發）（true/false）
            is_proxy TEXT,                  -- 指示代幣是否為代理合約（true/false）
            slippage_modifiable TEXT,       -- 指示代幣是否可修改滑點（true/false）
            is_blacklisted TEXT,            -- 指示代幣是否具有黑名單功能（true/false）
            min_sell_tax FLOAT,             -- 代幣的最低賣出稅率（百分比）
            max_sell_tax FLOAT,             -- 代幣的最高賣出稅率（百分比）
            min_buy_tax FLOAT,              -- 代幣的最低買入稅率（百分比）
            max_buy_tax FLOAT,              -- 代幣的最高買入稅率（百分比）
            is_contract_renounced TEXT,     -- 指示合約所有權是否已放棄（true/false）
            is_potentially_scam TEXT,       -- 指示代幣是否為潛在詐騙（true/false）
            transfer_pausable TEXT,         -- 指示代幣轉賬功能是否可暫停（true/false）
            warnings TEXT,                   -- 與代幣相關的警告信息
            FetchedAt TEXT,                 -- 獲取代幣數據的時間戳
            creationTime TEXT,              -- 代幣創建時間
            firstSwapTimestamp TEXT,        -- 代幣首次交易時間戳
            locksCreatedAt TEXT,            -- 代幣流動性鎖定創建時間
            creatorAddress TEXT,              
            smart_contract_verified INT,   
            twitter_verified INT         
        """
        self.create_table("tokens", schema)

    def create_contracts_table(self):
        """
        創建'contracts'表格，用於存儲智能合約的源代碼和編譯相關信息。
        
        此表格存儲從區塊鏈瀏覽器（如Etherscan、BSCScan）獲取的
        智能合約源代碼及其相關編譯器設置、優化信息等。
        
        返回:
            無返回值，但會通過create_table方法打印操作結果。
            
        表格結構:
            - 主鍵: ContractAddress (合約地址)
            - 包含源代碼、編譯器版本、優化設置等信息
        """
        schema = """
            ContractAddress TEXT PRIMARY KEY,    -- 智能合約地址，作為唯一識別符
            SourceCode TEXT,                     -- 合約源代碼（可能是多個文件的JSON格式）
            CompilerVersion TEXT,                -- 編譯合約所用的Solidity編譯器版本
            OptimizationUsed TEXT,               -- 是否使用了編譯優化（"1"表示是，"0"表示否）
            Runs TEXT,                           -- 優化運行次數設置
            EVMVersion TEXT,                     -- 目標EVM（以太坊虛擬機）版本
            Library TEXT,                        -- 合約使用的外部庫
            LicenseType TEXT,                    -- 合約源代碼的許可證類型（如MIT、GPL等）
            Proxy TEXT,                          -- 是否為代理合約
            Implementation TEXT,                 -- 若為代理合約，其實現合約的地址
            SwarmSource TEXT,                    -- SWARM源（分布式存儲中的源代碼引用）
            FetchedAt TEXT                       -- 獲取合約數據的時間戳
        """
        self.create_table("contracts", schema)

    def create_tweets_table(self):
        """
        創建'tweets'表格，用於存儲與加密代幣相關的Twitter推文數據。
        
        此表格存儲從Twitter API獲取的推文內容、互動統計、
        發布時間等信息，用於分析社交媒體與代幣價格、流動性等關係。
        
        返回:
            無返回值，但會通過create_table方法打印操作結果。
            
        表格結構:
            - 主鍵: tweet_id (推文唯一ID)
            - 包含推文內容、互動統計、用戶信息等
        """
        schema = """
            user_id TEXT NOT NULL,               -- 發布推文的用戶ID
            tweet_id TEXT PRIMARY KEY,           -- 推文的唯一識別符
            tweet_full_text TEXT,                -- 推文的完整文本內容
            tweet_favorite_count INTEGER,        -- 推文獲得的點讚數
            tweet_view_count INTEGER,            -- 推文的瀏覽次數
            tweet_quote_count INTEGER,           -- 推文的引用次數
            tweet_reply_count INTEGER,           -- 推文的回覆數
            tweet_retweet_count INTEGER,         -- 推文的轉發數
            tweet_created_at TEXT,               -- 推文的創建時間
            user_name TEXT,                      -- 發布推文的用戶名
            tweet_mention_list TEXT              -- 推文中提及的用戶列表（JSON格式）
        """
        self.create_table("tweets", schema)

    def create_twitter_users_table(self):
        """
        創建'twitter_users'表格，用於存儲Twitter用戶的基本信息。
        
        此表格保存與加密代幣相關的Twitter用戶資料，包括用戶ID、
        用戶名、簡介、創建時間等，用於關聯用戶與其發布的推文，
        以及確認用戶賬號狀態（是否可用、是否被封等）。
        
        返回:
            無返回值，但會通過create_table方法打印操作結果。
            
        表格結構:
            - 主鍵: username (Twitter用戶名)
            - 包含用戶基本信息及狀態
        """
        schema = '''
                user_id TEXT NOT NULL,                  -- Twitter用戶的數字ID
                username TEXT PRIMARY KEY,        -- Twitter用戶名（主鍵，唯一識別符）
                description TEXT,                 -- 用戶的個人簡介
                created_time TEXT,                -- 用戶賬號創建時間
                available TEXT                    -- 用戶賬號是否可用（"true"/"false"）
                
            '''
        self.create_table("twitter_users", schema)

    def create_owner_txn_table(self):
        """
        創建'owner_txn'表格，用於存儲代幣合約擁有者的交易記錄。
        
        此表格儲存從區塊鏈獲取的代幣擁有者的交易歷史，
        包含交易的詳細信息（區塊號、時間戳、交易值、
        函數調用等），用於分析擁有者行為模式和可能的
        惡意活動（如拉盤、砸盤等）。
        
        返回:
            無返回值，但會通過create_table方法打印操作結果。
            
        表格結構:
            - 主鍵: hash (交易哈希值)
            - 包含交易的完整區塊鏈記錄
        """
        schema = '''
                owner TEXT NOT NULL,              -- 交易關聯的代幣擁有者地址
                blockNumber INT,                  -- 包含此交易的區塊號
                blockHash TEXT,                   -- 包含此交易的區塊哈希值
                timeStamp INT,                    -- 交易的時間戳（Unix時間格式）
                hash TEXT PRIMARY KEY,            -- 交易的唯一哈希值（主鍵）
                nonce INT,                        -- 交易發送者的nonce值（交易計數器）
                transactionIndex INT,             -- 交易在區塊中的索引位置
                fromAddress TEXT,                 -- 交易發送方的地址
                toAddress TEXT,                   -- 交易接收方的地址
                value INT,                        -- 交易中轉移的以太幣/代幣數量（以wei為單位）
                gas INT,                          -- 交易的燃料限制
                gasPrice INT,                     -- 交易的燃料價格（以wei為單位）
                input TEXT,                       -- 交易的輸入數據（智能合約調用數據）
                methodId TEXT,                    -- 調用的合約方法ID（函數選擇器）
                functionName TEXT,                -- 調用的合約函數名稱
                contractAddress TEXT,             -- 相關智能合約的地址
                cumulativeGasUsed INT,            -- 交易執行時的累計燃料使用量
                gasUsed INT,                      -- 此交易消耗的燃料量
                confirmations INT,                -- 交易確認數
                isError TEXT                      -- 交易執行是否出錯（"0"表示成功，"1"表示失敗）
            '''
        self.create_table("owner_txn", schema)



    def add_column_to_table(self, table_name, column_name, column_type):
        """
        向現有表格添加新列。
        
        SQLite對表格結構修改的支持較為有限，此方法提供了
        添加新列的功能，但不支持修改或刪除現有列。
        
        參數:
            table_name (str): 要修改的表格名稱。
            column_name (str): 要添加的新列名稱。
            column_type (str): 新列的數據類型，如TEXT、INTEGER、REAL等。
                              也可包含約束，如"TEXT NOT NULL DEFAULT ''"。
            
        返回:
            無返回值，但會打印操作結果信息。
            
        注意:
            - SQLite不支持在ADD COLUMN時添加PRIMARY KEY或UNIQUE約束
            - 新增列將對現有行填充NULL值（除非指定DEFAULT）
            
        示例:
            db_manager.add_column_to_table(
                "tokens",
                "is_verified", 
                "BOOLEAN DEFAULT 0"
            )
        """
        self.execute_query(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        print(f"已向表格 '{table_name}' 添加列 '{column_name}'。")
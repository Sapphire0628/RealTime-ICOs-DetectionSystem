#########################################################################################################################################################
#                                                                                                                                                       #   
# Every 1 minute: The script checks for new tokens in the tokens table and fetches their contract info if not already in the contracts table.           #        
#                                                                                                                                                       #
# Every 5 minutes: The script checks the contracts table for entries without source code and attempts to re-fetch the data                              #
#                                                                                                                                                       #
#########################################################################################################################################################

# #######################################################################################
# ContractScraper: 智能合約源碼爬取器
# #######################################################################################
# 此爬蟲的主要功能:
# 1. 從數據庫的 tokens 表中識別尚未獲取源碼的新代幣
# 2. 通過 Etherscan API 獲取智能合約源碼和編譯相關信息
# 3. 定期重試獲取那些之前獲取失敗的合約源碼
# 4. 存儲以下信息到數據庫:
#    - 合約源碼 (SourceCode)
#    - 編譯器版本 (CompilerVersion)
#    - 優化設置 (OptimizationUsed)
#    - EVM版本 (EVMVersion)
#    - 許可證類型 (LicenseType)
#    - 代理合約信息 (Proxy, Implementation)
# #######################################################################################

import requests
import sqlite3
from datetime import datetime
import schedule
import time
import logging
from config import *
import random


class ContractScraper:
    """
    智能合約源碼爬蟲類
    
    負責從 Etherscan API 獲取智能合約源碼和相關編譯信息,
    並將這些信息存儲到數據庫中供後續分析使用
    """
    
    def __init__(self, db_path, etherscan_api_url, etherscan_api_key, log_file="../Real_Time_System/Log/contracts_scraper.log"):
        """
        初始化 ContractScraper
        
        參數:
            db_path: SQLite 數據庫文件路徑
            etherscan_api_url: Etherscan API 的 URL
            etherscan_api_key: Etherscan API 密鑰列表 (使用隨機密鑰避免請求限制)
            log_file: 日誌文件路徑
        """

        # Initialize logging
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

        # Set database path and API configurations
        self.db_path = db_path
        self.etherscan_api_url = etherscan_api_url
        self.etherscan_api_key = etherscan_api_key

    def fetch_contract_data(self, contract_address):
        """
        從 Etherscan API 獲取智能合約數據
        
        參數:
            contract_address: 要獲取源碼的智能合約地址
            
        返回:
            成功時返回包含合約源碼和編譯信息的字典,失敗時返回 None
        """
        try:

            response = requests.get(self.etherscan_api_url, params={
                "module": "contract",
                "action": "getsourcecode",
                "address": contract_address,
                "apikey": random.choice(self.etherscan_api_key) 
            })

            response_data = response.json()

            if response_data["status"] == "1" and response_data["message"] == "OK":
                result = response_data["result"][0]

                return {
                    "SourceCode": result["SourceCode"],
                    "CompilerVersion": result["CompilerVersion"],
                    "OptimizationUsed": result["OptimizationUsed"],
                    "Runs": result["Runs"],
                    "EVMVersion": result["EVMVersion"],
                    "Library": result["Library"],
                    "LicenseType": result["LicenseType"],
                    "Proxy": result["Proxy"],
                    "Implementation": result["Implementation"],
                    "SwarmSource": result["SwarmSource"]
                }
            else:

                logging.info(f"Failed to fetch data for contract {contract_address}: {response_data['message']}")
                return None
            
        except Exception as e:
            logging.error(f"Error fetching data for contract {contract_address}: {e}")
            return None

    def save_contract_data_to_db(self, contract_address, contract_data):
        """
        將智能合約數據保存到 SQLite 數據庫
        
        參數:
            contract_address: 智能合約地址
            contract_data: 包含合約源碼和編譯信息的字典
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Get the current timestamp
                fetched_at = datetime.now().isoformat()

                # Insert or update contract data with fetching time
                cursor.execute("""
                    INSERT OR REPLACE INTO contracts (
                        contractAddress, SourceCode, CompilerVersion, OptimizationUsed, Runs, EVMVersion, 
                        Library, LicenseType, Proxy, Implementation, SwarmSource, FetchedAt
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    contract_address,
                    contract_data["SourceCode"],
                    contract_data["CompilerVersion"],
                    contract_data["OptimizationUsed"],
                    contract_data["Runs"],
                    contract_data["EVMVersion"],
                    contract_data["Library"],
                    contract_data["LicenseType"],
                    contract_data["Proxy"],
                    contract_data["Implementation"],
                    contract_data["SwarmSource"],
                    fetched_at
                ))
                conn.commit()
                logging.info(f"Contract {contract_address} data saved to the database at {fetched_at}.")

        except Exception as e:

            logging.error(f"Error saving contract {contract_address} to the database: {e}")

    def check_new_tokens(self):
        """
        檢查數據庫中的新代幣並獲取其合約信息
        
        此方法查詢 tokens 表中存在但 contracts 表中不存在的合約地址,
        然後為每個新合約獲取源碼和編譯信息
        """
        logging.info("Checking for new tokens...")
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Find contract addresses in tokens table that are not in contracts table
                cursor.execute("""
                    SELECT t.contractAddress
                    FROM tokens t
                    LEFT JOIN contracts c ON t.contractAddress = c.contractAddress
                    WHERE c.contractAddress IS NULL
                """)
                new_tokens = [row[0] for row in cursor.fetchall()]

            # Fetch and save contract data for new tokens
            for contract_address in new_tokens:
                logging.info(f"Fetching data for new contract: {contract_address}")
                contract_data = self.fetch_contract_data(contract_address)
                if contract_data:
                    self.save_contract_data_to_db(contract_address, contract_data)
       

        except Exception as e:
            logging.error(f"Error checking new tokens: {e}")

    def check_missing_source_code(self):
        """
        檢查缺少源碼的合約並嘗試重新獲取
        
        此方法查詢 contracts 表中 SourceCode 為空的記錄,
        並嘗試重新獲取這些合約的源碼
        """
        logging.info("Checking for contracts missing source code...")
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Find contracts with empty SourceCode
                cursor.execute("""
                    SELECT contractAddress
                    FROM contracts
                    WHERE SourceCode IS NULL OR SourceCode = ''
                """)
                missing_source_contracts = [row[0] for row in cursor.fetchall()]

            # Re-fetch and update contract data
            for contract_address in missing_source_contracts:
                logging.info(f"Re-fetching data for contract: {contract_address}")
                contract_data = self.fetch_contract_data(contract_address)
                if contract_data:
                    self.save_contract_data_to_db(contract_address, contract_data)

        except Exception as e:
            logging.error(f"Error checking contracts missing source code: {e}")

    def start(self):
        """
        啟動定期任務來檢查新代幣和缺失源碼
        
        每1分鐘檢查一次新代幣
        每4分鐘檢查一次缺失源碼
        """
        logging.info("Starting the scheduler...")

        # Schedule tasks
        schedule.every(1).minutes.do(self.check_new_tokens)
        schedule.every(4).minutes.do(self.check_missing_source_code)

        # Run the scheduler
        while True:
            schedule.run_pending()



def main():

    # Set database path and API configurations
    contractscraper = ContractScraper(DB_PATH, ETHERSCAN_API_URL, ETHERSCAN_API_KEY)
    contractscraper.start()


if __name__ == "__main__":
    main()
#%%

# ########################################################################################
# DextoolScraper: 爬取 Dextools 平台上的代幣安全資訊與交易數據
# ########################################################################################
# 此爬蟲主要功能:
# 1. 從數據庫獲取尚未有安全審計資訊的代幣地址
# 2. 使用 Dextools API 檢查代幣的安全狀態和交易信息
# 3. 爬取包括:
#    - 蜜罐偵測 (Honeypot)
#    - 開源狀態 (Open Source)
#    - 稅率信息 (Buy/Sell Tax)
#    - 創建者地址 (Creator Address)
#    - 鏈上活動時間戳 (Creation Time, First Swap)
#    - 代幣鎖倉情況 (Locks)
#    - 安全風險警告 (Warnings)
# ########################################################################################
import re
import requests
from config import *
import sqlite3
import logging
from web3 import Web3
from datetime import datetime

from config import DB_PATH, INFURA_API_KEY, ERC20_ABI, FACTORY_ADDRESS, FACTORY_ABI, DEXTOOL_COOKIES, DEXTOOL_HEADERS
import time


class DextoolScraper:
    """
    爬取 Dextools 平台上的代幣安全信息和交易數據
    
    此爬蟲負責從 Dextools API 獲取代幣的詳細安全審計信息,
    並更新到數據庫中以幫助識別潛在的詐騙代幣和風險因素
    """
    
    def __init__(self, db_path, infura_api_key, erc20_abi, log_file="../Real_Time_System/Log/dextool_scraper.log"):
        """
        初始化 DextoolScraper
        
        參數:
            db_path: SQLite 數據庫路徑
            infura_api_key: Infura API 密鑰,用於連接以太坊區塊鏈
            erc20_abi: ERC20 代幣智能合約 ABI 接口定義
            log_file: 日誌文件路徑
        """

        # 設定日誌紀錄
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        
        # 初始化 Web3 提供者
        self.web3 = Web3(Web3.HTTPProvider(infura_api_key))
        
        # 設定資料庫路徑和 ABI
        self.db_path = db_path
        self.erc20_abi = erc20_abi

    def get_address_from_db(self):
        """
        從資料庫獲取尚未審計的代幣合約地址
        
        返回:
            address: 合約地址列表
        """
        logging.info("Fetching addresses from database")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
                SELECT ContractAddress FROM tokens WHERE creatorAddress IS NULL;                
            """)

        address = cursor.fetchall()
        conn.close()
        address = [c[0] for c in address]
        logging.info(f"Found {len(address)} addresses to process")
        return address

   
    def process_dextool_data(self,contractAddress, result):
        """
        處理從 Dextools 獲取的代幣數據，並提取相關的安全資訊
        
        參數:
            contractAddress: 代幣合約地址
            result: 從 Dextools 獲取的代幣數據
            
        返回:
            提取的代幣安全資訊元組
        """
        creationTime = result['creationTime']
        firstSwapTimestamp = result['firstSwapTimestamp']
        locksCreatedAt = result['token']['locks']
        creator = result['token']['audit']['external']['quickintel']['creator_address']
        TwitterUrl = result['token']['links']['twitter']
        WebsiteUrl = result['token']['links']['website']
        TelegramUrl = result['token']['links']['telegram']
        audit = result['token']['audit']['dextools']
        is_open_source = audit['is_open_source'] # Can detect when launch
        is_honeypot = audit['is_honeypot']       # Can detect when launch
        is_mintable = audit['is_mintable']       # Can detect when launch
        is_proxy = audit['is_proxy']            
        slippage_modifiable = audit['slippage_modifiable']   
        is_blacklisted = audit['is_blacklisted']             # Can detect when launch
        sell_tax = audit['sell_tax']                         
        min_sell_tax = sell_tax['min']
        max_sell_tax = sell_tax['max']                       
        buy_tax = audit['buy_tax']                           
        min_buy_tax = buy_tax['min']
        max_buy_tax = buy_tax['max'] 
        is_contract_renounced = audit['is_contract_renounced'] 
        is_potentially_scam = audit['is_potentially_scam']      
        transfer_pausable = audit['transfer_pausable']        # Can detect when launch
        warnings = audit['summary']['providers']['warning']
        pair_address = self.get_pair_address('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', contractAddress)
        if locksCreatedAt == []:
            locksCreatedAt = None
        if len(warnings) > 0:
            warnings = ','.join(warnings)
        else:
            warnings = None 
        return creationTime,firstSwapTimestamp, locksCreatedAt, creator, pair_address, TwitterUrl, WebsiteUrl, TelegramUrl, is_open_source, is_honeypot, is_mintable, is_proxy, slippage_modifiable, is_blacklisted, min_sell_tax, max_sell_tax, min_buy_tax, max_buy_tax, is_contract_renounced, is_potentially_scam, transfer_pausable, warnings

    
    def update_dextool_info(self,  creationTime, firstSwapTimestamp, locksCreatedAt, creatorAddress, address,pair_address, TwitterUrl, WebsiteUrl, TelegramUrl, is_open_source, is_honeypot, is_mintable, is_proxy, slippage_modifiable, is_blacklisted, min_sell_tax, max_sell_tax, min_buy_tax, max_buy_tax, is_contract_renounced, is_potentially_scam, transfer_pausable, warnings):
        """
        更新資料庫中的代幣安全資訊
        
        參數:
            creationTime: 代幣創建時間
            firstSwapTimestamp: 首次交易時間戳
            locksCreatedAt: 代幣鎖倉時間戳
            creatorAddress: 創建者地址
            address: 代幣合約地址
            pair_address: 交易對地址
            TwitterUrl: 推特鏈接
            WebsiteUrl: 網站鏈接
            TelegramUrl: 電報鏈接
            is_open_source: 是否開源
            is_honeypot: 是否為蜜罐
            is_mintable: 是否可鑄造
            is_proxy: 是否為代理合約
            slippage_modifiable: 是否可調整滑點
            is_blacklisted: 是否在黑名單中
            min_sell_tax: 最小賣出稅
            max_sell_tax: 最大賣出稅
            min_buy_tax: 最小買入稅
            max_buy_tax: 最大買入稅
            is_contract_renounced: 合約是否已放棄
            is_potentially_scam: 是否為潛在詐騙
            transfer_pausable: 轉帳是否可暫停
            warnings: 風險警告
        """
        

        logging.info(f"Updating database for address: {address} , pair address: {pair_address}")
        logging.info(f"Creation Time: {creationTime}, First Swap Timestamp: {firstSwapTimestamp}, Locks Created At: {locksCreatedAt}, Creator Address: {creatorAddress}")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            if TwitterUrl !=  '':
                twitter_user = re.search(r"^https:\/\/(?:x\.com|twitter\.com)\/([a-zA-Z0-9_]+)$", TwitterUrl)
                if twitter_user:
                    twitter_user = twitter_user.group(1)  
                    cursor.execute("""
                        UPDATE tokens SET TwitterUrl = ?, TwitterUser = ? WHERE ContractAddress = ?;
                    """, (TwitterUrl,twitter_user, address))
                    conn.commit()

            if WebsiteUrl !=  '':
                cursor.execute("""
                    UPDATE tokens SET WebsiteUrl = ? WHERE ContractAddress = ?;
                """, (WebsiteUrl, address))
                conn.commit()
            if  TelegramUrl != '':
                cursor.execute("""
                    UPDATE tokens SET TelegramUrl = ? WHERE ContractAddress = ?;
                """, (TelegramUrl, address))
                conn.commit()

            cursor.execute("""
                    UPDATE tokens SET creationTime = ?, firstSwapTimestamp = ? , locksCreatedAt = ?, creatorAddress = ?, pairAddress = ?, TwitterUrl = ?, WebsiteUrl = ?, TelegramUrl = ?, is_open_source = ?, is_honeypot = ?, is_mintable = ?,  is_proxy = ?, slippage_modifiable = ?, is_blacklisted = ?, min_sell_tax = ?, max_sell_tax = ?, min_buy_tax = ?,  max_buy_tax = ?, is_contract_renounced = ?, is_potentially_scam = ?, transfer_pausable = ?, warnings = ?  WHERE ContractAddress = ?;
                """, (creationTime, firstSwapTimestamp, locksCreatedAt, creatorAddress, pair_address, TwitterUrl, WebsiteUrl, TelegramUrl, is_open_source, is_honeypot, is_mintable, is_proxy, slippage_modifiable, is_blacklisted, min_sell_tax, max_sell_tax, min_buy_tax, max_buy_tax, is_contract_renounced, is_potentially_scam, transfer_pausable, warnings, address))
            conn.commit()
            logging.info("Database update successful")
        except Exception as e:
            logging.error(f"Error updating database: {str(e)}")
        finally:
            conn.close()


    def get_pair_address(self, token0, token1):
        """
        根據 Uniswap V2 工廠合約獲取交易對地址
        
        參數:
            token0: 代幣0地址
            token1: 代幣1地址
            
        返回:
            pair_address: 交易對合約地址
        """
        factory_contract = self.web3.eth.contract(address=FACTORY_ADDRESS, abi=FACTORY_ABI)
        # 確保地址為 checksum 格式
        token0 = self.web3.to_checksum_address(token0)
        token1 = self.web3.to_checksum_address(token1)
        
        # 調用 getPair 函數
        pair_address = factory_contract.functions.getPair(token0, token1).call()
        return pair_address
        
    def scrape_info(self):
        """
        爬取代幣資訊主程式
        
        從資料庫獲取代幣地址後,
        依序檢查每個代幣的安全資訊並更新至資料庫
        """
        address_list = self.get_address_from_db()
        
        
        for contractAddress in address_list:
            
            try:
                pair_address = self.get_pair_address('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', contractAddress)
                if pair_address == "0x0000000000000000000000000000000000000000":
                    continue
                logging.info(f"Processing token at {contractAddress} with pair address {pair_address}")

                params = {
                'address':  pair_address.lower(),
                'chain': 'ether',
                'audit': 'true',
                }
                result = requests.get('https://www.dextools.io/shared/data/pair', params=params, cookies=DEXTOOL_COOKIES, headers=DEXTOOL_HEADERS).json()
                logging.info(f"Fetched data for {contractAddress} from Dextools")
                logging.info(f"Result: {result}")
                result = result['data'][0]
                
                
            
                creationTime, firstSwapTimestamp, locksCreatedAt, creatorAddress, pair_address, TwitterUrl, WebsiteUrl, TelegramUrl, is_open_source, is_honeypot, is_mintable, is_proxy, slippage_modifiable, is_blacklisted, min_sell_tax, max_sell_tax, min_buy_tax, max_buy_tax, is_contract_renounced, is_potentially_scam, transfer_pausable, warnings = self.process_dextool_data(contractAddress, result)   

                self.update_dextool_info( creationTime, firstSwapTimestamp, locksCreatedAt, creatorAddress, contractAddress,pair_address,  TwitterUrl, WebsiteUrl, TelegramUrl, is_open_source, is_honeypot, is_mintable, is_proxy, slippage_modifiable, is_blacklisted, min_sell_tax, max_sell_tax, min_buy_tax, max_buy_tax, is_contract_renounced, is_potentially_scam, transfer_pausable, warnings)
            except Exception as e:
                logging.error(f"Error processing token at {contractAddress}: {e}")
                continue
    

                        





if __name__ == "__main__":
    scraper = DextoolScraper(DB_PATH, INFURA_API_KEY, ERC20_ABI)
    while True:
        try:
            scraper.scrape_info()
            logging.info("Waiting 5 minutes before next scrape...")
            time.sleep(300)  # 300 seconds = 5 minutes
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            time.sleep(300)  # Still wait 5 minutes on error



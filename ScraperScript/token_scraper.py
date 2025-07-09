#%%
# #######################################################################################
# TokenScraper: 以太坊區塊鏈 ERC20 代幣監控器
# #######################################################################################
# 此爬蟲的主要功能:
# 1. 實時監控以太坊區塊鏈上的新區塊
# 2. 識別區塊中的合約創建交易
# 3. 驗證新創建的合約是否為 ERC20 代幣
# 4. 獲取代幣基本信息:
#    - 代幣名稱 (name)和代幣符號 (symbol)
#    - 總供應量 (totalSupply)
#    - 小數位數 (decimals)
#    - 創建者地址 (owner)
#    - 創建區塊號 (createdBlock)
# 5. 將獲取的代幣信息存儲到數據庫中
# #######################################################################################

import time
from datetime import datetime
import sqlite3
from web3 import Web3
import logging
import schedule
from config import *


class TokenScraper:
    """
    以太坊區塊鏈 ERC20 代幣爬蟲類
    
    負責監控以太坊區塊鏈上的新區塊,識別並存儲新創建的 ERC20 代幣信息
    """
    
    def __init__(self, db_path, infura_api_key, erc20_abi, log_file="../Real_Time_System/Log/token_scraper.log"):
        """
        初始化 TokenScraper
        
        參數:
            db_path: SQLite 數據庫路徑
            infura_api_key: Infura API 密鑰,用於連接以太坊區塊鏈
            erc20_abi: ERC20 代幣智能合約的 ABI 接口定義
            log_file: 日誌文件路徑
        """
        # Set up logging
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )
        
        # Initialize Web3 provider
        self.web3 = Web3(Web3.HTTPProvider(infura_api_key))
        
        # Set database path and ABI
        self.db_path = db_path
        self.erc20_abi = erc20_abi

    def check_token(self, contract_address, receipt):
        """
        檢查地址是否為 ERC20 代幣並提取其信息
        
        此方法嘗試調用合約的標準 ERC20 函數(name, symbol, decimals, totalSupply, owner)
        來驗證合約是否為 ERC20 代幣並獲取基本信息
        
        參數:
            contract_address: 要檢查的合約地址
            receipt: 合約創建交易的收據
        """
        try:
            # Create a contract object
            contract = self.web3.eth.contract(address=contract_address, abi=self.erc20_abi)

            # Fetch token details
            name = contract.functions.name().call()
            symbol = contract.functions.symbol().call()
            decimals = contract.functions.decimals().call()
            total_supply = contract.functions.totalSupply().call() / (10 ** decimals)
            owner = contract.functions.owner().call()
            # Store the token details in the SQLite database
            created_block = receipt.blockNumber
            logging.info(f"Token {name} ({symbol}) created in block {created_block} Owner: {owner}")

            fetched_at = datetime.now().isoformat()  # e.g., '2025-02-04T10:45:00'

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO tokens 
                    (ContractAddress,Owner, TokenName, Symbol, TotalSupply, Decimal, CreatedBlock, FetchedAt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (contract_address,owner, name, symbol, total_supply, decimals, created_block , fetched_at),
                )
                conn.commit()
                # Log the successful storage of token information
                logging.info(f"Token {name} ({symbol}) with address {contract_address} stored in database.")

            
        except ValueError as ve:
            # Handle specific ValueError for contract not being ERC20
            logging.warning(f"ValueError for contract {contract_address}: {ve}")


                


    def process_block(self, block, processed_contracts):
        """
        處理單個區塊,尋找合約創建交易
        
        參數:
            block: Web3 區塊對象
            processed_contracts: 已處理過的合約地址集合
        """
        for tx in block.transactions:
            # Check if the transaction created a contract
            if tx.to is None:  # 'to' is None for contract creation
                try:
                    receipt = self.web3.eth.get_transaction_receipt(tx.hash)
                    contract_address = receipt.contractAddress

                    if contract_address and contract_address not in processed_contracts:
                        processed_contracts.add(contract_address)  # Mark as processed
                        self.check_token(contract_address, receipt)  # Call the function
                except Exception as e:
                    # Not ERC20 or some other error
                    continue


    def monitor_blocks(self, start_offset=100, sleep_interval=5):
        """
        持續監控以太坊區塊鏈上的新區塊
        
        參數:
            start_offset: 起始點(當前區塊高度減去此值)
            sleep_interval: 檢查新區塊的間隔時間(秒)
        """
        logging.info("Listening for new blocks...")
        processed_contracts = set()  # Track already processed contract addresses

        try:
            # Initial setup
            initial_latest = self.web3.eth.block_number
            start_block = max(0, initial_latest - start_offset)
            logging.info(f"Starting from block {start_block} up to {initial_latest}")

            # Process historical blocks
            for block_num in range(start_block, initial_latest + 1):
                block = self.web3.eth.get_block(block_num, full_transactions=True)
                self.process_block(block, processed_contracts)

            # Now monitor new blocks
            last_processed = initial_latest
            while True:
                current_latest = self.web3.eth.block_number
                if current_latest > last_processed:
                    logging.info(f"Processing blocks from {last_processed + 1} to {current_latest}")
                    for block_num in range(last_processed + 1, current_latest + 1):
                        block = self.web3.eth.get_block(block_num, full_transactions=True)
                        self.process_block(block, processed_contracts)
                    last_processed = current_latest
                else:
                    # No new blocks, sleep for a bit
                    time.sleep(sleep_interval)

        except KeyboardInterrupt:
            logging.info("\nStopped monitoring.")
        except Exception as e:
            logging.error(f"Error: {e}")

    def start(self):
        """
        啟動代幣監控器
        
        此方法啟動對以太坊區塊鏈的持續監控,以識別新的 ERC20 代幣
        """
        logging.info("Starting the scheduler...")

        # Run the scheduler
        while True:
            self.monitor_blocks(start_offset=10000, sleep_interval=10) # ethereum each block is about 15 seconds
            


def main():
    # Set database path and API configurations
    tokenscraper = TokenScraper(DB_PATH, INFURA_API_KEY, ERC20_ABI)
    tokenscraper.start()

if __name__ == "__main__":
    main()
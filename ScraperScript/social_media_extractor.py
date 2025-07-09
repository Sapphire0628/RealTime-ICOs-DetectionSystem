#%%

# #######################################################################################
# SocialMediaExtractor: 社交媒體鏈接提取器
# #######################################################################################
# 此爬蟲的主要功能:
# 1. 從智能合約源碼中提取社交媒體和項目相關鏈接
# 2. 提取並處理多種鏈接類型:
#    - Twitter/X 賬號
#    - Telegram 群組
#    - 項目網站
# 3. 清理和驗證提取的 URL
# 4. 從 Twitter URL 中提取用戶名
# 5. 更新數據庫中代幣的社交媒體信息
# #######################################################################################

import os
import sqlite3
import re
import json
import time
import logging
from config import *

class SocialMediaExtractor:
    """
    智能合約源碼中社交媒體鏈接提取器
    
    此類負責從合約源碼中提取社交媒體URL並更新數據庫
    """

    def __init__(self, db_path, log_file="../Real_Time_System/Log/social_media_extractor.log"):
        """
        初始化社交媒體提取器
        
        參數:
            db_path: 數據庫文件路徑
            log_file: 日誌文件路徑
        """
        self.db_path = db_path
        

        # Set up logging
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

        # URL extraction regex patterns
        self.url_patterns = {
            "twitter": r"(https?://(x\.com|twitter\.com)/[^\s\/\\]+)",  # Match Twitter URLs
            "telegram": r"(https?://t\.me/[^\s\\]+)",  # Match Telegram URLs
            "website": r"(https?://[^\s\\]+)",  # Match any general URL
        }

    def clean_url(self, url):
        """
        清理和規範化提取的 URL
        
        移除額外字符、片段和無效部分
        
        參數:
            url: 要清理的原始 URL
        返回:
            清理後的 URL 或 None
        """
        if url:
            url = url.strip().rstrip("\\\r\n")
            # Remove fragments or invalid characters
            for delim in ["#", "[", "]", "(", ")"]:
                if delim in url:
                    url = url.split(delim)[0]
            return url
        return None

    def get_source_content(self, source_code_json):
        """
        從 JSON 格式的源碼中提取內容
        
        處理複雜的 JSON 格式源碼,提取實際的代碼內容
        
        參數:
            source_code_json: JSON 格式的源碼字符串
        返回:
            提取的源碼內容或原始源碼
        """
        try:
            source_data = json.loads(source_code_json)
            for value in source_data.get("sources", {}).values():
                if "content" in value:
                    return value["content"]
        except (json.JSONDecodeError, AttributeError):
            pass
        return source_code_json  # Fallback to plain text if parsing fails

    def extract_urls(self, source_code):
        """
        從源碼中提取 Twitter、Telegram 和網站 URL
        
        參數:
            source_code: 合約源碼
        返回:
            (twitter_url, website_url, telegram_url) 元組
        """
        extracted_urls = {key: None for key in self.url_patterns}
        all_urls = re.findall(self.url_patterns["website"], source_code)

        for key, pattern in self.url_patterns.items():
            match = re.search(pattern, source_code)
            if match:
                extracted_urls[key] = self.clean_url(match.group(1))

        # Find the first general website URL not matching Twitter/Telegram
        for url in all_urls:
            clean = self.clean_url(url)
            if clean and clean not in (extracted_urls["twitter"], extracted_urls["telegram"]):
                extracted_urls["website"] = clean
                break

        return extracted_urls["twitter"], extracted_urls["website"], extracted_urls["telegram"]

    def update_tokens_table(self):
        """
        提取社交媒體 URL 並更新 tokens 表
        
        此方法從合約源碼中提取社交媒體鏈接,
        並僅在數據庫中相應字段為空時更新記錄
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ContractAddress, SourceCode 
                FROM contracts
                WHERE SourceCode IS NOT NULL;
                
            """)
            contracts = cursor.fetchall()
            


            for contract_address, source_code in contracts:
                twitter_url, website_url, telegram_url = self.extract_urls(source_code)



                # Fetch existing token data for comparison
                cursor.execute("""
                    SELECT TwitterUrl,TwitterUser, WebsiteUrl, TelegramUrl 
                    FROM tokens
                    WHERE ContractAddress = ?
                """, (contract_address,))
                token_data = cursor.fetchone()

                updates = []
                # Skip if no token data exists
                if token_data is None:
                    logging.warning(f"No token data found for ContractAddress: {contract_address}")
                    continue



                if  token_data[0] is None and twitter_url:
                    # if twitter_url  contains "dogecoin", "doge", "shiba", "shib", "floki" or "pepe" then skip
                    if any(keyword in twitter_url for keyword in ["VitalikButerin", "elonmusk", "cz_binance", "cb_doge", "WhiteHouse", "kanyewest","dogecoin","DEXToolsApp"]):
                            updates.append(("TwitterUrl", None))
                    else:
                        updates.append(("TwitterUrl", twitter_url))

                    print(twitter_url)
                if token_data[1] is None and twitter_url:
            
                    twitter_user_match = re.search(r"^https:\/\/(?:x\.com|twitter\.com)\/([a-zA-Z0-9_]+)$", twitter_url)
                    if twitter_user_match:
                        twitter_user = twitter_user_match.group(1)  # Extract username
                        if any(keyword in twitter_user for keyword in ["VitalikButerin", "elonmusk", "cz_binance", "cb_doge", "WhiteHouse", "kanyewest","dogecoin","DEXToolsApp"]):
                            updates.append(("TwitterUser", None))
                        else:
                            updates.append(("TwitterUser", twitter_user))



                if token_data[2] is None and website_url:
                    updates.append(("WebsiteUrl", website_url))
                if token_data[3] is None and telegram_url:
                    updates.append(("TelegramUrl", telegram_url))

                # Apply updates only if there are changes
                for column, value in updates:
                    # Ensure value is a valid SQLite type
                    if not isinstance(value, (str, type(None))):
                        raise ValueError(f"Invalid data type for column {column}: {type(value)}")
                            
                    cursor.execute(f"""
                        UPDATE tokens
                        SET {column} = ?
                        WHERE ContractAddress = ?
                    """, (value, contract_address))

                    conn.commit()
            logging.info("Tokens table updated successfully.")
            time.sleep(60)

        except sqlite3.Error as e:
            logging.error(f"An error occurred: {e}")


    def start(self):
        """
        啟動社交媒體提取器的持續運行
        
        此方法會持續執行 update_tokens_table 方法,
        從合約源碼中提取社交媒體鏈接
        """
        logging.info("Starting the scheduler...")


        # Run the scheduler
        while True:
            self.update_tokens_table()



def main():
    # Set database path and API configurations
    social_media_extractor = SocialMediaExtractor(DB_PATH)
    social_media_extractor.start()

if __name__ == "__main__":
    main()
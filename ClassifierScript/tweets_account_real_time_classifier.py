import os
from dotenv import load_dotenv
import pandas as pd
import json
import requests
import sqlite3
import logging
from datetime import datetime
import time
import schedule

load_dotenv()  # 加載.env文件中的環境變量
DB_PATH = os.getenv("DB_PATH")  # 從環境變量中獲取數據庫路徑
DS_API_KEY = os.getenv("DS_API_KEY")

# 設置日誌配置
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Log"))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_path = os.path.join(log_dir, "twitter_classifier.log")

# 配置日誌格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()  # 同時輸出到控制台
    ]
)

logger = logging.getLogger(__name__)



def get_active_twitter_accounts(conn):
    """Get active Twitter accounts from database"""
    logger.info("Fetching active Twitter accounts")
    query = """
    SELECT user_id, username 
    FROM twitter_users 
    WHERE available = 'True'
    """
    accounts = pd.read_sql_query(query, conn)
    logger.info(f"Found {len(accounts)} active Twitter accounts")
    return accounts

def get_tweets_history(conn, user_id):
    """Get tweet history for a given user_id"""
    logger.info(f"Fetching tweets history for user_id: {user_id}")
    query = f"""
    SELECT user_id, tweet_full_text, tweet_created_at 
    FROM tweets 
    WHERE user_id = ?
    """
    tweets = pd.read_sql_query(query, conn, params=(user_id,))
    logger.info(f"Retrieved {len(tweets)} tweets for user_id: {user_id}")
    return tweets

def get_response(payload, url, headers):
    """
    向指定URL發送POST請求並獲取響應
    
    Args:
        payload (dict): 請求內容
        url (str): API端點URL
        headers (dict): HTTP請求頭
        
    Returns:
        dict: API響應的JSON內容
    """
    try:
        logger.debug(f"Sending POST request to {url}")
        response = requests.request("POST", url, json=payload, headers=headers)
        response.raise_for_status()  # 檢查 HTTP 錯誤
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        raise

def create_payload(token_name, history, score, model_name):
    """
    根據指定的模型創建API請求負載
    
    Args:
        token_name (str): 代幣名稱
        history (str): 代幣相關推文歷史
        score (float): 相似度評分
        model_name (str): 要使用的模型名稱
        
    Returns:
        list/dict: 適合指定模型API的請求格式
    """
    # 創建通用的消息內容
    messages = [
            {
                "role": "system",
                "content": "You are an AI agent that classifies cryptocurrency tokens as scam (isScam = 1) or non-scam (isScam = 0) based on  Twitter history. Evaluate post frequency, content quality, engagement, and token name. Use a balanced scoring approach to avoid defaulting to scam. Return a JSON object with classification, confidence, and reasoning."
            },
            {
                "role": "user",
                "content": """Classify cryptocurrency tokens as scam (isScam = 1) or non-scam (isScam = 0) using  Twitter history.
                              
                    **Classification Criteria**:
                    1. **Post Frequency (30%)**:
                        - Scam: 1–2 posts at launch or unavailable history.
                        - Non-Scam: 3+ posts over weeks/months.
                    2. **Content Quality (40%)**:
                        - Scam: Hype-driven (e.g., 'moon', #MEMECOIN), no technical details (e.g., audits, burns, etc.,), repetitive.
                        - Non-Scam: Specific updates (e.g., partnerships, audits, listings, etc.,), transparent, varied content.
                    3. **Engagement (40%)**:
                        - Scam: One-way, hype-focused posts, no community interaction.
                        - Non-Scam: Community-driven (e.g., AMAs, contests, etc.,), responsive.
                  

                   **Steps**:
                    1. Score each criterion (0–100):
                    - Frequency: 100 if 3+ posts over weeks, 50 if 1–2 posts, 0 if unavailable.
                    - Content: 100 if specific/technical, 50 if mixed, 0 if only hype/repetitive.
                    - Engagement: 100 if community-driven, 50 if limited interaction, 0 if one-way.
                    2. Calculate weighted average score (0–100). If score ≥ 20, classify as non-scam (isScam = 0); else, scam (isScam = 1).
                    3. Set confidence: 0.9–1.0 for strong patterns (score < 20 or > 80), 0.5–0.7 for ambiguous (score 20–80).
                    4. Provide reasoning citing criteria.

                    Edge Cases:
                        - Ambiguous posts: Prioritize content quality and engagement; lower confidence.
                        - Hype in non-scam: Require 3+ posts with technical details or engagement for isScam = 0.
                    
                    **Example**:
                        - **Scam**: Token Name: MuskMoonCoin, Twitter History: [{'timestamp': '2025-06-01', 'content': 'To the MOON! #MEMECOIN'}]
                        - Output: {'token_name': 'MuskMoonCoin', 'is_scam': 1, 'confidence': 0.95, 'reasoning': 'Exploitative name (Musk). Single hype post, no technical details, no engagement.'}
                        - **Non-Scam**: Token Name: GameChain, Twitter History: [{'timestamp': '2025-05-01', 'content': 'Partnered with Web3 studio! #GameChain'}, {'timestamp': '2025-06-01', 'content': 'Audit done, Coingecko listed!'}]
                        - Output: {'token_name': 'GameChain', 'is_scam': 0, 'confidence': 0.90, 'reasoning': 'Project-aligned name. Multiple posts with partnerships and audits, community-focused.'}
                    """+
                    f"""
                    Input:
                    Token Name:{token_name}
                    Twitter History: {history}
                    """+
                    """
                            Output Format::
                            ```json
                            {
                                'token_name': '<string>',
                                'is_scam': <0 or 1>,
                                'confidence': <0.0–1.0>,
                                'reasoning': '<brief explanation>'
                                }
                            ```"""
            }
             ]
    
    # 根據不同模型返回不同格式的請求內容
    if model_name == "Grok":
        return messages
    if model_name == "DeepSeek":
        return{
        "model": "deepseek-ai/DeepSeek-V3",
        "messages": messages,
        "max_tokens": 8192, # 限制模型生成的最大token數
        "temperature": 0.5,  # 控制模型輸出的隨機性，值範圍0~1
        "top_p": 0.7,        # 控制核採樣範圍，值範圍0~1
        "n": 1               # 控制每個請求生成多少個不同答案
    }
    if model_name == "ChatGPT":
        return messages
    return 

def extract_and_convert_to_json(result_str):
    """
    從LLM返回的字符串中提取JSON對象
    
    Args:
        result_str (str): LLM返回的結果字符串
        
    Returns:
        dict: 解析後的JSON對象，解析失敗則返回None
    """
    try:
        # 找到JSON開始和結束的位置
        start_idx = result_str.find("{")
        end_idx = result_str.rfind("}") + 1
        
        # 提取JSON子字符串
        json_str = result_str[start_idx:end_idx]
        
        # 將字符串轉換為JSON
        result_json = json.loads(json_str)
        logger.debug(f"Successfully parsed JSON: {result_json}")
        return result_json
    except (ValueError, json.JSONDecodeError) as e:
        logger.error(f"Error parsing JSON: {e}")
        return None

def ds_request(token_name, history, score):
    """
    向DeepSeek API發送請求
    
    Args:
        token_name (str): 代幣名稱
        history (str): 代幣相關推文歷史
        score (float): 相似度評分
        
    Returns:
        dict: API的響應
    """
    logger.info(f"Preparing DeepSeek API request for token: {token_name}")
    url = "https://api.siliconflow.cn/v1/chat/completions"
    
    headers = {
            "Authorization": f"Bearer {DS_API_KEY}",
            "Content-Type": "application/json"}
    payload = create_payload(token_name, history, score, "DeepSeek")
    logger.info(f"Sending request to DeepSeek API for token: {token_name}")
    response = get_response(payload, url, headers)
    logger.info(f"Received response from DeepSeek API for token: {token_name}")
    
    return response

def update_token_verification(conn, username, is_scam):
    """Update token verification status in database"""
    logger.info(f"Updating token verification for {username}, is_scam={is_scam}")
    query = """
    UPDATE tokens 
    SET twitter_verified = ? 
    WHERE TwitterUser = ?
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (is_scam, username))
        conn.commit()
        logger.info(f"Successfully updated token verification for {username}")
    except sqlite3.Error as e:
        logger.error(f"SQLite error when updating token verification for {username}: {e}")
        raise

def main():
    # Database connection
    logger.info("Starting Twitter account real-time classifier")
    conn = None
    if not DB_PATH:
        logger.error("Error: DB_PATH environment variable is not set")
        return
    
    if not DS_API_KEY:
        logger.error("Error: DS_API_KEY environment variable is not set")
        return
        
    try:
        # 連接到SQLite資料庫
        logger.info(f"Connecting to database at {DB_PATH}")
        
        conn = sqlite3.connect(DB_PATH)
        logger.info("Database connection established")
        
        # DeepSeek API configuration
        url = "https://api.siliconflow.cn/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {DS_API_KEY}",
            "Content-Type": "application/json"
        }
        logger.info("DeepSeek API configured")
        
        # Get active Twitter accounts
        active_accounts = get_active_twitter_accounts(conn)
        
        for _, account in active_accounts.iterrows():
            try:
                logger.info(f"Processing Twitter account: {account['username']} (ID: {account['user_id']})")
                # Get tweets history
                tweets = get_tweets_history(conn, account['user_id'])
                
                if tweets.empty:
                    # If no tweets found, mark as scam
                    logger.warning(f"No tweets found for {account['username']}, marking as scam")
                    update_token_verification(conn, account['username'], 1)
                    continue
                
                # Process tweets data
                tweets['date'] = pd.to_datetime(tweets['tweet_created_at']).dt.date
                logger.info(f"Processing {len(tweets)} tweets for {account['username']}")
                
                # Group tweets by date
                tweets_history = {
                    str(date): texts 
                    for date, texts in zip(tweets['date'], tweets['tweet_full_text'])
                }
                
                # Create payload and get classification
                logger.info(f"Creating API payload for {account['username']}")
                payload = create_payload(account['username'], tweets_history, 0, "DeepSeek")
                logger.info(f"Sending request to DeepSeek API for {account['username']}")
                response = get_response(payload, url, headers)
                logger.info(f"Received response from DeepSeek API for {account['username']}")
                
                # Extract classification result
                if 'choices' in response:
                    result = extract_and_convert_to_json(response['choices'][0]['message']['content'])
                    if result and 'is_scam' in result:
                        logger.info(f"Classification result for {account['username']}: is_scam={result['is_scam']}, confidence={result.get('confidence', 'N/A')}")
                        # Update database with classification result
                        update_token_verification(conn, account['username'], result['is_scam'])
                    else:
                        logger.warning(f"Could not extract valid result from API response for {account['username']}")
                else:
                    logger.error(f"Invalid API response format for {account['username']}: {response}")
            except Exception as e:
                logger.error(f"Error processing account {account['username']}: {e}", exc_info=True)
    
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        # 確保資料庫連接正確關閉
        if conn:
            logger.info("Closing database connection")
            conn.close()
            
    logger.info("Twitter account real-time classifier completed")

if __name__ == "__main__":
    
    # Schedule the job to run daily
    schedule.every(1).days.do(main)
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute
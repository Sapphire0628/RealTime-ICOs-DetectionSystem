import os
import requests
import sqlite3
import time
import logging
import json
from openai import OpenAI  # OpenAI API客戶端
from dotenv import load_dotenv  # 用於加載環境變量
import logging
load_dotenv()  # 加載.env文件中的環境變量
DB_PATH = os.getenv("DB_PATH")  # 從環境變量中獲取數據庫路徑
OPAI_API_KEY = os.getenv("OPAI_API_KEY")  # 從環境變量中獲取OpenAI API密鑰
DS_API_KEY = os.getenv("DS_API_KEY")
GROK_API_KEY = os.getenv("GROK_API_KEY")



def get_response(payload, url, headers):
    """
    向指定URL發送POST請求並獲取響應
    
    Args:
        payload (dict): 請求體內容
        url (str): 請求的URL
        headers (dict): 請求頭
        
    Returns:
        dict: API響應的JSON內容
    """
    response = requests.request("POST", url, json=payload, headers=headers).json()

    return response

def create_payload(source_code , model_name):
    """
    根據模型名稱創建不同格式的請求內容
    
    Args:
        source_code (str): 智能合約源碼
        model_name (str): 使用的LLM模型名稱("Grok", "DeepSeek", "ChatGPT")
        
    Returns:
        dict或list: 適合指定模型的請求內容格式
    """
    messages = [
            {
                "role": "system",
                "content": "You are an ERC20 smart contract security analyzer."
            },
            {
                "role": "user",
                "content": f"""Analyze this smart contract for security features and return a JSON array with the following properties based on the provided source code: {source_code}

                            1. isHoneyPot: Check if the token may not be sold due to contract functions or is designed to trap users.
                            Key Checks:
                            - Admin Abuse: Can the deployer drain funds (e.g., via privileged functions)?
                            - Fund Drain:
                                Are there functions that steal ETH or tokens?
                                Are taxes or WETH sent to a withdrawable wallet?
                                Are tax rates extremely high (e.g., 99% or similar) indicating a honeypot? (Note: Taxes between 10%–50% are acceptable.)
                            - Tax Traps: Does the tax system contain traps (e.g., dynamic taxes that spike on sell, hidden fees)?
                            - Transfer Tricks: Does _transfer() block sells or hide burns (e.g., via reverts or silent failures)?
                            - DEX Exploits: Can the owner remove liquidity or manipulate swaps (e.g., via pair control)?
                            
                            2. isMintable: Check if new tokens can be created, allowing the deployer to arbitrarily manipulate token balances.
                            Key Checks:
                            - Supply Control: Are there functions to increase total supply?
                            - Hidden Mint Functions: Look for disguised minting logic (e.g., indirect calls, misleading names).
                            3. isProxy: Check if the contract uses a proxy pattern (e.g., delegatecall to an implementation contract).
                            4. isBlackList: Check for address blacklisting mechanisms. 
                            Key Checks:
                            - Hidden Control: Is there blacklisting logic (even post-renunciation)?
                            - Obfuscation Tricks: Are there misleading names or structures hiding blacklist functionality?
                            transferPausable: Check if token transfers can be paused by the deployer or another address.
                            
                            """+
                            """Format:
                            ```json
                            [
                              {"feature": "isHoneyPot", "value": "yes/no", "reason": "brief explanation"},
                              {"feature": "isMintable", "value": "yes/no", "reason": "brief explanation"},
                              {"feature": "isProxy", "value": "yes/no", "reason": "brief explanation"},
                              {"feature": "isBlackList", "value": "yes/no", "reason": "brief explanation"},
                              {"feature": "transferPausable", "value": "yes/no", "reason": "brief explanation"}
                            ]
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
    提取結果字符串並將其轉換為JSON對象
    
    Args:
        result_str (str): LLM返回的結果字符串
        
    Returns:
        list: 表示JSON數組的字典列表，解析失敗則返回None
    """
    try:
        # 找到 JSON 開始和結束的位置
        start_idx = result_str.find("[")
        end_idx = result_str.rfind("]") + 1

        # 提取 JSON 子字串
        json_str = result_str[start_idx:end_idx]

        # 將字串轉換為 JSON
        result_json = json.loads(json_str)
        return result_json
    except (ValueError, json.JSONDecodeError) as e:
        logging.info(f"Error parsing JSON: {e}")
        return None


def xai_request(source_code):
    """
    向X.AI (Grok) API發送請求以分析智能合約
    
    Args:
        source_code (str): 智能合約源碼
        
    Returns:
        str: API的響應內容
    """
    client = OpenAI(
        api_key= GROK_API_KEY,
        base_url="https://api.x.ai/v1",
        )
    
    
    messages = create_payload(source_code, "Grok")
    completion = client.chat.completions.create(
        model= 'grok-2-latest',
        messages=messages)

    return completion.choices[0].message.content
    
class SmartContractClassifier:
    def __init__(self, db_path,  log_file="../Real_Time_System/Log/smart_contracts_classifier.log"):
        self.db_path = db_path
        # Initialize the classifier pipeline
        # Initialize logging
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

        
    def connect_db(self):
        try:
            return sqlite3.connect(self.db_path)
        except sqlite3.Error as e:
            logging.error(f"Database connection error: {e}")
            return None

    def get_unverified_contracts(self, conn):
        try:
            cursor = conn.cursor()
            query = """
                SELECT contractAddress, sourceCode 
                FROM contracts 
                WHERE contractAddress NOT IN (
                    SELECT contractAddress 
                    FROM tokens 
                    WHERE smart_contract_verified IS NOT NULL
                )
            """
            return cursor.execute(query).fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching contracts: {e}")
            return []
        
    def parse_source_code(self, source_code, fetch_at=None):
        """
        Parse and clean smart contract source code from various formats.
        Args:
            source_code: The source code string to parse
            fetch_at: Timestamp when the code was fetched (optional)
        Returns:
            Cleaned source code string or None if parsing fails
        """
        try:
            if isinstance(source_code, str) and source_code.strip().startswith('{'):
                source_code = source_code.replace('{{', '{').replace('}}', '}')
                source_code = source_code.replace('\n', '').replace('\r', '')
                
                try:
                    source_code = json.loads(source_code)
                except json.JSONDecodeError as e:
                    logging.error(f"JSON parse error: {e}")
                    return None

                source_code = source_code.get('sources', '') if isinstance(source_code, dict) else source_code
                
                if len(source_code) == 1:
                    key_name = next(iter(source_code))
                    return source_code[key_name]['content']
                else:
                    return '\n'.join(
                        source_code[key]['content'] 
                        for key in source_code 
                        if isinstance(source_code[key], dict) and 'content' in source_code[key]
                    )
        except Exception as e:
            logging.error(f"Source code parsing error: {e}")
            return None

    def classify_contract(self, source_code):
        try:
            # Truncate source code if too long
            source_code = self.parse_source_code(source_code)
            if not source_code:
                return None
            logging.info(f"Parsed source code: {source_code[:1000]}...")  # Log first 1000 characters for brevity
            result = xai_request(source_code)
            result_json = extract_and_convert_to_json(result)

            
                
            converted_result = {
                item["feature"]: {"value": item["value"], "reason": item["reason"]}
                for item in result_json
            }
            logging.info(f"Classification result: {converted_result}")

            # if all value is 0 , then it is not a scam
            if all(item["value"] == "no" for item in converted_result.values()):
                logging.info("Contract classified as not a scam.")
                return 0
            else:
                logging.info("Contract classified as a scam.")
                return 1


        except Exception as e:
            return None

    def update_token_table(self, conn, contract_address, is_scam):
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE tokens 
                SET smart_contract_verified = ? 
                WHERE contractAddress = ?
            """, (is_scam, contract_address))
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Error updating token table: {e}")
            conn.rollback()

    def run(self):
        logging.info("Starting Smart Contract Classifier")
        while True:
            conn = self.connect_db()
            if conn:
                try:
                    # Get unverified contracts
                    contracts = self.get_unverified_contracts(conn)
                    logging.info(f"Found {len(contracts)} unverified contracts to process.")
                    
                    for contract_address, source_code in contracts:
                        # Classify the contract
                        logging.info(f"Processing contract {contract_address}")
                        is_scam = self.classify_contract(source_code)
                        
                        if is_scam is not None:
                            # Update the token table
                            self.update_token_table(conn, contract_address, is_scam)
                            logging.info(f"Processed contract {contract_address}: {'Scam' if is_scam else 'Not Scam'}")
                
                finally:
                    conn.close()
            
            # Wait for 60 seconds before next iteration
            time.sleep(60)



if __name__ == "__main__":
    classifier = SmartContractClassifier(DB_PATH, log_file="../Real_Time_System/Log/smart_contracts_classifier.log")
    while True:
        try:
            classifier.run()
            logging.info("Waiting 5 minutes before next scrape...")
            time.sleep(300)  # 300 seconds = 5 minutes
        except KeyboardInterrupt:
            logging.info("Classifier stopped by user")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            time.sleep(300)  # 300 seconds = 5 minutes
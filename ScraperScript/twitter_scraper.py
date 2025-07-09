#%%

# #######################################################################################
# Twitter爬蟲: 獲取與代幣相關的 Twitter 數據
# #######################################################################################
# 此爬蟲的主要功能:
# 1. 從 tokens 表中獲取代幣關聯的 Twitter 用戶
# 2. 檢查這些用戶是否已存在於 twitter_users 表中
# 3. 獲取新用戶的資料信息並存儲
# 4. 獲取用戶的最新推文並存儲到 tweets 表
# 5. 分析推文中提及的其他賬戶
# 6. 定期更新用戶推文和檢查用戶狀態
# #######################################################################################

import time
import json
import requests
import sqlite3
from datetime import datetime
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor
from config import *
import schedule
import logging
import random
from functools import partial
import pandas as pd


class TweetDatabase:
    """
    Twitter數據庫交互類
    
    負責處理所有與 Twitter 用戶和推文相關的數據庫操作,
    包括存儲和檢索推文、用戶信息等
    """
    
    def __init__(self, db_path):
        """
        初始化 Twitter 數據庫連接
        
        參數:
            db_path: SQLite 數據庫文件路徑
        """
        self.db_path = db_path

    def get_connection(self):
        """
        建立並返回新的數據庫連接
        
        返回:
            SQLite 數據庫連接對象
        """
        return sqlite3.connect(self.db_path)

    def update_tweets(self, user_id: str, tweets: List[Dict[str, Any]]):
        """
        更新用戶的最新推文到數據庫
        
        參數:
            user_id: Twitter 用戶ID
            tweets: 包含推文信息的字典列表
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            query = """
            INSERT OR IGNORE INTO tweets (
                user_id, tweet_id, tweet_full_text, tweet_favorite_count, tweet_view_count, tweet_quote_count,
                tweet_reply_count, tweet_retweet_count, tweet_created_at, user_name, tweet_mention_list
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            for tweet in tweets:
                #print('User Name: ', tweet["user_name"])
                #print('Timestamp: ', tweet["tweet_created_at"])
                #print('Tweet full text: ', tweet["tweet_full_text"])
                #print('------------------------------------ \n')

                cursor.execute(query, (user_id, tweet["tweet_id"], tweet["tweet_full_text"], tweet["tweet_favorite_count"],tweet["tweet_view_count"],tweet["tweet_quote_count"],tweet["tweet_reply_count"],tweet["tweet_retweet_count"],tweet["tweet_created_at"],tweet["user_name"],tweet["tweet_mention_list"]))
                conn.commit()


        except sqlite3.Error as e:
            logging.info(f"Database error (update_tweets): {e}")

    def get_new_twitter_users_from_db(self) -> List[str]:
        """
        獲取需要添加的新 Twitter 用戶
        
        從 tokens 表中獲取尚未存儲在 twitter_users 表中的 Twitter 用戶
        
        返回:
            新 Twitter 用戶名列表
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            # SQL query to fetch Twitter URLs from the tokens table that are NOT in the twitter_user table
            query = """
            SELECT t.TwitterUser 
            FROM tokens t
            WHERE t.TwitterUser IS NOT NULL 
            AND NOT EXISTS (
                SELECT 1 FROM twitter_users u WHERE u.username = t.TwitterUser
            )
            """
            cursor.execute(query)
            twitter_user = cursor.fetchall()

            # Extract and clean Twitter usernames
            return list(set(twitter_user))
        
        except sqlite3.Error as e:
            logging.info(f"Database error (get_twitter_users_from_db): {e}")
            return []
        except Exception as e:
            logging.info(f"Error processing Twitter URLs: {e}")
            return []
    

    
    def get_available_twitter_users(self) -> List[str]:
        """
        獲取所有可用的 Twitter 用戶名
        
        返回標記為可用且有有效用戶ID的用戶名列表
        
        返回:
            可用 Twitter 用戶名列表
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
           
            query = "SELECT username FROM twitter_users WHERE available = 'True' AND user_id > 1000000000000000000" 
            cursor.execute(query)
            twitter_user = cursor.fetchall()

            # Extract and clean Twitter usernames
            return list(set(twitter_user))
        
        except sqlite3.Error as e:
            logging.info(f"Database error (get_available_twitter_users): {e}")
            return []
        except Exception as e:
            logging.info(f"Error processing Twitter URLs: {e}")
            return []
    
    def save_user_info(self, user_dict :Dict[str, Any]):
        """
        將用戶信息保存到數據庫
        
        參數:
            user_dict: 包含用戶信息的字典
        """

        query = """
        INSERT OR REPLACE INTO twitter_users (
            user_id, username, created_time, description, available
        ) VALUES (?, ?, ?, ?, ?)
        """
        try:
            # Execute the query with values from user_dict
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                query,
                (
                    user_dict.get("user_id"),
                    user_dict.get("username"),
                    user_dict.get("created_time"),
                    user_dict.get("description"),
                    "True"
                ),
            )
            conn.commit()
        except sqlite3.Error as e:
            logging.info(f"Database error (save_user_info): {e}")
        except Exception as ex:
            logging.info(f"Unexpected error (save_user_info): {ex}")

        
    def save_unavailable_user_info(self, username):
        """
        將不可用用戶信息保存到數據庫
        
        用於標記那些無法訪問或不存在的 Twitter 用戶
        
        參數:
            username: Twitter 用戶名
        """

        query = """
        INSERT OR REPLACE INTO twitter_users (
            user_id, username, created_time, description, available
        ) VALUES (?, ?, ?, ?, ?)
        """
        try:
            # Execute the query with values from user_dict
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                query,
                (   None,
                    username,
                    None,
                    None,
                    "False"
                ),
            )
            conn.commit()
        except sqlite3.Error as e:
            logging.info(f"Database error (save_unavailable_user_info): {e}")
        except Exception as ex:
            logging.info(f"Unexpected error (save_unavailable_user_info): {ex}")

    def upadte_unavailable_user_info(self, username):
        """
        將用戶標記為不可用
        
        參數:
            username: 要標記為不可用的 Twitter 用戶名
        """

        query = f"""
        UPDATE twitter_users
        SET  available = 'False'
        WHERE username = '{username}';
        """
        try:
            logging.info(username , ": ", query)
            # Execute the query with values from user_dict
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                query
            )
            conn.commit()

        except sqlite3.Error as e:
            logging.info(f"Database error (save_unavailable_user_info): {e}")
        except Exception as ex:
            logging.info(f"Unexpected error (save_unavailable_user_info): {ex}")

    def get_all_user_ids(self) -> List[Any]:
        """
        獲取所有需要收集推文的用戶ID
        
        返回:
            可用且有效的 Twitter 用戶ID列表
        """

        # query = "SELECT user_id FROM twitter_users WHERE available = 'True' AND user_id > 1000000000000000000 "
        query = """SELECT twitter_users.user_id
                FROM twitter_users
                JOIN Tokens
                ON twitter_users.username = Tokens.TwitterUser
                WHERE twitter_users.available = 'True'
                AND twitter_users.user_id > 1000000000000000000"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            user_ids = [row[0] for row in cursor.fetchall()]  # Extract the first column (user_id) from each row

            return user_ids
        except sqlite3.Error as e:
            logging.info(f"Database error (get_all_user_ids): {e}")
            return []
        except Exception as ex:
            logging.info(f"Unexpected error (get_all_user_ids): {ex}")
            return []



class TwitterScraper:
    """
    Twitter數據爬蟲類
    
    負責從 Twitter 網頁API獲取用戶信息和推文數據
    """

    def __init__(self, get_tweet_url: str, get_user_url:str, auth: List[List[Any]], get_tweet_features: str, get_user_features:str, tweet_fieldToggles: Optional[str] = None, user_fieldToggles: Optional[str] = None, log_file: str = "../Real_Time_System/Log/twitter_scraper.log"):
        """
        初始化 Twitter 爬蟲
        
        參數:
            get_tweet_url: 獲取推文的API URL
            get_user_url: 獲取用戶的API URL
            auth: 認證信息列表
            get_tweet_features: 獲取推文所需的功能參數
            get_user_features: 獲取用戶所需的功能參數
            tweet_fieldToggles: 推文字段切換參數
            user_fieldToggles: 用戶字段切換參數
            log_file: 日誌文件路徑
        """
        self.tweet_url = get_tweet_url
        self.user_url = get_user_url
        self.auth = auth
        self.get_tweet_features = get_tweet_features
        self.get_user_features = get_user_features
        self.tweet_fieldToggles = tweet_fieldToggles
        self.user_fieldToggles = user_fieldToggles

        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

    def build_get_tweets_params(self, user_id: str, count: int) -> Dict[str, Any]:
        """
        構建獲取用戶推文的API參數
        
        參數:
            user_id: Twitter用戶ID
            count: 要獲取的推文數量
            
        返回:
            API參數字典
        """
        variables = {
            "userId": user_id,
            "count": count,
            "includePromotedContent": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withVoice": True,
            "withV2Timeline": True
        }
        return {
            'variables': json.dumps(variables),
            'features': self.get_tweet_features,
            'fieldToggles': self.tweet_fieldToggles,
        }

    def build_get_tweets_params_page_x(self, user_id: str, count: int, cursor_value: str) -> Dict[str, Any]:
        """
        構建帶分頁功能的獲取用戶推文API參數
        
        用於獲取更多推文(超過第一頁)
        
        參數:
            user_id: Twitter用戶ID
            count: 要獲取的推文數量
            cursor_value: 分頁游標值
            
        返回:
            API參數字典
        """
        variables = {
            "userId": user_id,
            "count": count,
            "cursor": cursor_value,
            "includePromotedContent": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withVoice": True,
            "withV2Timeline": True
        }
        return {
            'variables': json.dumps(variables),
            'features': self.get_tweet_features,
            'fieldToggles': self.tweet_fieldToggles,
        }

    def build_get_user_params(self, screen_name: str) -> Dict[str, Any]:
        """
        構建獲取用戶信息的API參數
        
        參數:
            screen_name: Twitter用戶名(不含@)
            
        返回:
            API參數字典
        """
        variables = {
            "screen_name": screen_name,
        }
        return {
            'variables': json.dumps(variables),
            'features': self.get_user_features,
            'fieldToggles': self.user_fieldToggles,
        }
    
    def fetch(self, url:str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        通過Twitter API獲取數據
        
        使用隨機選擇的認證信息發送請求以避免限制
        
        參數:
            url: API URL
            params: 請求參數
            
        返回:
            API響應的JSON數據
        """
        try:

            auth_set = random.choice(self.auth)
            response = requests.get(url, params=params, cookies=auth_set[0], headers=auth_set[1])
            response.raise_for_status()

            return response.json()
        except requests.exceptions.RequestException as e:
            logging.info(f"Error fetching tweets (fetch): {e}")
            
            return {}
    
    
    def get_user(self, screen_name: str, db: TweetDatabase) -> Optional[str]:
        """
        獲取Twitter用戶信息並存儲到數據庫
        
        參數:
            screen_name: Twitter用戶名
            db: TweetDatabase實例
            
        返回:
            成功時返回用戶ID,失敗時返回None
        """
        params = self.build_get_user_params(screen_name)

        try:
            response_json = self.fetch(self.user_url, params)
            
            # Extract user ID from the response
            if response_json['data'] != {}: # Check if the response is not empty
                
                user_result = response_json['data']['user']['result']


                self.process_user_response(user_result, screen_name, db)
            if response_json['data'] == {}:
                db.save_unavailable_user_info(screen_name)

        except requests.exceptions.RequestException as e:
            logging.info(f"HTTP request error (get_user): {e}")
        except KeyError as e:
            logging.info(f"Error parsing response (get_user): {e}")

        return None
    
    def check_user(self, screen_name: str, db: TweetDatabase) -> Optional[str]:
        """
        檢查Twitter用戶是否仍然可用
        
        參數:
            screen_name: Twitter用戶名
            db: TweetDatabase實例
        """
        params = self.build_get_user_params(screen_name)

        try:
            response_json = self.fetch(self.user_url, params)
            
            if response_json['data'] == {}:
                db.upadte_unavailable_user_info(screen_name)

        except requests.exceptions.RequestException as e:
            logging.info(f"HTTP request error (check_user): {e}")
        except KeyError as e:
            logging.info(f"Error parsing response (check_user): {e}")


    
    def process_user_response(self, user_result: dict, username:str, db: TweetDatabase) -> Optional[Dict[str, Any]]:
        """
        處理用戶數據API響應並保存到數據庫
        
        參數:
            user_result: 用戶API響應
            username: Twitter用戶名
            db: TweetDatabase實例
            
        返回:
            成功時返回用戶數據字典,失敗時返回None
        """
        try:
            if "message" in user_result:
                db.save_unavailable_user_info(username)
                return None
            else:
                db.save_user_info({
                    "user_id": user_result['rest_id'],
                    "username": username,
                    "created_time":  user_result['legacy']['created_at'],
                    "description": user_result['legacy']['description'],
                })
                                
        except KeyError as e:
            logging.info(f"Error parsing response (get_user): {e}")


    def get_latest_tweets(self, user_id: str, count: int = 30) -> List[Dict[str, Any]]:
        """
        獲取用戶最新的推文
        
        參數:
            user_id: Twitter用戶ID
            count: 要獲取的推文數量
            
        返回:
            推文數據字典列表
        """
        params = self.build_get_tweets_params(user_id, count)
        response_json = self.fetch(self.tweet_url, params)

        try:
            if response_json == {}:
                logging.info(f"No tweets found for user ID: {user_id}")
                return [] 
            response_entries = response_json['data']['user']['result']['timeline_v2']['timeline']['instructions'][-1]['entries']

            return self.process_tweet_response(response_entries)
        except (KeyError, IndexError) as e:
            logging.info(f"Error processing response (get_latest_tweets) ({user_id}): {e}")

            return []

    def get_all_tweets(self, user_id: str, max_results:int) -> List[Dict[str, Any]]:
        """
        獲取用戶所有推文(最多到max_results個)
        
        使用分頁功能獲取多頁推文
        
        參數:
            user_id: Twitter用戶ID
            max_results: 最大推文數量
            
        返回:
            推文數據字典列表
        """
        import math
        round_count = math.ceil(max_results / 20)

        params = self.build_get_tweets_params(user_id, 30)
        response_json = self.fetch(self.tweet_url, params)

        try:
            if response_json == {}:
                logging.info(f"No tweets found for user ID: {user_id}")
                return [] 
            response_entries = response_json['data']['user']['result']['timeline_v2']['timeline']['instructions'][-1]['entries']
            cursor_value = response_entries[-1]['content']['value']

            tweets_list = self.process_tweet_response(response_entries)

            for _ in range(round_count - 1):
                params = self.build_get_tweets_params_page_x(user_id, 30, cursor_value)
                response_json = self.fetch(self.tweet_url, params)
                response_entries = response_json['data']['user']['result']['timeline_v2']['timeline']['instructions'][-1]['entries']
                if len(response_entries) < 5:
                    break
                cursor_value = response_entries[-1]['content']['value']
                for i  in self.process_tweet_response(response_entries):
                    tweets_list.append(i)
                
                # each 5 round sleep 5 seconds
                if _ % 5 == 0:
                    time.sleep(5)

                if _ % 10 == 0:
                    time.sleep(60)

            logging.info(f"Total tweets fetched for user ID {user_id}: {len(tweets_list)}")
            return tweets_list
        
        except (KeyError, IndexError) as e:
            logging.info(f"Error processing response (get_all_tweets) ({user_id}): {e}")

            return []
    

    def parse_tweet(self, tweet_id, tweet_results: dict) -> Optional[Dict[str, Any]]:
        """
        解析單個推文數據
        
        從API響應中提取推文的關鍵信息
        
        參數:
            tweet_id: 推文ID
            tweet_results: 推文API響應
            
        返回:
            解析後的推文數據字典或None
        """
        
        try:
            if tweet_results is None or 'legacy' not in tweet_results or 'core' not in tweet_results:
                logging.info(f"Invalid tweet results for tweet ID : {tweet_id}")
                return None
            
            tweet_content = tweet_results['legacy']
            user_content = tweet_results['core']['user_results']['result']['legacy']

            return {
                "tweet_id": tweet_id,
                "tweet_full_text": tweet_content['full_text'],
                "tweet_favorite_count": tweet_content['favorite_count'],
                "tweet_view_count": tweet_results.get('views', {}).get('count', 0),
                "tweet_quote_count": tweet_content['quote_count'],
                "tweet_reply_count": tweet_content['reply_count'],
                "tweet_retweet_count": tweet_content['retweet_count'],
                "tweet_created_at": datetime.strptime(tweet_content['created_at'], "%a %b %d %H:%M:%S %z %Y").isoformat(),
                "user_name": user_content['name'],
                "tweet_mention_list": json.dumps(
                    {mention['screen_name']: mention['name'] for mention in tweet_content['entities']['user_mentions']},
                    ensure_ascii=False
                ),
            }
        except (KeyError, ValueError) as e:
            logging.info(f"Error parsing tweet (parse_tweet): {e}")

            logging.info('tweet_results : ', tweet_results)

            return None

    def process_tweet_response(self, response_entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        處理API響應中的推文條目
        
        過濾並解析API響應中的推文數據
        
        參數:
            response_entries: API響應中的條目列表
            
        返回:
            解析後的推文數據字典列表
        """
        tweets = []
        for entry in response_entries:
            entry_id = entry.get('entryId', '')
            if "who-to-follow" in entry_id:
                continue  # Skip "who-to-follow" entries
    
            if 'profile-conversation' in entry['entryId']:
                try:
                    parsed_tweet = self.parse_tweet(entry_id, entry['content']['items'][0]['item']['itemContent']['tweet_results']['result']) 
                except KeyError as e:
                    logging.info(f"Error parsing tweet (profile-conversation): {e}")
                    continue
            if "tweet" in entry['entryId']:
                # Extract tweet and user content
                try:
                    parsed_tweet = self.parse_tweet(entry_id, entry['content']['itemContent']['tweet_results']['result'])
                except KeyError as e:
                    logging.info(f"Error parsing tweet (tweet): {e}")
                    continue
            if parsed_tweet:
                tweets.append(parsed_tweet)
        return tweets
    
    def update_new_twitter_users(self, db: TweetDatabase):
        """
        更新新發現的Twitter用戶信息
        
        獲取並保存數據庫中新發現的Twitter用戶
        
        參數:
            db: TweetDatabase實例
        """
        try:
            logging.info("Updating new Twitter users...")    
            twitter_usernames = db.get_new_twitter_users_from_db()


            if twitter_usernames == []:
                logging.info("No new Twitter users to update.")
            else:
                for username in twitter_usernames:
                    logging.info(username[0])
                    self.get_user(username[0], db)
                    time.sleep(5)
            

            
        except KeyboardInterrupt:
            logging.info("Updating new Twitter users interrupted by user.")

    def check_twitter_users(self, db: TweetDatabase):
        """
        檢查現有Twitter用戶的可用性
        
        驗證已知用戶是否仍然可訪問
        
        參數:
            db: TweetDatabase實例
        """
        try:
            logging.info("Checking users...")    
            twitter_usernames = db.get_available_twitter_users()

            if twitter_usernames == []:
                logging.info("No new Twitter users to update.")
            else:
                for username in twitter_usernames:
                    logging.info(username[0])
                    self.check_user(username[0], db)
                    time.sleep(5)
            
        except KeyboardInterrupt:
            logging.info("Updating new Twitter users interrupted by user.")

    
    def scrape_tweets_periodically(self, db: TweetDatabase):
        """
        定期抓取所有用戶的推文
        
        從所有可用用戶獲取最新推文並更新數據庫
        
        參數:
            db: TweetDatabase實例
        """
        

        logging.info("Scraping tweets periodically...")
        user_ids = db.get_all_user_ids()
        for user_id in user_ids:
            latest_tweets = self.get_latest_tweets(user_id, 30)
            
            db.update_tweets(user_id, latest_tweets)
            logging.info(f"Updating tweets for user ID: {user_id}")
            time.sleep(5)





    def start(self, db: TweetDatabase):
        """
        啟動Twitter爬蟲的定期任務
        
        設置定期抓取推文和更新用戶的計劃任務
        
        參數:
            db: TweetDatabase實例
        """
        logging.info("Starting the scheduler...")
        logging.info("Scheduler started. Running tasks...")

        # Schedule tasks
        schedule.every(1).minutes.do(lambda: self.update_new_twitter_users(db))
        schedule.every(2).minutes.do(lambda: self.check_twitter_users(db))
        schedule.every(5).minutes.do(lambda: self.scrape_tweets_periodically(db))
        
        # Run the scheduler loop
        while True:
            schedule.run_pending()
            time.sleep(1)  # Prevent busy-waiting


def main():
    # Set database path and API configurations
    twitter_scraper = TwitterScraper(get_tweet_url, get_user_url, auth, get_tweet_features, get_user_features, tweet_fieldToggles, user_fieldToggles)
    db = TweetDatabase(DB_PATH)
    twitter_scraper.start(db)


if __name__ == "__main__":
    main()

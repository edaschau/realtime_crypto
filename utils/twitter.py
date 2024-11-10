from datetime import datetime, timezone, timedelta
import time
import requests
import logging
import params
import sqlite3
from utils.database import store_twitter_data
import params

TWITTER_USERNAMES = params.get_twitter_usernames()
SEARCH_URL = params.get_search_url()
DB_NAME = params.get_db_name()
MODELS = params.get_models()
CRYPTO_KEYWORDS = params.get_crypto_keywords()

from utils.sentimemt import get_model_responses
from keys.twitter import bearer_token
LAST_API_CALL = {}

def bearer_oauth(r):
    bearer = bearer_token()
    r.headers["Authorization"] = f"Bearer {bearer}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


def connect_to_endpoint(url, params):
    max_retries = 5
    retry_delay = 60  # seconds

    for attempt in range(max_retries):
        try:
            response = requests.get(url, auth=bearer_oauth, params=params)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                logging.warning(f"Rate limit hit. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise Exception(response.status_code, response.text)
        except Exception as e:
            logging.error(f"Error connecting to Twitter API: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(retry_delay)

    raise Exception("Max retries reached. Unable to connect to Twitter API.")

def is_within_time_window():
    now = datetime.now(timezone.utc)
    return 55 <= now.minute <= 59

def should_scrape_twitter(crypto_name):
    global LAST_API_CALL
    now = datetime.now(timezone.utc)
    last_whole_hour = now.replace(minute=0, second=0, microsecond=0)
    
    if crypto_name not in LAST_API_CALL:
        return True
    if LAST_API_CALL[crypto_name] < last_whole_hour:
        return True
    return is_within_time_window()

def get_twitter_data(crypto_name):
    global LAST_API_CALL
    now = datetime.now(timezone.utc)
    end_time = now - timedelta(seconds=10)
    
    if crypto_name in LAST_API_CALL:
        start_time = LAST_API_CALL[crypto_name]
    else:
        # If no previous API call, fetch for the last 24 hours
        start_time = end_time - timedelta(hours=24)
    
    start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    tweets = []
    for username in TWITTER_USERNAMES[crypto_name]:
        query_params = {
            'query': f'from:{username} {crypto_name.lower()}',
            'start_time': start_time_str,
            'end_time': end_time_str,
            'expansions': 'author_id',
            'tweet.fields': 'created_at,text'
        }
        json_response = connect_to_endpoint(SEARCH_URL, query_params)
        if 'data' in json_response:
            tweets.extend(json_response['data'])
    
    # Update the last API call time
    LAST_API_CALL[crypto_name] = now
    
    return tweets

def process_twitter_data():
    for crypto_name in CRYPTO_KEYWORDS.keys():
        if not should_scrape_twitter(crypto_name):
            logging.info(f"Not time to scrape Twitter for {crypto_name}. Skipping.")
            return

        tweets = get_twitter_data(crypto_name)
        if not tweets:
            logging.info(f"No new tweets found for {crypto_name}.")
            return

        # Store original Twitter data
        store_twitter_data(tweets, crypto_name)

        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        
        for model in MODELS:
            table_name = f"{crypto_name}_{model['name'].replace('/', '_').replace('-', '_').replace('.', '_')}_twitter"
            
            for tweet in tweets:
                sentiment = get_model_responses(f"Tweet: {tweet['text']}", model, crypto_name, is_twitter=True)
                
                columns = ', '.join([f'"{key.lower().replace(" ", "_")}"' for key in sentiment.keys()])
                placeholders = ', '.join(['?' for _ in sentiment])
                values = tuple(sentiment.values())
                
                cur.execute(f"""
                    INSERT OR REPLACE INTO "{table_name}" 
                    (tweet_id, author_id, text, created_at, {columns})
                    VALUES (?, ?, ?, ?, {placeholders})
                """, (tweet['id'], tweet['author_id'], tweet['text'], tweet['created_at'], *values))
                
                logging.info(f"Processed and stored {crypto_name} tweet for model {model['name']}: {tweet['id']}")
        
        conn.commit()
        conn.close()

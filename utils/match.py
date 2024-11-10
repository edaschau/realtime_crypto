import json
import logging
import sqlite3

from utils.sentimemt import get_model_responses
from utils.twitter import connect_to_endpoint
import params

SEARCH_URL = params.get_search_url()


def get_crypto_type(title, crypto_keywords):
    title_lower = title.lower()
    for crypto, keywords in crypto_keywords.items():
        if any(keyword in title_lower for keyword in keywords):
            return crypto
    return None



def process_article(url, title, content, db_name, datetime_utc, models):
    crypto_type = get_crypto_type(title)
    if crypto_type:
        conn = sqlite3.connect(db_name)
        cur = conn.cursor()
        
        for model in models:
            try:
                sentiment = get_model_responses(f"Title: {title}\n\nContent: {content}", model, crypto_type, is_twitter=False)
                
                # Sanitize the table name
                table_name = f"{crypto_type}_{model['name'].replace('/', '_').replace('-', '_').replace('.', '_')}_news"
                
                # Convert aspect names to lowercase for column names
                columns = ', '.join([f'"{key.lower().replace(" ", "_")}"' for key in sentiment.keys()])
                placeholders = ', '.join(['?' for _ in sentiment])
                values = tuple(sentiment.values())
                
                cur.execute(f"""
                    INSERT OR REPLACE INTO "{table_name}" 
                    (url, {columns})
                    VALUES (?, {placeholders})
                """, (url, *values))
                
                logging.info(f"Processed and stored {crypto_type} article for model {model['name']}: {url}")
            except Exception as e:
                logging.error(f"Error processing article {url} for model {model['name']}: {e}")
        
        conn.commit()
        conn.close()
    else:
        logging.info(f"Article not related to tracked cryptocurrencies: {url}")

from datetime import datetime, timezone, timedelta

# Add this global variable to track the last API call time for each crypto
LAST_API_CALL = {}

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

def get_twitter_data(crypto_name, twitter_usernames):
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
    for username in twitter_usernames[crypto_name]:
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

def store_twitter_data(tweets, crypto_name, db_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    for tweet in tweets:
        cur.execute("""
            INSERT OR REPLACE INTO twitter_data
            (tweet_id, crypto_name, author_id, text, created_at, json_data)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            tweet['id'],
            crypto_name,
            tweet['author_id'],
            tweet['text'],
            tweet['created_at'],
            json.dumps(tweet)
        ))
    
    conn.commit()
    conn.close()

def process_twitter_data(crypto_name, db_name, models):
    if not should_scrape_twitter(crypto_name):
        logging.info(f"Not time to scrape Twitter for {crypto_name}. Skipping.")
        return

    tweets = get_twitter_data(crypto_name)
    if not tweets:
        logging.info(f"No new tweets found for {crypto_name}.")
        return

    # Store original Twitter data
    store_twitter_data(tweets, crypto_name)

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    for model in models:
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


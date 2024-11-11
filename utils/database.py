from datetime import datetime, timezone
import json
import logging
import sqlite3
import time
from dateutil import parser
import params

DB_NAME = params.get_db_name()
CRYPTO_KEYWORDS = params.get_crypto_keywords()
MODELS = params.get_models()

NEWS_PROMPTS = params.get_news_prompts()
TWITTER_PROMPTS = params.get_twitter_prompts()

def get_unscraped_links():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT url FROM links WHERE is_scraped = 0")
    links = [row[0] for row in cur.fetchall()]
    conn.close()
    return links

def initialize_database():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    # Create links and articles tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS links (
            url TEXT PRIMARY KEY,
            first_seen_utc TIMESTAMP,
            first_seen_unix INTEGER,
            is_scraped INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            url TEXT PRIMARY KEY,
            title TEXT,
            author TEXT,
            datetime_utc TIMESTAMP,
            datetime_unix INTEGER,
            content TEXT,
            ticker_symbols TEXT,
            FOREIGN KEY (url) REFERENCES links (url)
        )
    """)
    
    # Create tables for each crypto type and model (for news)
    for crypto in CRYPTO_KEYWORDS.keys():
        for model in MODELS:
            table_name = f"{crypto}_{model['name'].replace('/', '_').replace('-', '_').replace('.', '_')}_news"
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    url TEXT PRIMARY KEY,
                    FOREIGN KEY (url) REFERENCES articles (url)
                )
            """)
            
            # Add columns for each prompt dynamically
            for prompt in NEWS_PROMPTS:
                column_name = prompt['aspect'].lower().replace(' ', '_')
                try:
                    cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" INTEGER')
                except sqlite3.OperationalError:
                    pass
    
    # Create Twitter tables for each crypto type and model
    for crypto in CRYPTO_KEYWORDS.keys():
        for model in MODELS:
            table_name = f"{crypto}_{model['name'].replace('/', '_').replace('-', '_').replace('.', '_')}_twitter"
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    tweet_id TEXT PRIMARY KEY,
                    author_id TEXT,
                    text TEXT,
                    created_at TIMESTAMP
                )
            """)
            
            # Add columns for each prompt dynamically
            for prompt in TWITTER_PROMPTS:
                column_name = prompt['aspect'].lower().replace(' ', '_')
                try:
                    cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" INTEGER')
                except sqlite3.OperationalError:
                    pass
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS twitter_data (
            tweet_id TEXT PRIMARY KEY,
            crypto_name TEXT,
            author_id TEXT,
            text TEXT,
            created_at TIMESTAMP,
            json_data TEXT
        )
    """)
    
    conn.commit()
    conn.close()

def update_new_links(found_links):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    now = datetime.now(timezone.utc)
    now_unix = int(now.timestamp())
    
    new_links = []
    for link in found_links:
        if ("/news/" in link) and (".html" in link) and ("https:" in link):
            cur.execute("""
                INSERT OR IGNORE INTO links (url, first_seen_utc, first_seen_unix)
                VALUES (?, ?, ?)
            """, (link, now, now_unix))
            if cur.rowcount > 0:
                logging.info(f"Added new link: {link}")
                new_links.append(link)
    
    conn.commit()
    conn.close()
    
    logging.info(f"Link scraping completed at {now}")
    return new_links

def store_article(url, article_data):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    title = str(article_data.get('title')) if article_data.get('title') is not None else None
    author = str(article_data.get('author')) if article_data.get('author') is not None else None
    datetime_str = str(article_data.get('datetime')) if article_data.get('datetime') is not None else None
    content = str(article_data.get('article')) if article_data.get('article') is not None else None
    ticker_symbols = json.dumps(article_data.get('ticker_symbols')) if article_data.get('ticker_symbols') is not None else None
    
    datetime_utc = None
    datetime_unix = None
    if datetime_str:
        try:
            parsed_date = parser.parse(datetime_str)
            datetime_utc = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            datetime_unix = int(time.mktime(parsed_date.timetuple()))
        except ValueError:
            logging.warning(f"Invalid date format for URL {url}: {datetime_str}")    
    
    cur.execute("""
        INSERT OR REPLACE INTO articles (url, title, author, content, datetime_utc, datetime_unix, ticker_symbols)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (url, title, author, content, datetime_utc, datetime_unix, ticker_symbols))
    
    cur.execute("UPDATE links SET is_scraped = 1 WHERE url = ?", (url,))
    
    conn.commit()
    conn.close()

    return title, content, datetime_utc

def store_twitter_data(tweets, crypto_name):
    conn = sqlite3.connect(DB_NAME)
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
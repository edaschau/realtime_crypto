from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup
import logging

from utils.database import update_new_links, store_article, get_unscraped_links

from bs4 import BeautifulSoup
import logging
import sqlite3

from utils.database import update_new_links, store_article, get_unscraped_links
from utils.browser import initialize_browser, handle_cookie_consent
import params

URL = params.get_news_url()
EXTRACTORS = params.get_extractors()
CRYPTO_KEYWORDS = params.get_crypto_keywords()
DB_NAME = params.get_db_name()
MODELS = params.get_models()

from utils.sentimemt import get_model_responses

def scrape_and_store_links(driver, url, link_extractor):
    new_links = []
    try:
        found_links = link_extractor.extract_links(driver, url)
        new_links = update_new_links(found_links)
    except Exception as e:
        logging.error(f"Error getting new links for {url} : {e}")
    return new_links

def get_and_store_article(driver, url, article_extractor):
    try:
        driver.get(url)
        handle_cookie_consent(driver)
        wait = WebDriverWait(driver, 3)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        data = article_extractor.extract_article_data(soup)
        store_article(url, data)
        return data
    except Exception as e:
        logging.error(f"Unexpected error during extraction for URL {url}: {e}")
        return None
    

def get_crypto_type(title):
    matched_cryptos = set()  # Using set to avoid duplicates
    text_to_check = title.lower()
    
    for crypto, keywords in CRYPTO_KEYWORDS.items():
        if any(keyword in text_to_check for keyword in keywords):
            matched_cryptos.add(crypto)
            
    return list(matched_cryptos)


def process_article(url,article_data):
    crypto_types = get_crypto_type(article_data["title"])
    if crypto_types:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        for crypto_type in crypto_types:
            for model in MODELS:
                try:
                    sentiment = get_model_responses(f"Title: {article_data["title"]}\n\nContent: {article_data["article"]}", model, crypto_type, is_twitter=False)
                    
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
                    conn.commit()
                    
                    logging.info(f"Processed and stored {crypto_type} article for model {model['name']}: {url}")
                except Exception as e:
                    logging.error(f"Error processing article {url} for model {model['name']}: {e}")
            
                
        conn.close()
    else:
        logging.info(f"Article not related to tracked cryptocurrencies: {url}")

def get_new_articles(driver):
    link_extractor = EXTRACTORS['link']
    article_extractor = EXTRACTORS['article']
    scrape_and_store_links(driver, URL, link_extractor)

    articles_data = {}
    new_links = get_unscraped_links()
    for link in new_links:
        articles_data[link] = get_and_store_article(driver, link, article_extractor)
        process_article(link, articles_data[link])

    return articles_data



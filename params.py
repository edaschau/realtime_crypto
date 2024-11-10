import csv

def get_db_name():
# Database configuration
    return 'crypto.db'

def get_news_url():
    return "https://finance.yahoo.com/topic/crypto/"



from importlib import import_module
def get_extractors():
        return {
            "article" : import_module("extractors.articles.yfin"),
            "link" : import_module("extractors.links.yfin"),
        }

# Models configuration
def get_models():
    return [
        {
            "name": "google/gemma-2-9b-it",
            "params": {
                "temperature": 0.1,
                "top_p": 0.9,
                "repetition_penalty": 0.5,
                "top_k": 40,
            }
        },
        {
            "name": "meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo",
            "params": {
                "temperature": 0.1,
                "top_p": 0.9,
                "repetition_penalty": 0.5,
                "top_k": 40,
            }
        },
    ]

# Crypto keywords
def get_crypto_keywords():
    return {
        'Bitcoin': ['bitcoin', 'btc'],
        'Ethereum': ['ethereum', 'eth'],
        'Dogecoin': ['dogecoin', 'doge']
    }

def get_search_url():
    return "https://api.twitter.com/2/tweets/search/recent"

def get_twitter_usernames():
    return {
        'Bitcoin': ['elonmusk', 'saylor', 'cz_binance', 'jack', 'coinbase'],
        'Ethereum': ['binance', 'coinbase', 'ethereum', 'VitalikButerin', 'arbitrum'],
        'Dogecoin': ['elonmusk', 'MattWallace888', 'mcuban', 'SnoopDogg', 'coinbase']
    }

def load_prompts(file_name):
    with open(file_name, 'r') as f:
        reader = csv.DictReader(f)
        return [{k.lower().replace(' ', '_'): v for k, v in row.items()} for row in reader]

def get_news_prompts():
    return load_prompts('news_prompts.csv')

def get_twitter_prompts():
    return load_prompts('twitter_prompts.csv')
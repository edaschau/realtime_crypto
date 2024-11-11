import sqlite3
import time
from datetime import datetime, timezone, timedelta
import json
from dateutil import parser
import logging
import random

# from utils.browser import initialize_browser
from utils.scrape import get_new_articles
from utils.database import initialize_database
from utils.sentimemt import get_model_responses
from utils.twitter import process_twitter_data
from utils.analysis import calculate_hourly_averages
import params

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import os
import csv
import sys

from selenium.webdriver.firefox.service import Service

def initialize_browser():
    # Get the project root directory (two levels up from browser.py)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Path to geckodriver in the main directory
    geckodriver_path = os.path.join(project_root, 'geckodriver')
    
    options = Options()
    # options.add_argument('-headless')
    options.set_preference('permissions.default.image', 2)
    options.set_preference('javascript.enabled', False)
    options.set_preference('media.autoplay.default', 5)
    options.set_preference('media.volume_scale', '0.0')
    
    service = Service(executable_path='geckodriver')
    return webdriver.Firefox(service=service, options=options)

def main():
    initialize_database()
    driver = initialize_browser()
    # service = Service(executable_path="geckodriver")

    try:
        while True:
            get_new_articles(driver)
            # process_twitter_data()
            calculate_hourly_averages()
            time.sleep(60)


    except KeyboardInterrupt:
        logging.info("Scraper manually terminated.")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()

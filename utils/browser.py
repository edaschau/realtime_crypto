# utils/browser.py
import os
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

def initialize_browser():
    current_dir = os.getcwd()
    
    # Path to geckodriver in the current directory
    geckodriver_path = os.path.join(current_dir, 'geckodriver')

    options = Options()
    # options.add_argument('-headless')
    options.set_preference('permissions.default.image', 2)
    options.set_preference('javascript.enabled', False)
    options.set_preference('media.autoplay.default', 5)
    options.set_preference('media.volume_scale', '0.0')
    
    service = Service(executable_path=geckodriver_path)
    return webdriver.Firefox(service=service, options=options)

def handle_cookie_consent(driver):
    try:
        wait = WebDriverWait(driver, 5)
        reject_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[name='reject']")))
        reject_button.click()
        logging.info("Cookie consent handled.")
    except Exception as e:
        logging.warning(f"Failed to handle cookie consent: {e}")
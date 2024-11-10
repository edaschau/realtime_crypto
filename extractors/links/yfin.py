from utils.browser import handle_cookie_consent
# ... rest of your yfin.py code
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import logging

def extract_links(driver, url):
    driver.get(url)
    handle_cookie_consent(driver)
    wait = WebDriverWait(driver, 3)
    wait.until(EC.presence_of_element_located((By.ID, "Fin-Stream")))

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    fin_stream = soup.find('div', id='Fin-Stream')
    if fin_stream:
        links = [a['href'] for a in fin_stream.find_all('a', href=True)]
    else:
        logging.warning("Fin-Stream div not found")
        links = []

    return links
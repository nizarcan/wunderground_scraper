from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import config as config
import threading

def create_new_driver(headless=True):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument('--log-level=3')
    driver = webdriver.Chrome(options=chrome_options)
    driver.get("https://www.wunderground.com/")
    celcius_button = driver.find_elements_by_xpath("""//*[@id="wuSettings"]""")[0]
    celcius_button.click()
    celcius_button = driver.find_elements_by_xpath("""//*[@id="wuSettings-quick"]/div/a[2]""")[0]
    celcius_button.click()
    return driver

def add_to_queue(queue, city):
    queue[city] = create_new_driver()

def create_driver_queue():
    print("\nInitiating chromedriver instances...")
    driver_queue = {}
    threads = []
    for city in config.WEATHER_URL_MAP.keys():
        x = threading.Thread(target=add_to_queue, args=(driver_queue, city), daemon=True)
        threads.append(x)
        x.start()

    for thread in threads:
        thread.join()
    
    return driver_queue

driver_queue = create_driver_queue()

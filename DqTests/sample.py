import time
from selenium import webdriver
from dotenv import load_dotenv

driver = webdriver.Chrome()
driver.get("https://www.google.com/")
time.sleep(5)

load_dotenv()
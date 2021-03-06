from selenium import webdriver
from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
# driver就是当前浏览器窗口
driver = webdriver.Chrome(chrome_options=chrome_options)

# 获取当前打开的网页html内容
html = driver.page_source
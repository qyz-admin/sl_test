import requests
import os
from excelControl import ExcelControl
from mysqlControl import MysqlControl
from wlMysql import WlMysql
from wlExecl import WlExecl
import datetime

url="https://pro.jd.com/mall/active/4BNKTNkRMHJ48QQ5LrUf6AsydtZ6/index.html"
try:
    r = requests.get(url)
    r.raise_for_status()
    r.encoding=r.apparent_encoding
    print(r.text[:100])
except:
    print("爬取失败")




if __name__=="__main__":
    response = requests.get("https://book.douban.com/subject/26986954/")
    content = response.content.decode("utf-8")
    print(content)
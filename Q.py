import os
from excelControl import ExcelControl
from mysqlControl import MysqlControl
from wlMysql import WlMysql
from wlExecl import WlExecl
import datetime
import requests

start = datetime.datetime.now()	
# team = 'slgat'
# match = {'slrb': r'D:\Users\Administrator\Desktop\查询订单',
# 		'sltg': r'D:\Users\Administrator\Desktop\查询订单',
# 		'slgat': r'D:\Users\Administrator\Desktop\查询订单',
# 		'slxmt': r'D:\Users\Administrator\Desktop\查询订单'}

# path = match[team]
# dirs = os.listdir(path=path)

# for dir in dirs:
# 	filePath = os.path.join(path, dir)
# 	print(filePath)
# 	if dir[:2] != '~$':
# 		# e.readExcel(filePath, team)
# 		print('导入耗时：', datetime.datetime.now() - start)

# ================时间练习=================
yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + ' 23:59:59'
print(yesterday)

print(yesterday[5:7])
last_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-01'
print(last_month)
print(last_month[5:7])
print('正在获取 ' + last_month[5:7] + '-' + yesterday[5:7] +' 月订单…………')


# ===============请求练习=================
r = requests.get('https://www.baidu.com') 
print(r.status_code)   
print(r.encoding)
print(r.text)  

print(r.headers)   
r.encoding = 'utf-8'
print(r.encoding)
print(r.content )
print(r.text)  


payload = {'key1': 'value1', 'key2': 'value2'}
r = requests.get("http://httpbin.org/get", params=payload)
print(r.url)  
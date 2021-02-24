import os
from excelControl import ExcelControl
from mysqlControl import MysqlControl
from wlMysql import WlMysql
from wlExecl import WlExecl
import datetime

start = datetime.datetime.now()
team = 'slgat'
match = {'slrb': r'D:\Users\Administrator\Desktop\查询订单',
		'sltg': r'D:\Users\Administrator\Desktop\查询订单',
		'slgat': r'D:\Users\Administrator\Desktop\查询订单',
		'slxmt': r'D:\Users\Administrator\Desktop\查询订单'}

path = match[team]
dirs = os.listdir(path=path)

e = ExcelControl()
m = MysqlControl()
w = WlMysql()
we = WlExecl()

for dir in dirs:
	filePath = os.path.join(path, dir)
	print(filePath)
	if dir[:2] != '~$':
		e.queryExecl(filePath, team)
		print('导入耗时：', datetime.datetime.now() - start)
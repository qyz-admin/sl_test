import os
from wlExecl0 import WlExecl
from wlMysql0 import WlMysql
from excelControl0 import ExcelControl
from mysqlControl0 import MysqlControl
import datetime
start = datetime.datetime.now()
print(start)
team = 'slgat'
# match = {'slrbwl': r'D:\Users\Administrator\Desktop\需要用到的文件\物流',
# 		'sltgwl': r'D:\Users\Administrator\Desktop\需要用到的文件\物流',
# 		'slgatwl': r'D:\Users\Administrator\Desktop\需要用到的文件\物流',
# 		'slxmtwl': r'D:\Users\Administrator\Desktop\需要用到的文件\物流\新马物流'}
match = {'slrb': r'D:\Users\Administrator\Desktop\需要用到的文件\日本签收表',
		'sltg': r'D:\Users\Administrator\Desktop\需要用到的文件\泰国签收表',
		'slgat': r'D:\Users\Administrator\Desktop\需要用到的文件\港台签收表',
		'slxmt': r'D:\Users\Administrator\Desktop\需要用到的文件\新马签收表'}
path = match[team]
dirs = os.listdir(path=path)
e = ExcelControl()
m = MysqlControl()
w = WlMysql()
we = WlExecl()
# ---读取execl文件---
for dir in dirs:
	filePath = os.path.join(path, dir)
	print(filePath)
	if dir[:2] != '~$':
		if dir[:6] == 'GIIKIN' or dir[:6] == 'Giikin':
			print('98')
			we.logisitis(filePath, team)
		else:
			print('02')
			e.readExcel(filePath, team)
print('导入耗时：', datetime.datetime.now() - start)
# ---数据库读取---
# m.creatMyOrder(team)
# print('处理耗时：', datetime.datetime.now() - start)
# if team == 'slxmt':
# 	w.exportOrder(team)
# else:
# 	m.connectOrder(team)
# 	print('输出耗时：', datetime.datetime.now() - start)
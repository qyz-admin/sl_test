import os
from excelControl import ExcelControl
from mysqlControl import MysqlControl
from wlMysql import WlMysql
from wlExecl import WlExecl
import datetime
start = datetime.datetime.now()	
team = 'slrb'
match = {'slrb': r'D:\Users\Administrator\Desktop\需要用到的文件\日本签收表',
		'sltg': r'D:\Users\Administrator\Desktop\需要用到的文件\泰国签收表',
		'slgat': r'D:\Users\Administrator\Desktop\需要用到的文件\港台签收表',
		'slxmt': r'D:\Users\Administrator\Desktop\需要用到的文件\新马签收表'}
'''
备注说明：
港台 需整理的表：香港顺航8月>(出货明细再copy一份保存) ；
				台湾龟山改派>(copy保存为xlsx格式);
				香港易速配顺丰>(总明细copy保存为xlsx格式);
日本 需整理的表：吉客印神龙直发签收表>(明细再copy一份保存；   改派明细不需要);
'''
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
		if dir[:6] == 'GIIKIN' or dir[:6] == 'Giikin':  # 新马物流的条件
			print('98')
			we.logisitis(filePath, team)
		else:
			print('02')
			e.readExcel(filePath, team)
print('导入耗时：', datetime.datetime.now() - start)
# ---数据库读取---
m.creatMyOrder(team)
print('处理耗时：', datetime.datetime.now() - start)
if team == 'slxmt':
	print('980')
	w.exportOrder(team)
	print('输出耗时：', datetime.datetime.now() - start)
else:
	print('020')
	m.connectOrder(team)
	print('输出耗时：', datetime.datetime.now() - start)


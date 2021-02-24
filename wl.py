import os
from wlExecl import WlExcel
from wlMysql import WlMysql
import datetime
start = datetime.datetime.now()
team = 'slxmt'
match = {'slrb': r'D:\Users\Administrator\Desktop\需要用到的文件\物流',
		'sltg': r'D:\Users\Administrator\Desktop\需要用到的文件\物流',
		'slgat': r'D:\Users\Administrator\Desktop\需要用到的文件\物流',
		'slxmt': r'D:\Users\Administrator\Desktop\需要用到的文件\物流'}
path = match[team]
dirs = os.listdir(path=path)
e = WlExcel()
m = WlMysql()

# ---读取execl文件---
for dir in dirs:
	filePath = os.path.join(path, dir)
	print(filePath)
	if dir[:2] != '~$':
		e.logisitis(filePath, team)
print('导入耗时：', datetime.datetime.now() - start)

# ---数据库读取---
m.creatLogisitis(team)
print('处理耗时：', datetime.datetime.now() - start)
m.printLogisitis(team)
print('输出耗时：', datetime.datetime.now() - start)
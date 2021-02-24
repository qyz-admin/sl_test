import os
from excelControl import ExcelControl
from mysqlControl import MysqlControl
import xlwings
import pandas as pd
import numpy as np
import datetime
import win32com.client
from dateutil.parser import parse
from numpy import nan as NaN

start = datetime.datetime.now()
team = 'sltg'
match = {'slrb': r'D:\Users\Administrator\Desktop\需要用到的文件\日本签收表',
		'sltg': r'D:\Users\Administrator\Desktop\需要用到的文件\泰国签收表',
		'slgat': r'D:\Users\Administrator\Desktop\需要用到的文件\港台签收表',
		'slxmt': r'D:\Users\Administrator\Desktop\需要用到的文件\新马签收表'}
path = match[team]
dirs = os.listdir(path=path)
print('第一步文件夹下的数组')
print(dirs)
e = ExcelControl()
m = MysqlControl()
print('第二步文件夹下的各个文件')
Filename = r'D:\Users\Administrator\Desktop\需要用到的文件\工作簿101.xlsx'
Filena = r'D:\Users\Administrator\Desktop\需要用到的文件\8月签收率  (港台)  3 - 副本.xlsm'

xls = win32com.client.Dispatch("Excel.Application")
xls.Workbooks.Open(Filename)
xls.Workbooks.Open(Filena)
ret = xls.Application.Run("读取直发数据()")
print(ret)
xls.Application.Quit()



# tm = 20200530
# tm = datetime.datetime.strptime("20200530", '%Y-%m-%d')
# print(tm)
# a=20181229
# b=str(a)
# c=parse(b)
# print(c)




for dir in dirs:
	filePath = os.path.join(path,dir)
	# print(filePath)
	if dir[:2] != '~$':
		print(dir)
		print(dir.split(".xlsx")[0])
#     	# e.readExecl(filePath,team)
		# app = xlwings.App(visible=False,add_book=False)
		# app.display_alerts = False

		# wb = app.books.open(filePath,update_links=False,read_only=True)
		# for sht in wb.sheets:
		# 	# if sht.name == 'result':
		# 	# 	sht.range('G1').value='订单编号'
		# 	# db= None
		# 	# file = sht.used_range.options(pd.DataFrame,header = 1,numbers=int,index=False).value
		# 	# print(file)
		# 	# if sht.name == 'result':
		# 	# 	columns = list(file.columns)
		# 	# 	print(columns)
		# 	# 	for x, column in enumerate(columns):
		# 	# 		if column == None:
		# 	# 			print(column)
		# 	# 	print('--------------')
		# 	# 	print(columns)
		# 	# 	print(sht.name)
		# 	if sht.name == '出货明细':
		# 		print(sht.name)
		# 		wb.sheets.add()
		# 		print('---------')
		# 		sht.range('G1').value='订单编号'
		# 	try:
		# 		file = sht.used_range.options(pd.DataFrame,header = 1,numbers=int,index=False).value
		# 		print(file)
		# 	except Exception as e:
		# 		print('xxxx查看失败：' + sht.name, str(Exception) + str(e))
		# wb.save()
		# wb.close()
		# app.quit()
print('遍历耗时：', datetime.datetime.now() - start)



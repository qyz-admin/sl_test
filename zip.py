import zipfile
import pandas as pd
import os
import datetime
from emailControl import EmailControl
from settings import Settings
import tkinter
from tkinter import messagebox
class ExcelZip():
	def __init__(self):
		Settings.__init__(self)
		self.e = EmailControl()
	def compress(self, team):  # 文件压缩
		# team = 'slgat'
		today = datetime.date.today().strftime('%Y.%m.%d')
		match = {'slgat': '港台',
				'sltg': '泰国',
				'slxmt': '新马',
				'slzb': '直播团队',
				'slyn': '越南',
				'slrb': '日本'}
		emailAdd = {'slgat': 'giikinliujun@163.com',
					'sltg': '1845389861@qq.com',
					'slxmt': 'zhangjing@giikin.com',
					'slzb': '直播团队',
					'slyn': '越南',
					'slrb': 'sunyaru@giikin.com'}
		filePath = ['D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}签收表.xlsx'.format(today, match[team])]

		if team == 'slgat':
			# 需要压缩的文件路径
			print(filePath[0])
			fileName = os.path.split(filePath[0])[-1]
			print('------正在生成需要压缩的文件>>> ' + fileName)
			zip_name = r'D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}签收表.zip'.format(today, match[team])
			print('已压缩后的文件名>>> ' + zip_name)

			size = os.path.getsize(filePath[0])
			print('------需要压缩的文件大小： ')
			print(size /1024/1024)
			if size / 1024 / 1024 > 50:
				print('------正在写入压缩文件中…………')
				zip = zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED, allowZip64=True)
				zip.write(filePath[0], fileName)
				filePath = ['D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}签收表.zip'.format(today, match[team])]
				print(filePath)
				print('------已写入压缩文件')
				self.e.send('{} 神龙{}签收表.xlsx'.format(today, match[team]), filePath,
					emailAdd[team])
			else:
				print('999')
				self.e.send('{} 神龙{}签收表.xlsx'.format(today, match[team]), filePath,
					emailAdd[team])
		else:
			print('0000')
			self.e.send('{} 神龙{}签收表.xlsx'.format(today, match[team]), filePath,
				emailAdd[team])


			# if os.path.isfile(filePath[0]):  # 如果src是一个文件
			# 	print('98')
			# 	fileName = os.path.split(filePath[0])[-1]
			# 	print(filePath[0])
			# 	print(fileName)

		# print('--------+++++++-------')
		# path = r'D:\\Users\\Administrator\\Desktop\\输出文件\2020.10.06 神龙港台签收表.xlsx'
		# out_name = r'D:\\Users\\Administrator\\Desktop\\输出文件\excel.zip'
		# if os.path.isfile(path):  # 如果src是一个文件
		# 	print('9808')
		# 	fileName = os.path.split(path)[-1]
		# 	print(path)
		# 	print(fileName)


if __name__ == '__main__':
	team = 'slgat'
	zp = ExcelZip()
	zp.compress(team)
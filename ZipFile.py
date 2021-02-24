import zipfile
import pandas as pd
import os
import datetime
from settings import Settings
import tkinter
from tkinter import messagebox

class ExcelZip():
	def __init__(self):
		Settings.__init__(self)
		# self.team = 'slgat'
		# self.today = datetime.date.today().strftime('%Y.%m.%d')
		# self.match = {'slgat': '港台',
		# 			'sltg': '泰国',
		# 			'slxmt': '新马',
		# 			'slzb': '直播团队',
		# 			'slyn': '越南',
		# 			'slrb': '日本'}
		# self.path = ['D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙港台签收表.xlsx'.format(self.today)]
		# self.out_name = r'D:\\Users\\Administrator\\Desktop\\输出文件\\excels.zip'  # 压缩包路径或名称
		# self.compress(team)

	def compress(self, team):  # 文件压缩
		# team = 'slgat'
		today = datetime.date.today().strftime('%Y.%m.%d')
		match = {'slgat': '港台',
				 'sltg': '泰国',
				 'slxmt': '新马',
				 'slzb': '直播团队',
				 'slyn': '越南',
				 'slrb': '日本'}
		path = 'D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙港台签收表.xlsx'.format(today, match[team])
		print(path)
		out_name = r'D:\\Users\\Administrator\\Desktop\\输出文件\excel.zip'  # 压缩包路径或名称

		f = zipfile.ZipFile(out_name, 'w', zipfile.ZIP_DEFLATED)
		p = zipfile.is_zipfile(r'D:\\Users\\Administrator\\Desktop\\输出文件\excel.zip')
		print(p)
		# print(f)
		# f.write(out_name)
		# f.close()

if __name__ == '__main__':
	team = 'slgat'
	zp = ExcelZip()
	zp.compress(team)

	print("这是一个弹出提示框")
	root=tkinter.Tk()
	# root.title('GUI')#标题
	root.geometry('350x169')#窗体大小
	root.resizable(False, False)#固定窗体
	# messagebox.showinfo("提示！！！","当前查询已完成--->>> 请前往（ 输出文件 ）查看")

	# root=tkinter.Tk()
	# root.title('GUI')#标题
	# root.geometry('0x0')#窗体大小
	# root.resizable(False, False)#固定窗体
	# # tkinter.Button(root, text='hello button',command=but).pack()
	# root.mainloop()
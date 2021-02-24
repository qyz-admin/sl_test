import os
from sqlalchemy import create_engine
from excelControl import ExcelControl
from mysqlControl import MysqlControl
from settings import Settings
import xlwings
import pandas as pd
import numpy as np
import datetime

class demo(Settings):
	def __init__(self):
		Settings.__init__(self)
		self.arg = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(self.mysql1['user'],
                                                                       self.mysql1['password'],
                                                                       self.mysql1['host'],
                                                                       self.mysql1['port'],
                                                                       self.mysql1['datebase']))
		# self.e = ExcelControl()
		# self. = MysqlControl()
	def bps(self):
		# team = 'slrb'
		# match = {'slrb': '"神龙家族-日本团队"'}
		# path = match[team]

		# yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + ' 23:59:59'
		# last_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-01'
		# start = datetime.datetime.now()

		sql = 'SELECT *  FROM tem; '
		dd = pd.read_sql_query(sql=sql,con=self.arg)
		print('---')
		print(dd)

		b = demo()
		b.bps()





import requests
from bs4 import BeautifulSoup # 抓标签里面元素的方法
import os
import xlwings
import pandas as pd
import datetime
import time

# from mysqlControl import MysqlControl
from settings import Settings
from sqlalchemy import create_engine
from queue import Queue
from threading import Thread #  使用 threading 模块创建线程
class BpsControl():
	def __init__(self, userName, password):
		Settings.__init__(self)
		self.userName = userName
		self.password = password
		self.session = requests.session()  #实例化session
		self.__load()
		self.q = Queue()    # 多线程调用的函数不能用return返回值，用来保存返回值
		self.engine1 = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(self.mysql1['user'],
																					self.mysql1['password'],
																					self.mysql1['host'],
																					self.mysql1['port'],
																					self.mysql1['datebase']))
		self.engine2 = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(self.mysql2['user'],
																					self.mysql2['password'],
																					self.mysql2['host'],
																					self.mysql2['port'],
																					self.mysql2['datebase']))
		self.engine3 = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(self.mysql3['user'],
																					self.mysql3['password'],
																					self.mysql3['host'],
																					self.mysql3['port'],
																					self.mysql3['datebase']))
	def __load(self):  # 登录系统保持会话状态
		url = r'https://goms.giikin.com/admin/login/index.html'
		data = {'username': self.userName,
				'password': self.password,
				'remember': '1'}
		# requests.session():维持会话,可以让我们在跨请求时保存某些参数
		r_header = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36'}
		r = self.session.post(url=url, headers=r_header, data=data)
		print('------  成功登陆系统后台  -------')
		print('               v           ')
	def getOrderInfo(self, orderId, searchType):                  # 进入查询界面
		url = 'https://goms.giikin.com/admin/order/orderquery.html'
		data = {'phone': None,
			'ship_email': None,
			'ip': None
				}
		if searchType == '订单号':
			data.update({'order_number': orderId,
						'waybill_number': None})
		elif searchType == '运单号':
			data.update({'order_number': None,
						'waybill_number': orderId})
		req = self.session.post(url=url, data=data)
		# print('-------已成功发送请求++++++')
		orderInfo = self._parseDate(req)   			# 获取订单简单信息
		# print(orderInfo)
		return orderInfo
	def _parseDate(self, req):  					# 对返回的response 进行处理
		# print('-------正在处理订单简单信息---------')
		soup = BeautifulSoup(req.text, 'lxml') 		# 创建 beautifulsoup 对象
		orderInfo = {}
		# print(soup)
		# print(soup.a['href'])
		labels = soup.find_all('th')   # 获取行标签的th值
		vals = soup.find_all('td')     # 获取表格的td的值
		# print('-------正在获取查询值..........')
		# print(labels)
		# print(vals)
		if len(labels) > len(vals) or len(labels) < len(vals):
			print('查询失败！！！')
		else:
			for i in range(len(labels)):
				orderInfo[str(labels[i]).replace("<th>", "").replace("</th>", "").strip()] = str(vals[i]).replace("<td>", "").replace("</td>", "").strip()
		# print('-------已处理订单简单信息---------')
		try:
			self.q.put(orderInfo)
		except Exception as e:
			print('放入失败---：', str(Exception) + str(e))
		# print(orderInfo)
		return orderInfo
	def getNumberT(self, team, searchType): # ----主线程的执行（多线程函数）
		match = {'slgat': '港台',
				'sltg': '泰国',
				'slxmt': '新马',
				'slzb': '直播团队',
				'slyn': '越南',
				'slrb': '日本'}
		print("========开始第一阶段查询（近6天）======")
		now_yesterday = (datetime.datetime.now()).strftime('%Y-%m-%d') + ' 23:59:59'
		last_yesterday = (datetime.datetime.now() - datetime.timedelta(days=4)).strftime('%Y-%m-%d') + ' 00:00:00'
		print(now_yesterday)
		print(last_yesterday)
		print('-----正在获取工作表的订单编号++++++')
		start = datetime.datetime.now()
		sql = '''SELECT order_number FROM 全部订单_{0} WHERE 全部订单_{0}.addtime>= '{1}' AND 全部订单_{0}.addtime<= '{2}';'''.format(team, last_yesterday, now_yesterday)
		ordersDict = pd.read_sql_query(sql=sql, con=self.engine3)
		print(ordersDict)
		ordersDict = ordersDict['order_number'].values.tolist()
		# print(ordersDict)
		print('获取耗时：', datetime.datetime.now() - start)
		print('------正在查询单个订单的详情++++++')
		print('主线程开始执行……………………')
		threads = []  # 多线程用线程池--
		for order in ordersDict:     # 注意前后数组的取值长度一致
			# print (order)   # print (ordersDict)
			threads.append(Thread(target=self.getOrderInfo, args=(order, searchType)))    #  -----也即是子线程
		print('子线程分配完成++++++')
		if threads:                  # 当所有的线程都分配完成之后，通过调用每个线程的start()方法再让他们开始。
			print(len(threads))
			for th in threads:
				th.start()           # print ("开启子线程…………")
			for th in threads:
				th.join()            # print ("退出子线程")
		else:
			print("没有需要运行子线程！！！")
		print('主线程执行结束---------')
		results = []
		print(self.q.qsize())
		print(self.q.empty())
		print(self.q.full())
		for i in range(len(ordersDict)):   # print(i)
			try:
				results.append(self.q.get())
			except Exception as e:
				print('取出失败---：', str(Exception) + str(e))
		print('-----执行结束---------')
		print('         V           ')
		# print(results)
		# pf = pd.DataFrame(list(results))  # 将字典列表转换为DataFrame
		pf = pd.DataFrame(results)
		pf.insert(0, '應付金額', '')
		pf.insert(0, '支付方式', '')
		pf.rename(columns={'规格': '规格中文'}, inplace=True)
		pf.dropna(subset=['订单号'],inplace=True)
		pf = pf[['订单号', '订单状态', '物流单号', '下单时间', '币种', '物流状态', '應付金額', '支付方式', '规格中文']]
		pf = pf.loc[:, ['订单号', '订单状态', '物流单号', '下单时间', '币种', '物流状态', '應付金額', '支付方式', '规格中文']]
		try:
			print('正在写入缓存表中…………')
			pf.to_sql('规格缓存_sltg', con=self.engine3, index=False, if_exists='replace')
			print('正在写入总订单表中…………')
			sql = 'REPLACE INTO 全部订单规格_sltg SELECT *, NOW() 添加时间  FROM 规格缓存_sltg;'
			#  sql = 'UPDATE 全部订单_sltg r INNER JOIN (SELECT 订单号,规格中文 FROM 规格缓存_sltg) t ON r.order_number= t.`订单号` SET r.op_id = t.`规格中文`;'
			pd.read_sql_query(sql=sql, con=self.engine3, chunksize=100)
		except Exception as e:
			print('缓存---：', str(Exception) + str(e))
		pf = pf.astype(str)   # dataframe的类型为dtype: object无法导入mysql中，需要转换为str类型
		print('------写入成功------')
		today = datetime.date.today().strftime('%Y.%m.%d')
		pf.to_excel('F:\\查询\\查询输出\\{} {} 订单查询.xlsx'.format(today, match[team]),
					sheet_name=match[team], index=False)
		print('------输出文件成功------')
		return ordersDict
	def getNumberAdd(self, team, searchType):   # ----主线程的执行（多线程函数）
		match = {'slgat': '港台',
				'sltg': '泰国',
				'slxmt': '新马',
				'slzb': '直播团队',
				'slyn': '越南',
				'slrb': '日本'}
		print("========开始第二阶段查询（补充）======")
		now_yesterday = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime('%Y-%m-%d') + ' 00:00:00'
		last_yesterday = (datetime.datetime.now() - datetime.timedelta(days=8)).strftime('%Y-%m-%d') + ' 00:00:00'
		print('-------正在获取工作表的订单编号++++++')
		start = datetime.datetime.now()
		sql = '''SELECT order_number FROM 全部订单_{0} WHERE 全部订单_{0}.op_id= '' And 全部订单_{0}.addtime>= '{1}' AND 全部订单_{0}.addtime<= '{2}';'''.format(team, last_yesterday, now_yesterday)
		ordersDict = pd.read_sql_query(sql=sql, con=self.engine3)
		print(ordersDict)
		ordersDict = ordersDict['order_number'].values.tolist()
		# print(ordersDict)
		print('获取耗时：', datetime.datetime.now() - start)
		print('--------正在查询单个订单的详情++++++')
		print('主线程开始执行……………………')
		threads = []  # 多线程用线程池--
		for order in ordersDict:     # 注意前后数组的取值长度一致
			# print (order)   # print (ordersDict)
			threads.append(Thread(target=self.getOrderInfo, args=(order, searchType)))    #  -----也即是子线程
		print('子线程分配完成++++++')
		if threads:                  # 当所有的线程都分配完成之后，通过调用每个线程的start()方法再让他们开始。
			print(len(threads))
			for th in threads:
				th.start()           # print ("开启子线程…………")
			for th in threads:
				th.join()            # print ("退出子线程")
		else:
			print("没有需要运行子线程！！！")
		print('子线程运行结束---------')
		results = []
		print(self.q.qsize())
		print(self.q.empty())
		print(self.q.full())
		for i in range(len(ordersDict)):    # print(i)
			try:
				results.append(self.q.get())
			except Exception as e:
				print('取出失败---：', str(Exception) + str(e))
		print('-----订单获取执行结束---------')
		print('         V           ')
		# print(results)
		# pf = pd.DataFrame(list(results))  # 将字典列表转换为DataFrame
		pf = pd.DataFrame(results)
		pf.insert(0, '應付金額', '')
		pf.insert(0, '支付方式', '')
		pf.rename(columns={'规格': '规格中文'}, inplace=True)
		pf.dropna(subset=['订单号'],inplace=True)
		pf = pf[['订单号', '订单状态', '物流单号', '下单时间', '币种', '物流状态', '應付金額', '支付方式', '规格中文']]
		pf = pf.loc[:, ['订单号', '订单状态', '物流单号', '下单时间', '币种', '物流状态', '應付金額', '支付方式', '规格中文']]
		try:
			print('正在写入缓存表中…………')
			pf.to_sql('规格缓存_sltg', con=self.engine3, index=False, if_exists='replace')
			sql = 'REPLACE INTO 全部订单规格_sltg SELECT *, NOW() 添加时间  FROM 规格缓存_sltg;'
			# sql = 'UPDATE 全部订单_sltg r INNER JOIN (SELECT 订单号,规格中文 FROM 规格缓存_sltg) t ON r.order_number= t.`订单号` SET r.op_id = t.`规格中文`;'
			pd.read_sql_query(sql=sql, con=self.engine3, chunksize=100)
		except Exception as e:
			print('缓存---：', str(Exception) + str(e))
		pf = pf.astype(str)   # dataframe的类型为dtype: object无法导入mysql中，需要转换为str类型
		print('------缓存成功------')
		today = datetime.date.today().strftime('%Y.%m.%d')
		pf.to_excel('F:\\查询\\查询输出\\{} {} 订单补充查询.xlsx'.format(today, match[team]),
					sheet_name=match[team], index=False)
		print('------输出文件成功------')
		return ordersDict

	# 获取泰国海外仓
	def sltg_HaiWaiCang(self, house):
		match = {'shifeng': 'Tracking Number',
				'chaoshidai': '运单号',
				'bojiatu': '上架单号', }
		match1 = {'shifeng': '海外仓库存_时丰',
				'chaoshidai': '海外仓库存_超时代在库',
				'bojiatu': '海外仓库存_博佳图', }
		today = datetime.date.today().strftime('%Y.%m.%d')
		sql = '''SELECT 订单编号,
						运单号,
						产品id,
						产品名称,
						规格中文,
						数量,
						qb.订单状态 
				FROM
					(SELECT 
						a.order_number '订单编号', 
						a.waybill_number '运单号',
						a.goods_id '产品id',
						a.goods_name '产品名称',
						a.op_id '规格',
						a.quantity '数量',
						a.order_status '订单状态' 
					FROM 
						全部订单_sltg a 
					INNER JOIN 
						(SELECT DISTINCT 
							upper(`{0}`) 'Tracking Number'
						FROM {1}) b 
						ON a.waybill_number = b.`Tracking Number`) qb 
					INNER JOIN 全部订单规格_sltg b 
						ON qb.`订单编号` = b.`订单号`;'''.format(match[house], match1[house])
		print('正在查询' + match1[house] + '订单…………')
		df = pd.read_sql_query(sql=sql, con=self.engine3)
		# print(df)
		print('正在写入excel…………')
		df.to_excel('D:\\Users\\Administrator\\Desktop\\查询\\{} {}.xlsx'.format(today, match1[house]),
					sheet_name=match1[house], index=False)
		print('输出文件成功…………')
if __name__ == '__main__':                    # 以老后台的简单查询为主，
	start = datetime.datetime.now()	
	print('======正在启动查询订单程序>>>>>')
	print('               v           ')
	# s = Bds('qiyuanzhang@jikeyin.com', 'qiyuanzhang123.')
	s = BpsControl('nixiumin@giikin.com', 'nixiumin123@.')
	# s.__load()
	# # s.getOrderInfo("NR010230026492511", '订单号')
	# s.getOrderInfo("TH009281245118873", '订单号')

	# 获取订单明细（各团队）
	match = {'slgat': '港台',
		'sltg': '泰国',
		'slxmt': '新马',
		'slzb': '直播团队',
		'slyn': '越南',
		'slrb': '日本'}
	# team = 'slgat'
	# searchType = '运单号'  # 运单号，订单号   查询切换
	# s.getNumber(team, searchType)

	# rq = '2020-09-29'
	# team = 'sltg'
	# searchType = '订单号'  # 运单号，订单号   查询切换
	# print("========开始第一阶段查询（近6天）======")
	# s.getNumberT(team, searchType, last_yesterday, now_yesterday)
	# print('查询耗时：', datetime.datetime.now() - start)
	# time.sleep(10)
	#
	# print("========开始第二阶段查询（补充）======")
	# s.getNumberAdd(team, searchType, last_yesterday, now_yesterday)
	# print('补充耗时：', datetime.datetime.now() - start)
	# print('         ^           ')

	# 获取泰国海外仓
	house = 'shifeng'
	match0 = {'shifeng': 'Tracking Number',
			 'chaoshidai': '运单号',
			'bojiatu': '上架单号', }
	s.sltg_HaiWaiCang(house)

	house = 'bojiatu'
	s.sltg_HaiWaiCang(house)




import requests
import json
import sys
from bs4 import BeautifulSoup # 抓标签里面元素的方法
import os
import xlwings
import pandas as pd
import datetime
import time

from dateutil.relativedelta import relativedelta
# from mysqlControl import MysqlControl
from settings import Settings
from sqlalchemy import create_engine
from queue import Queue
from threading import Thread #  使用 threading 模块创建线程
class BpsControl99(Settings):
	def __init__(self, userName, password):
		Settings.__init__(self)
		self.userName = userName
		self.password = password
		self.session = requests.session()  #	实例化session，维持会话,可以让我们在跨请求时保存某些参数
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
		r_header = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36'}
		r = self.session.post(url=url, headers=r_header, data=data)
		print('------  成功登陆系统后台  -------')

	def getOrderInfo(self, orderId, searchType):                  # 进入查询界面
		url = 'https://goms.giikin.com/admin/order/orderquery.html'
		data = {'phone': None,
				'ship_email': None,
				'ip': None}
		if searchType == '订单号':
			data.update({'order_number': orderId,
						'waybill_number': None})
		elif searchType == '运单号':
			data.update({'order_number': None,
						'waybill_number': orderId})
		req = self.session.post(url=url, data=data)
		print('-------已成功发送请求++++++')
		orderInfo = self._parseDate(req)   			# 获取订单简单信息
		orderInfo = self._getOrderInfo(orderInfo) # 获取订单详细信息
		print(orderInfo)

	def _parseDate(self, req):  					# 对返回的response 进行处理； 处理订单简单信息
		soup = BeautifulSoup(req.text, 'lxml') 		# 创建 beautifulsoup 对象
		orderInfo = {}
		labels = soup.find_all('th')   				# 获取行标签的th值
		vals = soup.find_all('td')     				# 获取表格的td的值
		if len(labels) > len(vals) or len(labels) < len(vals):
			print('查询失败！！！')
		else:
			for i in range(len(labels)):
				orderInfo[str(labels[i]).replace("<th>", "").replace("</th>", "").strip()] = str(vals[i]).replace("<td>", "").replace("</td>", "").strip()
		print('-------已处理订单简单信息---------')
		try:
			self.q.put(orderInfo)
		except Exception as e:
			print('放入失败---：', str(Exception) + str(e))
		return orderInfo

	def _getOrderInfo(self, orderInfo):  # 对处理后的键值对（字典-字符串）再处理，处理订单详细信息
		id = orderInfo['操作'].replace('<a href="/admin/order/info/id/', '').replace('.html" target="_blank">查看详情</a>', '')
		url = 'https://goms.giikin.com/admin/order/info/id/' + str(id) + '.html'
		req = self.session.get(url=url)
		soup = BeautifulSoup(req.text, 'lxml')
		lables = req.text.split('td-label">')
		vals = req.text.split('td-text">')
		print(soup)
		print(1)
		if len(lables) > len(vals):
			for i in range(1, len(lables)-1):
				orderInfo[lables[i].split('</td>')[0].strip()] = vals[i].split('</td>')[0].strip()
		elif len(lables) < len(vals):
			for i in range(1, len(lables)):
				orderInfo[lables[i].split('</td>')[0].strip()] = vals[i].split('</td>')[0].strip()
		lables = req.text.split('%">')
		vals = req.text.split('<td>')
		print(2)
		print(lables)
		print(3)
		print(vals)
		for i in range(1, len(lables)):
			if len(lables) == len(vals):
				orderInfo[lables[i].split('</td>')[0].strip()] = vals[i].split('</td>')[0].strip()
			else:
				lst = []
				for i1 in range(int((len(vals) - 1)/(len(lables) - 1))):
					lst.append(vals[i1 * (len(lables) - 1) + i].split('</td>')[0].strip())
				orderInfo[lables[i].split('</td>')[0].strip()] = lst
		self.q.put(orderInfo)
		print('-------已处理订单详细数据---------')
		return orderInfo


	def getNumberT(self, team, searchType): # ----主线程的执行（多线程函数）
		match = {'slgat': '港台',
				'sltg': '泰国',
				'slxmt': '新马',
				'slzb': '直播团队',
				'slyn': '越南',
				'slrb': '日本'}
		print("======== 开始订单产品详情查询 ======")
		month_begin = (datetime.datetime.now() - relativedelta(months=4)).strftime('%Y-%m-%d')
		start = datetime.datetime.now()
		sql = '''SELECT id,`订单编号`  FROM {0}_order_list sl 
				WHERE sl.`日期`> '{1}' 
					AND  sl.`产品名称` IS NULL 
					AND sl.`系统订单状态` != '已删除' ;'''.format(team, month_begin)
		ordersDict = pd.read_sql_query(sql=sql, con=self.engine1)
		print(ordersDict)
		ordersDict = ordersDict['订单编号'].values.tolist()
		print(ordersDict)
		print('获取耗时：', datetime.datetime.now() - start)

		print('------正在查询单个订单的详情++++++')
		print('主线程开始执行……………………')
		threads = []  # 多线程用线程池--
		for order in ordersDict:     # 注意前后数组的取值长度一致
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
		pc=pd.concat([pf, pf['产品ID'].str.split(',', expand=True)], axis=1)
		print(pc)
		print(11)
		pf['订单编号']=pf['订单号'] + pf['产品ID'].astype(str)
		print(pf['订单编号'])
		print(type(pf))
		print(pf['产品ID'].str.split(','))

		print(pf['产品ID'].str.split(' ', expand=True).values.tolist())
		print(55)
		print(pf['产品ID'].values.tolist())
		print(66)

		# pf.insert(0, '應付金額', '')
		# pf.insert(0, '支付方式', '')
		# pf.rename(columns={'规格': '规格中文'}, inplace=True)
		# pf.dropna(subset=['订单号'],inplace=True)
		# pf = pf[['订单号', '订单状态', '物流单号', '下单时间', '币种', '物流状态', '應付金額', '支付方式', '规格中文']]
		# pf = pf.loc[:, ['订单号', '订单状态', '物流单号', '下单时间', '币种', '物流状态', '應付金額', '支付方式', '规格中文']]
		# try:
		# 	print('正在写入缓存表中…………')
		# 	pf.to_sql('规格缓存_sltg', con=self.engine3, index=False, if_exists='replace')
		# 	print('正在写入总订单表中…………')
		# 	sql = 'REPLACE INTO 全部订单规格_sltg SELECT *, NOW() 添加时间  FROM 规格缓存_sltg;'
		# 	#  sql = 'UPDATE 全部订单_sltg r INNER JOIN (SELECT 订单号,规格中文 FROM 规格缓存_sltg) t ON r.order_number= t.`订单号` SET r.op_id = t.`规格中文`;'
		# 	pd.read_sql_query(sql=sql, con=self.engine3, chunksize=100)
		# except Exception as e:
		# 	print('缓存---：', str(Exception) + str(e))
		# pf = pf.astype(str)   # dataframe的类型为dtype: object无法导入mysql中，需要转换为str类型
		print('------写入成功------')
		today = datetime.date.today().strftime('%Y.%m.%d')
		pf.to_excel('F:\\查询\\查询输出\\{} {} 9908099订单查询.xlsx'.format(today, match[team]),
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


	def productIdInfoT(self, orderId, searchType,team):  # 进入查询界面，
		start = datetime.datetime.now()
		month_begin = (datetime.datetime.now() - relativedelta(months=4)).strftime('%Y-%m-%d')
		sql = '''SELECT id,`订单编号`  FROM {0}_order_list sl 
				WHERE sl.`日期`> '{1}' 
					AND  sl.`产品名称` IS NULL 
					AND sl.`系统订单状态` != '已删除' ;'''.format(team, month_begin)
		ordersDict = pd.read_sql_query(sql=sql, con=self.engine1)
		if ordersDict.empty:
			print('无需要更新的产品id信息！！！')
			sys.exit()
		orderId = list(ordersDict['订单编号'])
		orderId = ', '.join(orderId)
		print('获取耗时：', datetime.datetime.now() - start)
		url = r'http://gimp.giikin.com/service?service=gorder.customer&action=getQueryOrder'
		data = {'phone': None,
				'email': None,
				'ip': None,
				'_token': '05135e6a194a01c9c0b2d76ef221a770'}
		if searchType == '订单号':
			data.update({'orderPrefix': orderId,
					 	'shippingNumber': None})
		elif searchType == '运单号':
			data.update({'order_number': None,
					 	'shippingNumber': orderId})
		r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
					'Referer': 'http://gimp.giikin.com/front/orderToolsServiceQuery'}
		req = self.session.post(url=url, headers=r_header, data=data)
		print('已成功发送请求++++++')
		print('正在处理json数据…………')
		req = json.loads(req.text)  	# json类型数据转换为dict字典
		print('正在转化数据为dataframe…………')
		ordersDict = []
		for result in req['data']['list']:
			print(result)
			# 添加新的字典键-值对，为下面的重新赋值用
			result['productId'] = 0
			result['saleName'] = 0
			result['saleProduct'] = 0
			result['spec'] = 0
			result['link'] = 0
			# print(result['specs'])
			# spe = ''
			# spe2 = ''
			# spe3 = ''
			# spe4 = ''
			# # 产品详细的获取
			# for ind, re in enumerate(result['specs']):
			#     print(ind)
			#     print(re)
			#     print(result['specs'][ind])
			#     spe = spe + ';' + result['specs'][ind]['saleName']
			#     spe2 = spe2 + ';' + result['specs'][ind]['saleProduct']
			#     spe3 = spe3 + ';' + result['specs'][ind]['spec']
			#     spe4 = spe4 + ';' + result['specs'][ind]['link']
			#     spe = spe + ';' + result['specs'][ind]['saleProduct'] + result['specs'][ind]['spec'] + result['specs'][ind]['link'] + result['specs'][ind]['saleName']
			# result['specs'] = spe
			# # del result['specs']             # 删除多余的键值对
			# result['saleName'] = spe
			# result['saleProduct'] = spe2
			# result['spec'] = spe3
			# result['link'] = spe4
			print(9555)
			result['saleName'] = result['specs'][0]['saleName']
			result['saleProduct'] = result['specs'][0]['saleProduct']
			result['spec'] = result['specs'][0]['spec']
			result['link'] = result['specs'][0]['link']
			result['productId'] = (result['specs'][0]['saleProduct']).split('#')[1]
			print(9555)
			quest = ''
			for re in result['questionReason']:
				quest = quest + ';' + re
			print(quest)
			result['questionReason'] = quest
			delr = ''
			for re in result['delReason']:
				delr = delr + ';' + re
			print(delr)
			result['delReason'] = delr
			auto = ''
			for re in result['autoVerify']:
				auto = auto + ';' + re
			print(auto)
			result['autoVerify'] = auto
			self.q.put(result)
		print('00')
		print(len(req['data']['list']))
		print('99')
		for i in range(len(req['data']['list'])):
			ordersDict.append(self.q.get())
		data = pd.json_normalize(ordersDict)
		print(data)
		print(data[['orderNumber', 'productId']])
		print('正在写入缓存中......')
		try:
			data[['orderNumber', 'productId']].to_sql('d1', con=self.engine1, index=False, if_exists='replace')
			sql = '''SELECT orderNumber ,
						productId,
						dp.`name` ,
						dc.ppname cate,
						dc.pname second_cate,
						dc.`name` third_cate
					FROM d1
					LEFT JOIN dim_product dp ON  d1.productId = dp.id
					LEFT JOIN dim_cate dc ON  dc.id = dp.third_cate_id;'''
			df = pd.read_sql_query(sql=sql, con=self.engine1)
			print(df)
			df.to_sql('tem_product_id', con=self.engine1, index=False, if_exists='replace')
			print('正在更新产品详情…………')
			sql = '''update {0}_order_list a, tem_product_id b
			                        set a.`产品id`=b.`productId`,
			                            a.`产品名称`=b.`name`,
					                    a.`父级分类`=b.`cate` ,
					                    a.`二级分类`=b.`second_cate`,
					                    a.`三级分类`=b.`third_cate`
					                where a.`订单编号`=b.`orderNumber`;'''.format(team)
			pd.read_sql_query(sql=sql, con=self.engine1, chunksize=1000)
		except Exception as e:
			print('更新失败：', str(Exception) + str(e))
		print('更新成功…………')
		print('更新耗时：', datetime.datetime.now() - start)
if __name__ == '__main__':                    # 以老后台的简单查询为主，
	start = datetime.datetime.now()
	s = BpsControl99('qiyuanzhang@jikeyin.com', 'qiyuanzhang123.0')
	# s.getOrderInfo("NA201116233802701506", '订单号')
	team = 'slgat'
	s.productIdInfoT("UP210308012148546055,UP210308221139084693", '订单号', team)

	# # 获取订单明细（各团队）
	# match = {'slgat': '港台',
	# 	'sltg': '泰国',
	# 	'slxmt': '新马',
	# 	'slzb': '直播团队',
	# 	'slyn': '越南',
	# 	'slrb': '日本'}
	#
	# # rq = '2020-09-29'
	# team = 'slgat'
	# searchType = '订单号'  # 运单号，订单号   查询切换
	# # # print("========开始第一阶段查询（近6天）======")
	# s.getNumberT(team, searchType)
	#
	#
	#
	#
	# time.sleep(10)



	# # 获取泰国海外仓
	# house = 'shifeng'
	# match0 = {'shifeng': 'Tracking Number',
	# 		 'chaoshidai': '运单号',
	# 		'bojiatu': '上架单号', }
	# s.sltg_HaiWaiCang(house)



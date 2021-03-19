import pandas as pd
import os
import datetime
import xlwings

import requests
import json
import sys
from queue import Queue
from dateutil.relativedelta import relativedelta
from threading import Thread #  使用 threading 模块创建线程
import pandas.io.formats.excel

from sqlalchemy import create_engine
from settings import Settings
from emailControl import EmailControl
from openpyxl import load_workbook  # 可以向不同的sheet写入数据
from openpyxl.styles import Font, Border, Side, PatternFill, colors, \
    Alignment  # 设置字体风格为Times New Roman，大小为16，粗体、斜体，颜色蓝色


# -*- coding:utf-8 -*-
class QueryControl(Settings):
    def __init__(self):
        Settings.__init__(self)
        self.session = requests.session()  # 实例化session，维持会话,可以让我们在跨请求时保存某些参数
        self.q = Queue()  # 多线程调用的函数不能用return返回值，用来保存返回值
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
        self.engine20 = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(self.mysql20['user'],
                                                                                    self.mysql20['password'],
                                                                                    self.mysql20['host'],
                                                                                    self.mysql20['port'],
                                                                                    self.mysql20['datebase']))
        self.engine3 = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(self.mysql3['user'],
                                                                                    self.mysql3['password'],
                                                                                    self.mysql3['host'],
                                                                                    self.mysql3['port'],
                                                                                    self.mysql3['datebase']))
        self.e = EmailControl()
    def reSetEngine(self):
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

    # 获取签收表内容
    def readFormHost(self, team):
        match2 = {'slgat': '港台',
                 'sltg': '泰国',
                 'slxmt': '新马',
                 'slrb': '日本'}
        match3 = {'新加坡': 'slxmt',
                  '马来西亚': 'slxmt',
                  '菲律宾': 'slxmt',
                  '新马': 'slxmt',
                  '日本': 'slrb',
                  '香港': 'slgat',
                  '台湾': 'slgat',
                  '港台': 'slgat',
                  '泰国': 'sltg'}
        start = datetime.datetime.now()
        path = r'D:\Users\Administrator\Desktop\需要用到的文件\数据库'
        dirs = os.listdir(path=path)
        # ---读取execl文件---
        for dir in dirs:
            filePath = os.path.join(path, dir)
            if dir[:2] != '~$':
                print(filePath)
                self.wbsheetHost(filePath, team)
        print('处理耗时：', datetime.datetime.now() - start)
    # 工作表的订单信息
    def wbsheetHost(self, filePath, team):
        print('---正在获取签收表的详情++++++')
        fileType = os.path.splitext(filePath)[1]
        app = xlwings.App(visible=False, add_book=False)
        app.display_alerts = False
        if 'xls' in fileType:
            wb = app.books.open(filePath, update_links=False, read_only=True)
            for sht in wb.sheets:
                try:
                    db = None
                    # db = sht.used_range.value
                    db = sht.used_range.options(pd.DataFrame, header=1, numbers=int, index=False).value
                    columns = list(db.columns)  # 获取数据的标题名，转为列表
                    columns_value = ['商品链接', '规格(中文)', '收货人', '拉黑率', '电话长度', '邮编长度', '配送地址', '地址翻译',
                                     '邮箱', '留言', '审核人', '预选物流公司(新)', '是否api下单', '特价', '站点ID', '异常提示',
                                     '删除原因', '备注', '删除人', 'IP', '超商店铺ID', '超商店铺名称', '超商网点地址', '超商类型',
                                     '省洲', '市/区', '优化师', '问题原因', '审单类型']
                    for column_val in columns_value:
                        if column_val in columns:
                            db.drop(labels=[column_val], axis=1, inplace=True)  # 去掉多余的旬列表
                    db['运单号'] = db['运单号'].str.strip()
                    print(db.columns)
                except Exception as e:
                    print('xxxx查看失败：' + sht.name, str(Exception) + str(e))
                if db is not None and len(db) > 0:
                    print('++++正在导入：' + sht.name + ' 共：' + str(len(db)) + '行',
                          'sheet共：' + str(sht.used_range.last_cell.row) + '行')
                    # 将返回的dateFrame导入数据库的临时表
                    self.writeCacheHost(db)
                    print('++++正在更新：' + sht.name + '--->>>到总订单')
                    # 将数据库的临时表替换进指定的总表
                    self.replaceSqlHost(team)
                    print('++++----->>>' + sht.name + '：订单更新完成++++')
                else:
                    print('----------数据为空导入失败：' + sht.name)
            wb.close()
        app.quit()

    # 写入临时缓存表
    def writeCacheHost(self, dataFrame):
        dataFrame.to_sql('d1_host', con=self.engine1, index=False, if_exists='replace')
    # 写入总表
    def replaceSqlHost(self, team):
        if team == 'slgat':
            sql = '''SELECT EXTRACT(YEAR_MONTH  FROM h.下单时间) 年月,
            				        IF(IF(DAYOFMONTH(h.下单时间) > '20', '3', IF(DAYOFMONTH(h.下单时间) < '10', '2', h.`币种`)),IF(DAYOFMONTH(h.下单时间) > '20', '3', IF(DAYOFMONTH(h.下单时间) < '10', '2', h.`币种`)),'2') 旬,
            			            DATE(h.下单时间) 日期,
            				        h.运营团队 团队,
            				        IF(h.`币种` = '台币', 'TW', IF(h.`币种` = '港币', 'HK', h.`币种`)) 区域,
            				        IF(h.`币种` = '台币', '台湾', IF(h.`币种` = '港币', '香港', h.`币种`)) 币种,
            				        h.平台 订单来源,
            				        订单编号,
            				        数量,
            				        h.联系电话 电话号码,
            				        h.运单号 运单编号,
            				        IF(h.`订单类型` in ('未下架未改派','直发下架'), '直发', '改派') 是否改派,
            				        h.物流渠道 物流方式,
            				        dim_trans_way.simple_name 物流名称,
            				        dim_trans_way.remark 运输方式,
            				        IF(h.`货物类型` = 'P 普通货', 'P', IF(h.`货物类型` = 'T 特殊货', 'T', h.`货物类型`)) 货物类型,
            				        是否低价,
            				        产品id,
            				        产品名称,
            				        dim_cate.ppname 父级分类,
            				        dim_cate.pname 二级分类,
                		            dim_cate.`name` 三级分类,
            				        h.支付方式 付款方式,
            				        h.应付金额 价格,
            				        下单时间,
            				        审核时间,
            				        h.发货时间 仓储扫描时间,
            				        null 完结状态,
            				        h.完成时间 完结状态时间,
            				        null 价格RMB,
            				        null 价格区间,
            				        null 成本价,
            				        null 物流花费,
            				        null 打包花费,
            				        null 其它花费,
            				        h.重量 包裹重量,
            				        h.体积 包裹体积,
            				        邮编,
            				        h.转采购时间 添加物流单号时间,
            				        null 订单删除原因,
            				        h.订单状态 系统订单状态,
            				        IF(h.`物流状态` in ('发货中'), '在途', h.`物流状态`) 系统物流状态
                            FROM d1_host h 
                            LEFT JOIN dim_product ON  dim_product.id = h.产品id
                            LEFT JOIN dim_cate ON  dim_cate.id = dim_product.third_cate_id
                            LEFT JOIN dim_trans_way ON  dim_trans_way.all_name = h.`物流渠道`; '''.format(team)
        elif team == 'slrb':
            sql = '''SELECT EXTRACT(YEAR_MONTH  FROM h.下单时间) 年月,
				                IF(IF(DAYOFMONTH(h.下单时间) > '20', '3', IF(DAYOFMONTH(h.下单时间) < '10', '2', h.`币种`)),IF(DAYOFMONTH(h.下单时间) > '20', '3', IF(DAYOFMONTH(h.下单时间) < '10', '2', h.`币种`)),'2') 旬,
			                    DATE(h.下单时间) 日期,
				                h.运营团队 团队,
				                IF(h.`币种` = '日币', 'JP', h.`币种`) 区域,
				                IF(h.`币种` = '日币', '日本', h.`币种`) 币种,
				                h.平台 订单来源,
				                订单编号,
				                数量,
				                h.联系电话 电话号码,
				                h.运单号 运单编号,
				                IF(h.`订单类型` in ('未下架未改派','直发下架'), '直发', '改派') 是否改派,
				                h.物流渠道 物流方式,
			--	                IF(h.`物流渠道` LIKE '%捷浩通%', '捷浩通', IF(h.`物流渠道` LIKE '%翼通达%','翼通达', IF(h.`物流渠道` LIKE '%博佳图%', '博佳图', IF(h.`物流渠道` LIKE '%保辉达%', '保辉达物流', IF(h.`物流渠道` LIKE '%万立德%','万立德', h.`物流渠道`))))) 物流名称,
				                dim_trans_way.simple_name 物流名称,
				                dim_trans_way.remark 运输方式,
				                IF(h.`货物类型` = 'P 普通货', 'P', IF(h.`货物类型` = 'T 特殊货', 'T', h.`货物类型`)) 货物类型,
				                是否低价,
				                产品id,
				                产品名称,
				                dim_cate.ppname 父级分类,
				                dim_cate.pname 二级分类,
    		                    dim_cate.`name` 三级分类,
				                h.支付方式 付款方式,
				                h.应付金额 价格,
				                下单时间,
				                审核时间,
				                h.发货时间 仓储扫描时间,
				                null 完结状态,
				                h.完成时间 完结状态时间,
				                null 价格RMB,
				                null 价格区间,
				                null 成本价,
				                null 物流花费,
				                null 打包花费,
				                null 其它花费,
				                h.重量 包裹重量,
				                h.体积 包裹体积,
				                邮编,
				                h.转采购时间 添加物流单号时间,
				                null 订单删除原因,
				                h.订单状态 系统订单状态,
				                IF(h.`物流状态` in ('发货中'), '在途', h.`物流状态`) 系统物流状态
                    FROM d1_host h 
                    LEFT JOIN dim_product ON  dim_product.id = h.产品id
                    LEFT JOIN dim_cate ON  dim_cate.id = dim_product.third_cate_id
                    LEFT JOIN dim_trans_way ON  dim_trans_way.all_name = h.`物流渠道`;'''.format(team)
        elif team == 'sltg':
            sql = '''SELECT EXTRACT(YEAR_MONTH  FROM h.下单时间) 年月,
				                IF(IF(DAYOFMONTH(h.下单时间) > '20', '3', IF(DAYOFMONTH(h.下单时间) < '10', '2', h.`币种`)),IF(DAYOFMONTH(h.下单时间) > '20', '3', IF(DAYOFMONTH(h.下单时间) < '10', '2', h.`币种`)),'2') 旬,
			                    DATE(h.下单时间) 日期,
				                h.运营团队 团队,
				                IF(h.`币种` = '泰铢', 'TH', h.`币种`) 区域,
				                IF(h.`币种` = '泰铢', '泰国', h.`币种`) 币种,
				                h.平台 订单来源,
				                订单编号,
				                数量,
				                h.联系电话 电话号码,
				                h.运单号 运单编号,
				                IF(h.`订单类型` in ('未下架未改派','直发下架'), '直发', '改派') 是否改派,
				                h.物流渠道 物流方式,
                                dim_trans_way.simple_name 物流名称,
				                dim_trans_way.remark 运输方式,
				                IF(h.`货物类型` = 'P 普通货', 'P', IF(h.`货物类型` = 'T 特殊货', 'T', h.`货物类型`)) 货物类型,
				                是否低价,
				                产品id,
				                产品名称,
				                dim_cate.ppname 父级分类,
				                dim_cate.pname 二级分类,
    		                    dim_cate.`name` 三级分类,
				                h.支付方式 付款方式,
				                h.应付金额 价格,
				                下单时间,
				                审核时间,
				                h.发货时间 仓储扫描时间,
				                null 完结状态,
				                h.完成时间 完结状态时间,
				                null 价格RMB,
				                null 价格区间,
				                null 成本价,
				                null 物流花费,
				                null 打包花费,
				                null 其它花费,
				                h.重量 包裹重量,
				                h.体积 包裹体积,
				                邮编,
				                h.转采购时间 添加物流单号时间,
				                null 订单删除原因,
				                h.订单状态 系统订单状态,
				                IF(h.`物流状态` in ('发货中'), '在途', h.`物流状态`) 系统物流状态
                    FROM d1_host h 
                    LEFT JOIN dim_product ON  dim_product.id = h.产品id
                    LEFT JOIN dim_cate ON  dim_cate.id = dim_product.third_cate_id
                    LEFT JOIN dim_trans_way ON  dim_trans_way.all_name = h.`物流渠道`;'''.format(team)
        else:
            sql = '''SELECT EXTRACT(YEAR_MONTH  FROM h.下单时间) 年月,
				                IF(IF(DAYOFMONTH(h.下单时间) > '20', '3', IF(DAYOFMONTH(h.下单时间) < '10', '2', h.`币种`)),IF(DAYOFMONTH(h.下单时间) > '20', '3', IF(DAYOFMONTH(h.下单时间) < '10', '2', h.`币种`)),'2') 旬,
			                  DATE(h.下单时间) 日期,
				                h.运营团队 团队,
-- 								IF(IF(h.`币种` = '马来西亚', 'MY', IF(h.`币种` ='菲律宾', 'PH',IF(h.`币种` = '新加坡', 'SG',h.`币种`)))) 区域,
							    null 区域,
				                币种,
				                h.平台 订单来源,
				                订单编号,
				                数量,
				                h.联系电话 电话号码,
				                h.运单号 运单编号,
				                IF(h.`订单类型` in ('未下架未改派','直发下架'), '直发', '改派') 是否改派,
				                h.物流渠道 物流方式,
								dim_trans_way.simple_name 物流名称,
				                dim_trans_way.remark 运输方式,
				                IF(h.`货物类型` = 'P 普通货', 'P', IF(h.`货物类型` = 'T 特殊货', 'T', h.`货物类型`)) 货物类型,
				                是否低价,
				                产品id,
				                产品名称,
				                dim_cate.ppname 父级分类,
				                dim_cate.pname 二级分类,
    		                    dim_cate.`name` 三级分类,
				                h.支付方式 付款方式,
				                h.应付金额 价格,
				                下单时间,
				                审核时间,
				                h.发货时间 仓储扫描时间,
				                null 完结状态,
				                h.完成时间 完结状态时间,
				                null 价格RMB,
				                null 价格区间,
				                null 成本价,
				                null 物流花费,
				                null 打包花费,
				                null 其它花费,
				                h.重量 包裹重量,
				                h.体积 包裹体积,
				                邮编,
				                h.转采购时间 添加物流单号时间,
				                null 订单删除原因,
				                h.订单状态 系统订单状态,
				                IF(h.`物流状态` in ('发货中'), '在途', h.`物流状态`) 系统物流状态
                    FROM d1_host h 
                    LEFT JOIN dim_product ON  dim_product.id = h.产品id
                    LEFT JOIN dim_cate ON  dim_cate.id = dim_product.third_cate_id
                    LEFT JOIN dim_trans_way ON  dim_trans_way.all_name = h.`物流渠道`;'''.format(team)
        try:
            print('正在导入临时表中......')
            df = pd.read_sql_query(sql=sql, con=self.engine1)
            columns = list(df.columns)
            columns = ', '.join(columns)
            df.to_sql('d1_host_cp', con=self.engine1, index=False, if_exists='replace')
            print('正在更新表总表中......')
            sql = '''INSERT IGNORE INTO {}_order_list({}, 记录时间) SELECT *, NOW() 记录时间 FROM d1_host_cp; '''.format(team, columns)
            pd.read_sql_query(sql=sql, con=self.engine1, chunksize=2000)
        except Exception as e:
            print('插入失败：', str(Exception) + str(e))

    # 更新团队订单明细（新后台的）
    def orderInfo(self, tokenid, searchType, team, last_month):  # 进入查询界面，
        print('正在获取需要订单信息')
        start = datetime.datetime.now()
        # month_begin = (datetime.datetime.now() - relativedelta(months=4)).strftime('%Y-%m-%d')
        sql = '''SELECT id,`订单编号`  FROM {0}_order_list sl WHERE sl.`日期`= '{1}';'''.format(team, last_month)
        ordersDict = pd.read_sql_query(sql=sql, con=self.engine1)
        if ordersDict.empty:
            print('无需要更新订单信息！！！')
            # sys.exit()
            return
        orderId = list(ordersDict['订单编号'])
        print('获取耗时：', datetime.datetime.now() - start)
        max_count = len(orderId)    # 使用len()获取列表的长度，上节学的
        n = 0
        while n < max_count:        # 这里用到了一个while循环，穿越过来的
            ord = ', '.join(orderId[n:n + 10])
            print(ord)
            n = n + 10
            self.orderIdquery(tokenid, ord, searchType, team)
        print('更新耗时：', datetime.datetime.now() - start)

    def orderIdquery(self, tokenid, orderId, searchType, team):  # 进入查询界面，
        start = datetime.datetime.now()
        url = r'http://gimp.giikin.com/service?service=gorder.customer&action=getQueryOrder'
        data = {'phone': None,
                'email': None,
                'ip': None,
                '_token': tokenid}
        if searchType == '订单号':
            data.update({'orderPrefix': orderId,
                         'shippingNumber': None})
        elif searchType == '运单号':
            data.update({'order_number': None,
                         'shippingNumber': orderId})
        r_header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
            'Referer': 'http://gimp.giikin.com/front/orderToolsServiceQuery'}
        req = self.session.post(url=url, headers=r_header, data=data)
        print('已成功发送请求++++++')
        print('正在处理json数据…………')
        req = json.loads(req.text)  # json类型数据转换为dict字典
        print('正在转化数据为dataframe…………')
        # print(req)
        ordersDict = []
        for result in req['data']['list']:
            # print(result)
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
            result['saleName'] = result['specs'][0]['saleName']
            result['saleProduct'] = result['specs'][0]['saleProduct']
            result['spec'] = result['specs'][0]['spec']
            result['link'] = result['specs'][0]['link']
            result['productId'] = (result['specs'][0]['saleProduct']).split('#')[1]
            quest = ''
            for re in result['questionReason']:
                quest = quest + ';' + re
            result['questionReason'] = quest
            delr = ''
            for re in result['delReason']:
                delr = delr + ';' + re
            result['delReason'] = delr
            auto = ''
            for re in result['autoVerify']:
                auto = auto + ';' + re
            result['autoVerify'] = auto
            self.q.put(result)
        # print(len(req['data']['list']))
        for i in range(len(req['data']['list'])):
            ordersDict.append(self.q.get())
        data = pd.json_normalize(ordersDict)
        print('正在写入缓存中......')
        df = data[['orderNumber', 'wayBillNumber', 'logisticsName', 'logisticsStatus', 'orderStatus', 'isSecondSend',
                   'currency', 'area', 'currency', 'shipInfo.shipPhone', 'quantity', 'productId']]
        print(df)
        try:
            df.to_sql('d1_cp', con=self.engine1, index=False, if_exists='replace')
            print('正在更新订单详情…………')
            sql = '''update {0}_order_list a, d1_cp b
                            set a.`数量`= b.`quantity`,
            		            a.`电话号码`=b.`shipInfo.shipPhone` ,
            		            a.`运单编号`=b.`wayBillNumber`,
            		            a.`系统订单状态`= b.`orderStatus`,
            		            a.`系统物流状态`= b.`logisticsStatus`,
            		            a.`是否改派`= IF(b.`isSecondSend`='否', '直发', '改派'),
            		            a.`物流方式`= b.`logisticsName`
            		where a.`订单编号`= b.`orderNumber`;'''.format(team)
            pd.read_sql_query(sql=sql, con=self.engine1, chunksize=1000)
        except Exception as e:
            print('更新失败：', str(Exception) + str(e))
        print('更新成功…………')

if __name__ == '__main__':
    m = QueryControl()
    match1 = {'slgat': '港台',
              'sltg': '泰国',
              'slxmt': '新马',
              'slrb': '日本'}
    # 新系统导入昨天的数据
    # for team in ['sltg', 'slgat', 'slrb', 'slxmt']:
    for team in ['slgat']:
        m.readFormHost(team)

    # for team in ['slgat', 'slrb', 'sltg', 'slxmt']:
    #     tokenid= '3d87b7e525063b4cdb6e61dc52e4c248'
        # m.productIdInfo(tokenid, '订单号', team)

    #   台湾token, 日本token：822c880fa174efd1228cce6802fd8783
    #   新马token, 泰国token：d1d26a93ebd20cc52dd389fe474016e2

    # begin = datetime.date(2021, 2, 1)
    # print(begin)
    # end = datetime.date(2021, 3, 17)
    # print(end)
    # for i in range((end - begin).days):  # 按天循环获取订单状态
    #     day = begin + datetime.timedelta(days=i)
    #     yesterday = str(day) + ' 23:59:59'
    #     last_month = str(day)
    #     print('正在更新 ' + last_month + ' 号订单信息…………')
    #     team = 'slgat'
    #     searchType = '订单号'  # 运单号，订单号   查询切换
    #     tokenid = '3d87b7e525063b4cdb6e61dc52e4c248'
    #     m.orderInfo(tokenid, searchType, team, last_month)
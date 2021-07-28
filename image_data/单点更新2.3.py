import pandas as pd
import os
import datetime
import xlwings

import requests
import json
import time
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
class QueryTwo(Settings):
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

    def getOrderList(self, searchType, team, last_month):  # 进入订单检索界面
        print('第一阶段获取-钉钉用户信息......')
        url = r'https://login.dingtalk.com/login/login_with_pwd'
        data = {'mobile': '+86-18538110674',
                'pwd': 'qyz04163510',
                'goto': 'https://oapi.dingtalk.com/connect/oauth2/sns_authorize?appid=dingoajqpi5bp2kfhekcqm&response_type=code&scope=snsapi_login&state=STATE&redirect_uri=http://gsso.giikin.com/admin/dingtalk_service/getunionidbytempcode',
                'pdmToken': '',
                'araAppkey': '1917',
                'araToken': '0#19171626053345684041338444061627030846321982G1E2B0816DEBF96BC4199761B6A1F3C0FCD91FB',
                'araScene': 'login',
                'captchaImgCode': '',
                'captchaSessionId': '',
                'type': 'h5'}
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Origin': 'https://login.dingtalk.com',
                    'Referer': 'https://login.dingtalk.com/'}
        req = self.session.post(url=url, headers=r_header, data=data, allow_redirects=False)
        req = req.json()
        req_url = req['data']
        loginTmpCode = req_url.split('loginTmpCode=')[1]        # 获取loginTmpCode值
        print(loginTmpCode)
        print('+++已获取loginTmpCode值+++')

        time.sleep(1)
        print('第二阶段请求-登录页面......')
        url = r'http://gsso.giikin.com/admin/dingtalk_service/gettempcodebylogin.html'
        data = {'tmpCode': loginTmpCode,
                'system': 1,
                'url': '',
                'ticker': '',
                'companyId': 1}
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Origin': 'https://login.dingtalk.com',
                    'Referer': 'http://gsso.giikin.com/admin/login/logout.html'}
        req = self.session.post(url=url, headers=r_header, data=data, allow_redirects=False)
        print(req.text)
        print('+++请求登录页面url成功+++')


        time.sleep(1)
        print('第三阶段请求-dingtalk服务器......')
        print('（一）加载dingtalk_service跳转页面......')
        url = req.text
        data = {'tmpCode': loginTmpCode,
                'system': 1,
                'url': '',
                'ticker': '',
                'companyId': 1}
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': 'http://gsso.giikin.com/'}
        req = self.session.get(url=url, headers=r_header, data=data, allow_redirects=False)
        print(req.headers)
        gimp = req.headers['Location']
        print('+++已获取跳转页面+++')
        time.sleep(1)
        print('（二）请求dingtalk_service的cookie值......')
        url = gimp
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': 'http://gsso.giikin.com/'}
        req = self.session.get(url=url, headers=r_header, allow_redirects=False)
        print(req)
        print('+++已获取cookie值+++')


        time.sleep(2)
        print('第四阶段页面-重定向跳转中......')
        print('（一）加载chooselogin.html页面......')
        url = r'http://gsso.giikin.com/admin/login_by_dingtalk/chooselogin.html'
        data = {'user_id': 1343}
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': gimp,
                    'Origin': 'http://gsso.giikin.com'}
        req = self.session.post(url=url, headers=r_header, data=data, allow_redirects=False)
        print(req.headers)
        index = req.headers['Location']
        print('+++已获取gimp.giikin.com页面')
        time.sleep(2)
        print('（二）加载gimp.giikin.com页面......')
        url = index
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': index}
        req = self.session.get(url=url, headers=r_header, allow_redirects=False)
        print(req.headers)
        index2 = req.headers['Location']
        print('+++已获取index.html页面')

        time.sleep(2)
        print('（三）加载index.html页面......')
        url = 'http://gimp.giikin.com/' + index2
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': 'http://gsso.giikin.com/'}
        req = self.session.get(url=url, headers=r_header, allow_redirects=False)
        print(req.headers)
        index_system = req.headers['Location']
        print('+++已获取index.html?_system=18正式页面')


        time.sleep(2)
        print('第五阶段正式页面-重定向跳转中......')
        print('（一）加载index.html?_system页面......')
        url = index_system
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': 'http://gsso.giikin.com/'}
        req = self.session.get(url=url, headers=r_header, allow_redirects=False)
        print(req.headers)
        index_system2 = req.headers['Location']
        print('+++已获取index.html?_ticker=页面......')
        time.sleep(2)
        print('（二）加载index.html?_ticker=页面......')
        url = index_system2
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': 'http://gsso.giikin.com/'}
        req = self.session.get(url=url, headers=r_header, allow_redirects=False)
        print(req)
        print(req.headers)
        print('++++++已成功登录++++++')


        time.sleep(2)
        print('>>>>>>正式查询中<<<<<<')
        start = datetime.datetime.now()
        sql = '''SELECT id,`订单编号`  FROM {0}_order_list sl WHERE sl.`日期` = '{1}';'''.format(team, last_month)
        ordersDict = pd.read_sql_query(sql=sql, con=self.engine1)
        if ordersDict.empty:
            print('无需要更新订单信息！！！')
            return
        orderId = list(ordersDict['订单编号'])
        print('获取耗时：', datetime.datetime.now() - start)
        max_count = len(orderId)  # 使用len()获取列表的长度，上节学的
        n = 0
        while n < max_count:  # 这里用到了一个while循环，穿越过来的
            orderId = ', '.join(orderId[n:n + 500])
            # print(ord)
            n = n + 500
            print('+++正在查询订单信息中')
            url = r'https://gimp.giikin.com/service?service=gorder.customer&action=getOrderList'
            r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                        'origin': 'https: // gimp.giikin.com',
                        'Referer': 'https://gimp.giikin.com/front/orderToolsOrderSearch'}
            data = {'page': 1, 'pageSize': 500,
                    'orderNumberFuzzy': None, 'shipUsername': None, 'phone': None, 'email': None, 'ip': None, 'productIds': None,
                    'saleIds': None, 'payType': None, 'logisticsId': None, 'logisticsStyle': None, 'logisticsMode': None, 'type': None, 'collId': None, 'isClone': None,
                    'currencyId': None, 'emailStatus': None, 'befrom': None, 'areaId': None, 'reassignmentType': None, 'lowerstatus': '',
                    'warehouse': None, 'isEmptyWayBillNumber': None, 'logisticsStatus': None, 'orderStatus': None, 'tuan': None,
                    'tuanStatus': None, 'hasChangeSale': None, 'optimizer': None,'volumeEnd': None, 'volumeStart': None, 'chooser_id': None,
                    'service_id': None, 'autoVerifyStatus': None, 'shipZip': None, 'remark': None, 'shipState': None, 'weightStart': None,
                    'weightEnd': None, 'estimateWeightStart': None, 'estimateWeightEnd': None, 'order': None, 'sortField': None,
                    'orderMark': None, 'remarkCheck': None, 'preSecondWaybill': None, 'whid': None}
            if searchType == '订单号':
                data.update({'orderPrefix': orderId,
                            'shippingNumber': None})
            elif searchType == '运单号':
                data.update({'order_number': None,
                            'shippingNumber': orderId})
            proxy = '39.105.167.0:40005'    # 使用代理服务器
            proxies = {'http': 'socks5://' + proxy,
                        'https': 'socks5://' + proxy}
            # req = self.session.post(url=url, headers=r_header, data=data, proxies=proxies)
            req = self.session.post(url=url, headers=r_header, data=data)
            # print(req.text)
            print('+++已成功发送请求......')
            print('正在处理json数据转化为dataframe…………')
            req = json.loads(req.text)  # json类型数据转换为dict字典
            # print(req)
            ordersDict = []
            for result in req['data']['list']:
                # print(result)
                try:
                    # 添加新的字典键-值对，为下面的重新赋值用
                    result['productId'] = 0
                    # result['saleName'] = 0
                    result['saleProduct'] = 0
                    result['spec'] = 0
                    result['link'] = 0
                    # result['saleName'] = result['specs'][0]['saleName']
                    result['saleProduct'] = result['specs'][0]['saleProduct']
                    result['spec'] = result['specs'][0]['spec']
                    result['link'] = result['specs'][0]['link']
                    result['productId'] = (result['specs'][0]['saleProduct']).split('#')[1]
                except Exception as e:
                    print('转化失败：', str(Exception) + str(e) + str(result['orderNumber']))
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
            try:
                df = data[['orderNumber', 'currency', 'area', 'productId', 'quantity', 'shipInfo.shipPhone', 'wayBillNumber',
                        'orderStatus', 'logisticsStatus', 'logisticsName', 'addTime', 'logisticsUpdateTime', 'onlineTime', 'finishTime', 'transferTime',
                        'deliveryTime', 'reassignmentTypeName', 'dpeStyle', 'amount']]
                print(df)
                print('正在更新临时表中......')
                df.to_sql('d1_cpy', con=self.engine1, index=False, if_exists='replace')
                sql = '''SELECT DATE(h.addTime) 日期,
            				    IF(h.`currency` = '日币', '日本', IF(h.`currency` = '泰铢', '泰国', IF(h.`currency` = '港币', '香港', IF(h.`currency` = '台币', '台湾', IF(h.`currency` = '韩元', '韩国', h.`currency`))))) 币种,
            				    h.orderNumber 订单编号,
            				    h.quantity 数量,
            				    h.`shipInfo.shipPhone` 电话号码,
            				    h.wayBillNumber 运单编号,
            				    h.orderStatus 系统订单状态,
            				    IF(h.`logisticsStatus` in ('发货中'), '在途', h.`logisticsStatus`) 系统物流状态,
            				    IF(h.`reassignmentTypeName` in ('未下架未改派','直发下架'), '直发', '改派') 是否改派,
            				    TRIM(h.logisticsName) 物流方式,
            				    dim_trans_way.simple_name 物流名称,
            				    IF(h.`dpeStyle` = 'P 普通货', 'P', IF(h.`dpeStyle` = 'T 特殊货', 'T', h.`dpeStyle`)) 货物类型,
            				    h.transferTime 审核时间,
            				    h.onlineTime 上线时间,
            				    h.deliveryTime 仓储扫描时间,
            				    h.finishTime 完结状态时间
                            FROM d1_cpy h
                                LEFT JOIN dim_product ON  dim_product.id = h.productId
                                LEFT JOIN dim_cate ON  dim_cate.id = dim_product.third_cate_id
                                LEFT JOIN dim_trans_way ON  dim_trans_way.all_name = TRIM(h.logisticsName);'''.format(team)
                df = pd.read_sql_query(sql=sql, con=self.engine1)
                df.to_sql('d1_cpy_cp', con=self.engine1, index=False, if_exists='replace')
                # print('正在更新表总表中......')
                # sql = '''update {0}_order_list a, d1_cpy_cp b
                #             set a.`币种`= b.`币种`,
                #                 a.`数量`= b.`数量`,
                #                 a.`电话号码`= b.`电话号码` ,
                #                 a.`运单编号`= IF(b.`运单编号` = '', NULL, b.`运单编号`),
                #                 a.`系统订单状态`= IF(b.`系统订单状态` = '', NULL, b.`系统订单状态`),
                #                 a.`系统物流状态`= IF(b.`系统物流状态` = '', NULL, b.`系统物流状态`),
                #                 a.`是否改派`= b.`是否改派`,
                #                 a.`物流方式`= IF(b.`物流方式` = '',NULL, b.`物流方式`),
                #                 a.`物流名称`= b.`物流名称`,
                #                 a.`货物类型`= IF(b.`货物类型` = '', NULL, b.`货物类型`),
                #                 a.`审核时间`= b.`审核时间`,
                #                 a.`上线时间`= b.`上线时间`,
                #                 a.`仓储扫描时间`= b.`仓储扫描时间`,
                #                 a.`完结状态时间`= b.`完结状态时间`
                #     where a.`订单编号`=b.`订单编号`;'''.format(team)
                # pd.read_sql_query(sql=sql, con=self.engine1, chunksize=1000)
            except Exception as e:
                print('更新失败：', str(Exception) + str(e))
            print('++++++本批次更新成功+++++++')

if __name__ == '__main__':
    m = QueryTwo()
    start: datetime = datetime.datetime.now()


    # -----------------------------------------------单个查询测试使用（三）-----------------------------------------
    team = 'gat'              # ['slgat', 'slgat_hfh', 'slrb', 'sltg', 'slxmt', 'slxmt_hfh']
    searchType = '订单号'      # 运单号，订单号   查询切换
    last_month = '2021-05-01'
    m.getOrderList(searchType, team, last_month)


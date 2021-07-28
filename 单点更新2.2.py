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

    def Info(self, tokenid, orderId, searchType, team):  # 进入订单检索界面
        print('第一阶段获取获取loginTmpCode值......')
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
        # 获取loginTmpCode值
        req = req.json()
        print(req)
        print(00)
        req_url = req['data']
        print(req_url)
        print('+++已成功......')
        loginTmpCode = req_url.split('loginTmpCode=')[1]
        print(loginTmpCode)
        print('+++已成功发送请求......')

        time.sleep(1)
        print('第二阶段获取获取  值......')
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
        # 获取loginTmpCode值
        print(req)
        print(req.text)
        print(33)
        print(req.cookies)
        print('+++已成功......')







        time.sleep(1)
        print('第三阶段获取获取  值......')
        url = req.text
        data = {'tmpCode': loginTmpCode,
                'system': 1,
                'url': '',
                'ticker': '',
                'companyId': 1}
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': 'http://gsso.giikin.com/'}
        req = self.session.get(url=url, headers=r_header, data=data, allow_redirects=False)
        # 获取loginTmpCode值
        print(req)
        print(req.text)
        print(44)
        print(req.headers)
        print(req.headers['Location'])
        gimp = req.headers['Location']
        print('+++已成功......')
        time.sleep(1)
        print('第四阶段获取获取  值......')
        url = gimp
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': 'http://gsso.giikin.com/'}
        req = self.session.get(url=url, headers=r_header, allow_redirects=False)
        # 获取loginTmpCode值
        print(req)
        # print(req.text)
        print(55)
        print('+++已成功......')



        time.sleep(2)
        print('第五阶段获取获取  值......')
        url = r'http://gsso.giikin.com/admin/login_by_dingtalk/chooselogin.html'
        data = {'user_id': 1343}
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': gimp,
                    'Origin': 'http://gsso.giikin.com'}
        req = self.session.post(url=url, headers=r_header, data=data, allow_redirects=False)
        # 获取loginTmpCode值
        print(req)
        # print(req.text)
        print(66)
        print(req.headers)
        print(req.headers['Location'])
        index = req.headers['Location']
        print('+++已成功......')
        time.sleep(2)
        print('第六阶段获取获取  值......')
        url = index
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': index}
        req = self.session.get(url=url, headers=r_header, allow_redirects=False)
        # 获取loginTmpCode值
        print(req)
        print(77)
        print(req.headers)
        print(req.headers['Location'])
        index2 = req.headers['Location']
        print('+++已成功......')
        time.sleep(2)
        print('第七阶段获取获取  值......')
        url = 'http://gimp.giikin.com/' + index2
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': 'http://gsso.giikin.com/'}
        req = self.session.get(url=url, headers=r_header, allow_redirects=False)
        # 获取loginTmpCode值
        print(req)
        print(88)
        print(req.headers)
        print(req.headers['Location'])
        index_system = req.headers['Location']
        print('+++已成功......')

        time.sleep(2)
        print('第八阶段获取获取  值......')
        url = index_system
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': 'http://gsso.giikin.com/'}
        req = self.session.get(url=url, headers=r_header, allow_redirects=False)
        # 获取loginTmpCode值
        print(req)
        print(99)
        print(req.headers)
        print(req.headers['Location'])
        index_system2 = req.headers['Location']
        print('+++已成功......')
        time.sleep(2)
        print('第九阶段获取获取  值......')
        url = index_system2
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Referer': 'http://gsso.giikin.com/'}
        req = self.session.get(url=url, headers=r_header, allow_redirects=False)
        # 获取loginTmpCode值
        print(req)
        print(req.text)
        print(100)
        print(req.headers)
        print('+++已成功......')


        time.sleep(2)
        print('第十阶段查询中......')
        url = r'https://gimp.giikin.com/service?service=gorder.customer&action=getOrderList'
        r_header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'origin': 'https: // gimp.giikin.com',
                    'Referer': 'https://gimp.giikin.com/front/orderToolsOrderSearch'}
        data = {'page': 1, 'pageSize': 10,
                'orderPrefix': 'NR107241605153931',
                'orderNumberFuzzy': '', 'shipUsername': '', 'phone': '', 'shippingNumber': '', 'email': '', 'ip': '', 'productIds': '',
                'saleIds': '','payType': '', 'logisticsId': '', 'logisticsStyle': '','logisticsMode': '', 'type': '', 'collId': '', 'isClone': '',
                'currencyId': '', 'emailStatus': '', 'befrom': '', 'areaId': '', 'reassignmentType': '', 'lowerstatus': '',
                'warehouse': '', 'isEmptyWayBillNumber': '', 'logisticsStatus': '', 'orderStatus': '', 'tuan': '',
                'tuanStatus': '', 'hasChangeSale': '', 'optimizer': '','volumeEnd': '', 'volumeStart': '', 'chooser_id': '',
                'service_id': '', 'autoVerifyStatus': '', 'shipZip': '', 'remark': '', 'shipState': '', 'weightStart': '',
                'weightEnd': '', 'estimateWeightStart': '', 'estimateWeightEnd': '', 'order': '', 'sortField': '',
                'orderMark': '', 'remarkCheck': '', 'preSecondWaybill': '', 'whid': ''
                }
        req = self.session.post(url=url, headers=r_header, allow_redirects=False)
        # 获取loginTmpCode值
        print(req)
        print(req.text)
        print(110)
        print(req.headers)
        print('+++已成功......')




if __name__ == '__main__':
    m = QueryTwo()
    start: datetime = datetime.datetime.now()
    match1 = {'slgat': '神龙-港台',
             'slgat_hfh': '火凤凰-港台',
             'slgat_hs': '红杉-港台',
             'sltg': '神龙-泰国',
             'slrb': '神龙-日本',
             'slrb_jl': '精灵-日本',
             'slrb_js': '金狮-日本',
             'slxmt': '神龙-新马',
             'slxmt_t': '神龙-T新马',
             'slxmt_hfh': '火凤凰-新马'}

    # -----------------------------------------------单个查询测试使用（三）-----------------------------------------
    team = 'slgat'              # ['slgat', 'slgat_hfh', 'slrb', 'sltg', 'slxmt', 'slxmt_hfh']
    searchType = '订单号'      # 运单号，订单号   查询切换
    tokenid = '09f5f0d91a3b33a7d358470ca8aaad2a'
    # tickerid = '255755c8118a841aba0f9643b49363d3'
    # m.orderInfoQuery(tokenid, tickerid, 'NR103021446315734', searchType, team)
    # m.orderInfoQuery(tokenid, '1599128016', searchType, team)
    m.Info(tokenid, '1599128016', searchType, team)

    # last_month = '2021-03-18'
    # m.orderInfo(tokenid, searchType, team, last_month)

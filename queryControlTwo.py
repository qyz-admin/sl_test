import pandas as pd
import os
import datetime
import xlwings as xl

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

    def writeSqlReplace(self, dataFrame):
        dataFrame.to_sql('tem', con=self.engine1, index=False, if_exists='replace')

    def replaceInto(self, team, dfColumns):
        columns = list(dfColumns)
        columns = ', '.join(columns)
        if team == 'slrb':
            print(team + '---9')
            sql = 'REPLACE INTO {}({}, 添加时间) SELECT *, NOW() 添加时间 FROM tem; '.format(team, columns)
        else:
            print(team)
            sql = 'INSERT IGNORE INTO {}({}, 添加时间) SELECT *, NOW() 添加时间 FROM tem; '.format(team, columns)
        try:
            pd.read_sql_query(sql=sql, con=self.engine1, chunksize=100)
        except Exception as e:
            print('插入失败：', str(Exception) + str(e))

    def readSql(self, sql):
        db = pd.read_sql_query(sql=sql, con=self.engine1)
        # db = pd.read_sql(sql=sql, con=self.engine1) or team == 'slgat'
        return db

    # 更新团队品类明细（新后台的第二部分）
    def cateIdInfo(self, tokenid, team):  # 进入产品检索界面，
        print('正在获取需要更新的产品id信息')
        start = datetime.datetime.now()
        month_begin = (datetime.datetime.now() - relativedelta(months=2)).strftime('%Y-%m-%d')
        sql = '''SELECT id,`订单编号`, `产品id` , null 父级分类, null 二级分类, null 三级分类 FROM {0}_order_list sl 
    			WHERE sl.`日期`> '{1}' AND (sl.`父级分类` IS NULL or sl.`父级分类` = '')
    				AND ( NOT sl.`系统订单状态` IN ('已删除','问题订单','支付失败','未支付'));'''.format(team, month_begin)
        ordersDict = pd.read_sql_query(sql=sql, con=self.engine1)
        ordersDict.to_sql('d1_cp_cate', con=self.engine1, index=False, if_exists='replace')  # 写入临时品类缓存表中
        if ordersDict.empty:
            print('无需要更新的品类id信息！！！')
            return
        orderId = list(ordersDict['产品id'])
        orderId = [str(i) for i in orderId]  # join函数就是字符串的函数,参数和插入的都要是字符串
        print('获取耗时：', datetime.datetime.now() - start)
        max_count = len(orderId)    # 使用len()获取列表的长度，上节学的
        n = 0
        while n < max_count:        # 这里用到了一个while循环，穿越过来的
            cateid = ', '.join(orderId[n:n + 90])
            print(cateid)
            n = n + 90
            self.cateIdquery(tokenid, cateid, team)


    def cateIdquery(self):  # 进入产品检索界面，
        start = datetime.datetime.now()
        # productid = '508746'
        # token = '7dd7c0085722cf49493c5ab2ecbc6234'
        data = {'page': 1,
                'pageSize': 10,
                'orderPrefix': 'NA210629113713WCS6F5',
                'phone': None,
                'email': None,
                'ip': None,
                'PHPSESSID': 'c97d2uvvtq9qgepe49t9h20mmr'}
        url = 'https://gimp.giikin.com/service?service=gorder.customer&action=getOrderList'
        r_header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Referer': 'http://gsso.giikin.com/'}
        # rq = requests.get(url=url, headers=r_header)
        rq = requests.post(url=url, headers=r_header,data=data)
        print(rq)
        print(rq.text)
        print(rq.headers)
        print('已成功发送请求++++++')
        # req = rq.json()  # json类型数据



if __name__ == '__main__':
    m = QueryControl()
    match1 = {'slgat': '港台',
              'sltg': '泰国',
              'slxmt': '新马',
              'slzb': '直播团队',
              'slyn': '越南',
              'slrb': '日本'}
    # messagebox.showinfo("提示！！！", "当前查询已完成--->>> 请前往（ 输出文件 ）查看")
    #  各团队全部订单表-函数
    # m.tgOrderQuan('sltg')

    # team = 'slgat'
    # for tem in ['台湾', '香港']:
    #     m.OrderQuan(team, tem)

    #  订单花费明细查询
    # match9 = {'slgat_zqsb': '港台',
    #           'sltg_zqsb': '泰国',
    #           'slxmt_zqsb': '新马',
    #           'slrb_zqsb_rb': '日本'}
    # team = 'sltg_zqsb'
    # m.sl_tem_cost(team, match9[team])

    team = 'slgat'  # 第一部分查询
    token = 'aa57bf4a0cdc0fbfcf1f093732b96005'
    pro = '508746'
    m.cateIdquery()
    # m.productIdInfo(token, '订单号', team)
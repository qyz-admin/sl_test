import pandas as pd
from sqlalchemy import create_engine
from settings import Settings
from emailControl import EmailControl
from tkinter import messagebox

import datetime
class MysqlControl(Settings):
    def __init__(self):
        Settings.__init__(self)
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
    def creatMyOrderrrrrrrr(self, team):
        match = {'slgat': '"神龙家族-港澳台"',
                 'sltg': '"神龙家族-泰国"',
                 'slxmt': '"神龙家族-新加坡", "神龙家族-马来西亚"',
                 'slzb': '"神龙家族-直播团队"',
                 'slyn': '"神龙家族-越南"',
                 'slrb': '"神龙家族-日本团队"'}
        # 第一部分查询
        yesterday = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-10'
        # yesterday = '2020-09-27 23:59:59'
        last_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-01'
        # last_month = '2020-09-20 00:00:00'
        sql = '''SELECT a.id, 
			a.month 年月, 
			a.month_mid 旬, 
			a.rq 日期, 
			dim_area.name 团队, 
			a.region_code 区域,
			dim_currency_lang.pname 币种,
			a.beform 订单来源, 
			a.order_number 订单编号, 
			a.ship_phone 电话号码, 
			UPPER(a.waybill_number) 运单编号,
			a.order_status 系统订单状态id, 
			a.logistics_status 系统物流状态id, 
			IF(a.second=0,'直发','改派') 是否改派, 
			dim_trans_way.all_name 物流方式,
			dim_trans_way.simple_name 物流名称,
			dim_trans_way.remark 运输方式,
			a.logistics_type 货物类型,
			IF(a.low_price=0,'否','是') 是否低价, 
			a.product_id 产品id, 
			gk_product.name 产品名称, 
			dim_cate.ppname 父级分类,
			dim_cate.pname 二级分类,
			dim_cate.name 三级分类,
			dim_payment.pay_name 付款方式, 
			a.amount 价格, 
			a.addtime 下单时间, 
			a.verity_time 审核时间, 
			a.delivery_time 仓储扫描时间, 
			a.finish_status 完结状态, 
			a.endtime 完结状态时间, 
			a.salesRMB 价格RMB, 
			intervals.intervals 价格区间,
			a.purchase 成本价, 
			a.logistics_cost 物流花费, 
			a.package_cost 打包花费, 
			a.other_fee 其它花费, 
			a.weight 包裹重量,
			a.volume 包裹体积,
			a.ship_zip 邮编
FROM 
		gk_order a left join dim_area ON dim_area.id = a.area_id 
                   left join dim_payment on dim_payment.id = a.payment_id
                   left join gk_product on gk_product.id = a.product_id 
                   left join dim_trans_way on dim_trans_way.id = a.logistics_id
                   left join dim_cate on dim_cate.id = gk_product.third_cate_id 
                   left join intervals on intervals.id = a.intervals
                   left join dim_currency_lang on dim_currency_lang.id = a.currency_lang_id
WHERE 
	a.rq >= '{}' AND (a.verity_time <= '{}' OR ISNULL(a.verity_time)) 
	AND dim_area.name IN ({});'''.format(last_month, yesterday, match[team])
        print('正在获取上月第一个10天的订单…………')
        df = pd.read_sql_query(sql=sql, con=self.engine2)
        print('----已获取上月第一个10天的订单')
        sql = 'SELECT * FROM dim_order_status;'
        df1 = pd.read_sql_query(sql=sql, con = self.engine1)
        print('----已获取订单状态')
        print('正在合并订单状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统订单状态id', right_on='id', how='left')
        print('----已合并订单状态')
        sql = 'SELECT * FROM dim_logistics_status;'
        df1 = pd.read_sql_query(sql=sql, con=self.engine1)
        print('----已获取物流状态')
        print('正在合并物流状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统物流状态id', right_on='id', how='left')
        df = df.drop(labels=['id', 'id_y', '系统订单状态id', '系统物流状态id'], axis=1)
        df.rename(columns={'id_x': 'id', 'name_x': '系统订单状态', 'name_y': '系统物流状态'}, inplace=True)
        print('----已获取近两个月订单与物流状态')
        # self.reSetEngine()
        print('正在写入数据库…………')
        # 这一句会报错,需要修改my.ini文件中的[mysqld]段中的"max_allowed_packet = 1024M"
        df.to_sql(team + '_order_list', con=self.engine1, index=False, if_exists='replace')
        print('----已写入数据库')
        # return '写入完成'
        # 第二部分查询
        yesterday = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-20' + ' 23:59:59'
        # yesterday = '2020-09-27 23:59:59'
        last_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-10' + ' 00:00:00'
        # last_month = '2020-09-20 00:00:00'
        sql = '''SELECT a.id, 
            a.month 年月, 
            a.month_mid 旬, 
            a.rq 日期, 
            dim_area.name 团队, 
            a.region_code 区域,
            dim_currency_lang.pname 币种,
            a.beform 订单来源, 
            a.order_number 订单编号, 
            a.ship_phone 电话号码, 
            UPPER(a.waybill_number) 运单编号,
            a.order_status 系统订单状态id, 
            a.logistics_status 系统物流状态id, 
            IF(a.second=0,'直发','改派') 是否改派, 
            dim_trans_way.all_name 物流方式,
            dim_trans_way.simple_name 物流名称,
            dim_trans_way.remark 运输方式,
            a.logistics_type 货物类型,
            IF(a.low_price=0,'否','是') 是否低价, 
            a.product_id 产品id, 
            gk_product.name 产品名称, 
            dim_cate.ppname 父级分类,
            dim_cate.pname 二级分类,
            dim_cate.name 三级分类,
            dim_payment.pay_name 付款方式, 
            a.amount 价格, 
            a.addtime 下单时间, 
            a.verity_time 审核时间, 
            a.delivery_time 仓储扫描时间, 
            a.finish_status 完结状态, 
            a.endtime 完结状态时间, 
            a.salesRMB 价格RMB, 
            intervals.intervals 价格区间,
            a.purchase 成本价, 
            a.logistics_cost 物流花费, 
            a.package_cost 打包花费, 
            a.other_fee 其它花费, 
            a.weight 包裹重量,
            a.volume 包裹体积,
            a.ship_zip 邮编
FROM 
        gk_order a left join dim_area ON dim_area.id = a.area_id 
                   left join dim_payment on dim_payment.id = a.payment_id
                   left join gk_product on gk_product.id = a.product_id 
                   left join dim_trans_way on dim_trans_way.id = a.logistics_id
                   left join dim_cate on dim_cate.id = gk_product.third_cate_id 
                   left join intervals on intervals.id = a.intervals
                   left join dim_currency_lang on dim_currency_lang.id = a.currency_lang_id
WHERE 
    a.rq >= '{}' AND (a.verity_time <= '{}' OR ISNULL(a.verity_time)) 
    AND dim_area.name IN ({});'''.format(last_month, yesterday, match[team])
        print('正在获取上月第二个10天的订单…………')
        df = pd.read_sql_query(sql=sql, con=self.engine2)
        print('----已获取上月第二个10天的订单')
        sql = 'SELECT * FROM dim_order_status;'
        df1 = pd.read_sql_query(sql=sql, con = self.engine1)
        print('----已获取订单状态')
        print('正在合并订单状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统订单状态id', right_on='id', how='left')
        print('----已合并订单状态')
        sql = 'SELECT * FROM dim_logistics_status;'
        df1 = pd.read_sql_query(sql=sql, con=self.engine1)
        print('----已获取物流状态')
        print('正在合并物流状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统物流状态id', right_on='id', how='left')
        df = df.drop(labels=['id', 'id_y', '系统订单状态id', '系统物流状态id'], axis=1)
        df.rename(columns={'id_x': 'id', 'name_x': '系统订单状态', 'name_y': '系统物流状态'}, inplace=True)
        print('----已获取近两个月订单与物流状态')
        # self.reSetEngine()
        print('正在写入数据库…………')
        # 这一句会报错,需要修改my.ini文件中的[mysqld]段中的"max_allowed_packet = 1024M"
        df.to_sql(team + '_order_list', con=self.engine1, index=False, if_exists='replace')
        print('----已写入数据库')
        # return '写入完成'
        # 第三部分查询
        yesterday = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m-%d') + ' 23:59:59'
        # yesterday = '2020-09-27 23:59:59'
        last_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-20' + ' 00:00:00'
        # last_month = '2020-09-20 00:00:00'
        sql = '''SELECT a.id, 
            a.month 年月, 
            a.month_mid 旬, 
            a.rq 日期, 
            dim_area.name 团队, 
            a.region_code 区域,
            dim_currency_lang.pname 币种,
            a.beform 订单来源, 
            a.order_number 订单编号, 
            a.ship_phone 电话号码, 
            UPPER(a.waybill_number) 运单编号,
            a.order_status 系统订单状态id, 
            a.logistics_status 系统物流状态id, 
            IF(a.second=0,'直发','改派') 是否改派, 
            dim_trans_way.all_name 物流方式,
            dim_trans_way.simple_name 物流名称,
            dim_trans_way.remark 运输方式,
            a.logistics_type 货物类型,
            IF(a.low_price=0,'否','是') 是否低价, 
            a.product_id 产品id, 
            gk_product.name 产品名称, 
            dim_cate.ppname 父级分类,
            dim_cate.pname 二级分类,
            dim_cate.name 三级分类,
            dim_payment.pay_name 付款方式, 
            a.amount 价格, 
            a.addtime 下单时间, 
            a.verity_time 审核时间, 
            a.delivery_time 仓储扫描时间, 
            a.finish_status 完结状态, 
            a.endtime 完结状态时间, 
            a.salesRMB 价格RMB, 
            intervals.intervals 价格区间,
            a.purchase 成本价, 
            a.logistics_cost 物流花费, 
            a.package_cost 打包花费, 
            a.other_fee 其它花费, 
            a.weight 包裹重量,
            a.volume 包裹体积,
            a.ship_zip 邮编
FROM 
        gk_order a left join dim_area ON dim_area.id = a.area_id 
                   left join dim_payment on dim_payment.id = a.payment_id
                   left join gk_product on gk_product.id = a.product_id 
                   left join dim_trans_way on dim_trans_way.id = a.logistics_id
                   left join dim_cate on dim_cate.id = gk_product.third_cate_id 
                   left join intervals on intervals.id = a.intervals
                   left join dim_currency_lang on dim_currency_lang.id = a.currency_lang_id
WHERE 
    a.rq >= '{}' AND (a.verity_time <= '{}' OR ISNULL(a.verity_time)) 
    AND dim_area.name IN ({});'''.format(last_month, yesterday, match[team])
        print('正在获取上月第三个10天的订单…………')
        df = pd.read_sql_query(sql=sql, con=self.engine2)
        print('----已获取上月第三个10天的订单')
        sql = 'SELECT * FROM dim_order_status;'
        df1 = pd.read_sql_query(sql=sql, con = self.engine1)
        print('----已获取订单状态')
        print('正在合并订单状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统订单状态id', right_on='id', how='left')
        print('----已合并订单状态')
        sql = 'SELECT * FROM dim_logistics_status;'
        df1 = pd.read_sql_query(sql=sql, con=self.engine1)
        print('----已获取物流状态')
        print('正在合并物流状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统物流状态id', right_on='id', how='left')
        df = df.drop(labels=['id', 'id_y', '系统订单状态id', '系统物流状态id'], axis=1)
        df.rename(columns={'id_x': 'id', 'name_x': '系统订单状态', 'name_y': '系统物流状态'}, inplace=True)
        print('----已获取近两个月订单与物流状态')
        # self.reSetEngine()
        print('正在写入数据库…………')
        # 这一句会报错,需要修改my.ini文件中的[mysqld]段中的"max_allowed_packet = 1024M"
        df.to_sql(team + '_order_list', con=self.engine1, index=False, if_exists='replace')
        print('----已写入数据库')
        # return '写入完成'
        # 第四部分查询
        yesterday = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m') + '-10' + ' 23:59:59'
        # yesterday = '2020-09-27 23:59:59'
        last_month = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m-%d') + ' 00:00:00'
        # last_month = '2020-09-20 00:00:00'
        sql = '''SELECT a.id, 
            a.month 年月, 
            a.month_mid 旬, 
            a.rq 日期, 
            dim_area.name 团队, 
            a.region_code 区域,
            dim_currency_lang.pname 币种,
            a.beform 订单来源, 
            a.order_number 订单编号, 
            a.ship_phone 电话号码, 
            UPPER(a.waybill_number) 运单编号,
            a.order_status 系统订单状态id, 
            a.logistics_status 系统物流状态id, 
            IF(a.second=0,'直发','改派') 是否改派, 
            dim_trans_way.all_name 物流方式,
            dim_trans_way.simple_name 物流名称,
            dim_trans_way.remark 运输方式,
            a.logistics_type 货物类型,
            IF(a.low_price=0,'否','是') 是否低价, 
            a.product_id 产品id, 
            gk_product.name 产品名称, 
            dim_cate.ppname 父级分类,
            dim_cate.pname 二级分类,
            dim_cate.name 三级分类,
            dim_payment.pay_name 付款方式, 
            a.amount 价格, 
            a.addtime 下单时间, 
            a.verity_time 审核时间, 
            a.delivery_time 仓储扫描时间, 
            a.finish_status 完结状态, 
            a.endtime 完结状态时间, 
            a.salesRMB 价格RMB, 
            intervals.intervals 价格区间,
            a.purchase 成本价, 
            a.logistics_cost 物流花费, 
            a.package_cost 打包花费, 
            a.other_fee 其它花费, 
            a.weight 包裹重量,
            a.volume 包裹体积,
            a.ship_zip 邮编
FROM 
        gk_order a left join dim_area ON dim_area.id = a.area_id 
                   left join dim_payment on dim_payment.id = a.payment_id
                   left join gk_product on gk_product.id = a.product_id 
                   left join dim_trans_way on dim_trans_way.id = a.logistics_id
                   left join dim_cate on dim_cate.id = gk_product.third_cate_id 
                   left join intervals on intervals.id = a.intervals
                   left join dim_currency_lang on dim_currency_lang.id = a.currency_lang_id
WHERE 
    a.rq >= '{}' AND (a.verity_time <= '{}' OR ISNULL(a.verity_time)) 
    AND dim_area.name IN ({});'''.format(last_month, yesterday, match[team])
        print('正在获取本月第一个10天的订单…………')
        df = pd.read_sql_query(sql=sql, con=self.engine2)
        print('----已获取本月第一个10天的订单')
        sql = 'SELECT * FROM dim_order_status;'
        df1 = pd.read_sql_query(sql=sql, con = self.engine1)
        print('----已获取订单状态')
        print('正在合并订单状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统订单状态id', right_on='id', how='left')
        print('----已合并订单状态')
        sql = 'SELECT * FROM dim_logistics_status;'
        df1 = pd.read_sql_query(sql=sql, con=self.engine1)
        print('----已获取物流状态')
        print('正在合并物流状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统物流状态id', right_on='id', how='left')
        df = df.drop(labels=['id', 'id_y', '系统订单状态id', '系统物流状态id'], axis=1)
        df.rename(columns={'id_x': 'id', 'name_x': '系统订单状态', 'name_y': '系统物流状态'}, inplace=True)
        print('----已获取近两个月订单与物流状态')
        # self.reSetEngine()
        print('正在写入数据库…………')
        # 这一句会报错,需要修改my.ini文件中的[mysqld]段中的"max_allowed_packet = 1024M"
        df.to_sql(team + '_order_list', con=self.engine1, index=False, if_exists='replace')
        print('----已写入数据库')
        # return '写入完成'
        # 第五部分查询
        yesterday = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m') + '-20' + ' 23:59:59'
        # yesterday = '2020-09-27 23:59:59'
        last_month = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m') + '-10' + ' 00:00:00'
        # last_month = '2020-09-20 00:00:00'
        sql = '''SELECT a.id, 
            a.month 年月, 
            a.month_mid 旬, 
            a.rq 日期, 
            dim_area.name 团队, 
            a.region_code 区域,
            dim_currency_lang.pname 币种,
            a.beform 订单来源, 
            a.order_number 订单编号, 
            a.ship_phone 电话号码, 
            UPPER(a.waybill_number) 运单编号,
            a.order_status 系统订单状态id, 
            a.logistics_status 系统物流状态id, 
            IF(a.second=0,'直发','改派') 是否改派, 
            dim_trans_way.all_name 物流方式,
            dim_trans_way.simple_name 物流名称,
            dim_trans_way.remark 运输方式,
            a.logistics_type 货物类型,
            IF(a.low_price=0,'否','是') 是否低价, 
            a.product_id 产品id, 
            gk_product.name 产品名称, 
            dim_cate.ppname 父级分类,
            dim_cate.pname 二级分类,
            dim_cate.name 三级分类,
            dim_payment.pay_name 付款方式, 
            a.amount 价格, 
            a.addtime 下单时间, 
            a.verity_time 审核时间, 
            a.delivery_time 仓储扫描时间, 
            a.finish_status 完结状态, 
            a.endtime 完结状态时间, 
            a.salesRMB 价格RMB, 
            intervals.intervals 价格区间,
            a.purchase 成本价, 
            a.logistics_cost 物流花费, 
            a.package_cost 打包花费, 
            a.other_fee 其它花费, 
            a.weight 包裹重量,
            a.volume 包裹体积,
            a.ship_zip 邮编
FROM 
        gk_order a left join dim_area ON dim_area.id = a.area_id 
                   left join dim_payment on dim_payment.id = a.payment_id
                   left join gk_product on gk_product.id = a.product_id 
                   left join dim_trans_way on dim_trans_way.id = a.logistics_id
                   left join dim_cate on dim_cate.id = gk_product.third_cate_id 
                   left join intervals on intervals.id = a.intervals
                   left join dim_currency_lang on dim_currency_lang.id = a.currency_lang_id
WHERE 
    a.rq >= '{}' AND (a.verity_time <= '{}' OR ISNULL(a.verity_time)) 
    AND dim_area.name IN ({});'''.format(last_month, yesterday, match[team])
        print('正在获取本月第二个10天的订单…………')
        df = pd.read_sql_query(sql=sql, con=self.engine2)
        print('----已获取本月第二个10天的订单')
        sql = 'SELECT * FROM dim_order_status;'
        df1 = pd.read_sql_query(sql=sql, con = self.engine1)
        print('----已获取订单状态')
        print('正在合并订单状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统订单状态id', right_on='id', how='left')
        print('----已合并订单状态')
        sql = 'SELECT * FROM dim_logistics_status;'
        df1 = pd.read_sql_query(sql=sql, con=self.engine1)
        print('----已获取物流状态')
        print('正在合并物流状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统物流状态id', right_on='id', how='left')
        df = df.drop(labels=['id', 'id_y', '系统订单状态id', '系统物流状态id'], axis=1)
        df.rename(columns={'id_x': 'id', 'name_x': '系统订单状态', 'name_y': '系统物流状态'}, inplace=True)
        print('----已获取近两个月订单与物流状态')
        # self.reSetEngine()
        print('正在写入数据库…………')
        # 这一句会报错,需要修改my.ini文件中的[mysqld]段中的"max_allowed_packet = 1024M"
        df.to_sql(team + '_order_list', con=self.engine1, index=False, if_exists='replace')
        print('----已写入数据库')
        return '写入完成'
        # 第六部分查询
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d') + ' 23:59:59'
        # yesterday = '2020-09-27 23:59:59'
        last_month = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m') + '-20' + ' 00:00:00'
        # last_month = '2020-09-20 00:00:00'
        sql = '''SELECT a.id, 
            a.month 年月, 
            a.month_mid 旬, 
            a.rq 日期, 
            dim_area.name 团队, 
            a.region_code 区域,
            dim_currency_lang.pname 币种,
            a.beform 订单来源, 
            a.order_number 订单编号, 
            a.ship_phone 电话号码, 
            UPPER(a.waybill_number) 运单编号,
            a.order_status 系统订单状态id, 
            a.logistics_status 系统物流状态id, 
            IF(a.second=0,'直发','改派') 是否改派, 
            dim_trans_way.all_name 物流方式,
            dim_trans_way.simple_name 物流名称,
            dim_trans_way.remark 运输方式,
            a.logistics_type 货物类型,
            IF(a.low_price=0,'否','是') 是否低价, 
            a.product_id 产品id, 
            gk_product.name 产品名称, 
            dim_cate.ppname 父级分类,
            dim_cate.pname 二级分类,
            dim_cate.name 三级分类,
            dim_payment.pay_name 付款方式, 
            a.amount 价格, 
            a.addtime 下单时间, 
            a.verity_time 审核时间, 
            a.delivery_time 仓储扫描时间, 
            a.finish_status 完结状态, 
            a.endtime 完结状态时间, 
            a.salesRMB 价格RMB, 
            intervals.intervals 价格区间,
            a.purchase 成本价, 
            a.logistics_cost 物流花费, 
            a.package_cost 打包花费, 
            a.other_fee 其它花费, 
            a.weight 包裹重量,
            a.volume 包裹体积,
            a.ship_zip 邮编
FROM 
        gk_order a left join dim_area ON dim_area.id = a.area_id 
                   left join dim_payment on dim_payment.id = a.payment_id
                   left join gk_product on gk_product.id = a.product_id 
                   left join dim_trans_way on dim_trans_way.id = a.logistics_id
                   left join dim_cate on dim_cate.id = gk_product.third_cate_id 
                   left join intervals on intervals.id = a.intervals
                   left join dim_currency_lang on dim_currency_lang.id = a.currency_lang_id
WHERE 
    a.rq >= '{}' AND (a.verity_time <= '{}' OR ISNULL(a.verity_time)) 
    AND dim_area.name IN ({});'''.format(last_month, yesterday, match[team])
        print('正在获取本月第三个10天的订单…………')
        df = pd.read_sql_query(sql=sql, con=self.engine2)
        print('----已获取本月第三个10天的订单')
        sql = 'SELECT * FROM dim_order_status;'
        df1 = pd.read_sql_query(sql=sql, con = self.engine1)
        print('----已获取订单状态')
        print('正在合并订单状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统订单状态id', right_on='id', how='left')
        print('----已合并订单状态')
        sql = 'SELECT * FROM dim_logistics_status;'
        df1 = pd.read_sql_query(sql=sql, con=self.engine1)
        print('----已获取物流状态')
        print('正在合并物流状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统物流状态id', right_on='id', how='left')
        df = df.drop(labels=['id', 'id_y', '系统订单状态id', '系统物流状态id'], axis=1)
        df.rename(columns={'id_x': 'id', 'name_x': '系统订单状态', 'name_y': '系统物流状态'}, inplace=True)
        print('----已获取近两个月订单与物流状态')
        # self.reSetEngine()
        print('正在写入数据库…………')
        # 这一句会报错,需要修改my.ini文件中的[mysqld]段中的"max_allowed_packet = 1024M"
        df.to_sql(team + '_order_list', con=self.engine1, index=False, if_exists='replace')
        print('----已写入数据库')
        return '写入完成'

    def creatMyOrder(self, team):
            match = {'slgat': '"神龙家族-港澳台"',
                     'sltg': '"神龙家族-泰国"',
                     'slxmt': '"神龙家族-新加坡", "神龙家族-马来西亚"',
                     'slzb': '"神龙家族-直播团队"',
                     'slyn': '"神龙家族-越南"',
                     'slrb': '"神龙家族-日本团队"'}
            # 第一部分查询
            yesterday = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-10'
            # yesterday = '2020-09-27 23:59:59'
            last_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-01'
            # last_month = '2020-09-20 00:00:00'
            sql = '''SELECT a.id, 
    			a.month 年月, 
    			a.month_mid 旬, 
    			a.rq 日期, 
    			dim_area.name 团队, 
    			a.region_code 区域,
    			dim_currency_lang.pname 币种,
    			a.beform 订单来源, 
    			a.order_number 订单编号, 
    			a.ship_phone 电话号码, 
    			UPPER(a.waybill_number) 运单编号,
    			a.order_status 系统订单状态id, 
    			a.logistics_status 系统物流状态id, 
    			IF(a.second=0,'直发','改派') 是否改派, 
    			dim_trans_way.all_name 物流方式,
    			dim_trans_way.simple_name 物流名称,
    			dim_trans_way.remark 运输方式,
    			a.logistics_type 货物类型,
    			IF(a.low_price=0,'否','是') 是否低价, 
    			a.product_id 产品id, 
    			gk_product.name 产品名称, 
    			dim_cate.ppname 父级分类,
    			dim_cate.pname 二级分类,
    			dim_cate.name 三级分类,
    			dim_payment.pay_name 付款方式, 
    			a.amount 价格, 
    			a.addtime 下单时间, 
    			a.verity_time 审核时间, 
    			a.delivery_time 仓储扫描时间, 
    			a.finish_status 完结状态, 
    			a.endtime 完结状态时间, 
    			a.salesRMB 价格RMB, 
    			intervals.intervals 价格区间,
    			a.purchase 成本价, 
    			a.logistics_cost 物流花费, 
    			a.package_cost 打包花费, 
    			a.other_fee 其它花费, 
    			a.weight 包裹重量,
    			a.volume 包裹体积,
    			a.ship_zip 邮编
    FROM 
    		gk_order a left join dim_area ON dim_area.id = a.area_id 
                       left join dim_payment on dim_payment.id = a.payment_id
                       left join gk_product on gk_product.id = a.product_id 
                       left join dim_trans_way on dim_trans_way.id = a.logistics_id
                       left join dim_cate on dim_cate.id = gk_product.third_cate_id 
                       left join intervals on intervals.id = a.intervals
                       left join dim_currency_lang on dim_currency_lang.id = a.currency_lang_id
    WHERE 
    	a.rq >= '{}' AND (a.verity_time <= '{}' OR ISNULL(a.verity_time)) 
    	AND dim_area.name IN ({});'''.format(last_month, yesterday, match[team])
            print('正在获取上月第一个10天的订单…………')
            df = pd.read_sql_query(sql=sql, con=self.engine2)
            print('----已获取上月第一个10天的订单')
            sql = 'SELECT * FROM dim_order_status;'
            df1 = pd.read_sql_query(sql=sql, con=self.engine1)
            print('----已获取订单状态')
            print('正在合并订单状态…………')
            df = pd.merge(left=df, right=df1, left_on='系统订单状态id', right_on='id', how='left')
            print('----已合并订单状态')
            sql = 'SELECT * FROM dim_logistics_status;'
            df1 = pd.read_sql_query(sql=sql, con=self.engine1)
            print('----已获取物流状态')
            print('正在合并物流状态…………')
            df = pd.merge(left=df, right=df1, left_on='系统物流状态id', right_on='id', how='left')
            df = df.drop(labels=['id', 'id_y', '系统订单状态id', '系统物流状态id'], axis=1)
            df.rename(columns={'id_x': 'id', 'name_x': '系统订单状态', 'name_y': '系统物流状态'}, inplace=True)
            print('----已获取近两个月订单与物流状态')
            # self.reSetEngine()
            print('正在写入数据库…………')
            # 这一句会报错,需要修改my.ini文件中的[mysqld]段中的"max_allowed_packet = 1024M"
            df.to_sql(team + '_order_list', con=self.engine1, index=False, if_exists='replace')
            print('----已写入数据库')
            return '写入完成'
    def connectOrder(self, team):
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
        sql = '''SELECT 年月, 旬, 日期, 团队,币种, 区域, 订单来源, a.订单编号 订单编号, 电话号码, a.运单编号 运单编号,
                    IF(出货时间='1990-01-01 00:00:00', '', 出货时间) 出货时间, IF(ISNULL(c.标准物流状态), b.物流状态, c.标准物流状态) 物流状态, c.`物流状态代码` 物流状态代码,
                    IF(状态时间='1990-01-01 00:00:00', '', 状态时间) 状态时间,IF(上线时间='1990-01-01 00:00:00', '', 上线时间) 上线时间, 系统订单状态, IF(ISNULL(d.订单编号), 系统物流状态, '已退货') 系统物流状态, IF(ISNULL(d.订单编号), NULL, '已退货') 退货登记,
                    IF(ISNULL(d.订单编号), IF(ISNULL(系统物流状态), IF(ISNULL(c.标准物流状态) OR c.标准物流状态 = '未上线', IF(系统订单状态 IN ('已转采购', '待发货'), '未发货', '未上线') , c.标准物流状态), 系统物流状态), '已退货') 最终状态, 
                    是否改派,物流方式,物流名称,运输方式,货物类型,是否低价,付款方式,产品id,产品名称,父级分类,
                    二级分类,三级分类,下单时间,审核时间,仓储扫描时间,完结状态时间,价格,价格RMB,价格区间, 
                    包裹重量,包裹体积,邮编,IF(ISNULL(b.运单编号), '否', '是') 签收表是否存在,
                    b.订单编号 签收表订单编号, b.运单编号 签收表运单编号, 原运单号, b.物流状态 签收表物流状态, b.添加时间
                FROM {0}_order_list a  LEFT JOIN (SELECT * FROM {0} WHERE id IN 
        (SELECT MAX(id) FROM {0} GROUP BY 运单编号) ORDER BY id) b 
        ON a.`运单编号` = b.`运单编号`
				LEFT JOIN {0}_logisitis_match c ON b.物流状态 = c.签收表物流状态 
				LEFT JOIN {0}_return d ON a.订单编号 = d.订单编号
				WHERE a.系统订单状态 IN ('已审核', '待发货', '已转采购', '已发货', '已收货', '已完成', '已退货(销售)', 
				'已退货(物流)', '已退货(不拆包物流)', '待发货转审核') 
				ORDER BY a.`下单时间`;'''.format(team)
        df = pd.read_sql_query(sql=sql, con=self.engine1)
        today = datetime.date.today().strftime('%Y.%m.%d')
        print('正在写入excel…………')
        df.to_excel('D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}签收表.xlsx'.format(today, match[team]),
                    sheet_name=match[team], index=False)
        print('----已写入excel')
        print(df)

        # filePath = ['D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}签收表.xlsx'.format(today, match[team])]
        # print('输出文件成功…………')
        # if team == 'slgat':
        #     messagebox.showinfo("提示！！！", "当前查询已完成--->>> 请前往（ 输出文件 ）查看发送")
        # else:
        #     self.e.send('{} 神龙{}签收表.xlsx'.format(today, match[team]), filePath,
        #             emailAdd[team])
    def noWaybillNumber(self, team):
        match1 = {'slgat': '港台',
                 'sltg': '泰国',
                 'slxmt': '新马',
                 'slzb': '直播团队',
                 'slyn': '越南',
                 'slrb': '日本'}
        match = {'slgat': '"神龙家族-港澳台"',
                 'sltg': '"神龙家族-泰国"',
                 'slxmt': '"神龙家族-新加坡", "神龙家族-马来西亚"',
                 'slzb': '"神龙家族-直播团队"',
                 'slyn': '"神龙家族-越南"',
                 'slrb': '"神龙家族-日本团队"'}
        emailAdd = {'slgat': 'giikinliujun@163.com',
                  'sltg': '1845389861@qq.com',
                  'slxmt': 'zhangjing@giikin.com',
                  'slzb': '直播团队',
                  'slyn': '越南',
                  'slrb': 'sunyaru@giikin.com'}
        print('正在查询{}无运单订单列表…………'.format(match[team]))
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        last_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-01'
        # last_month = '2020-07-21'
        sql = '''
        SELECT a.rq 日期, 
            dim_area.name 团队, 
            a.order_number 订单编号,
            a.waybill_number 运单编号,
            a.order_status 系统订单状态id, 
            IF(a.second=0,'直发','改派') 是否改派, 
            dim_trans_way.all_name 物流方式, 
            a.addtime 下单时间, 
            a.verity_time 审核时间
        FROM 
            gk_order a 
            left join dim_area ON dim_area.id = a.area_id
            left join dim_trans_way on dim_trans_way.id = a.logistics_id
        WHERE 
        	a.rq >= '{}' AND a.rq <= '{}'
        	AND dim_area.name IN ({})
        	AND ISNULL(waybill_number) 
            AND order_status NOT IN (1, 8, 11, 14, 16)
        ORDER BY a.rq;
        '''.format(last_month, yesterday, match[team])
        df = pd.read_sql_query(sql=sql, con=self.engine2)
        sql = 'SELECT * FROM dim_order_status;'
        df1 = pd.read_sql_query(sql=sql, con=self.engine1)
        print('正在合并订单状态…………')
        df = pd.merge(left=df, right=df1, left_on='系统订单状态id', right_on='id', how='left')
        df = df.drop(labels=['id', '系统订单状态id', ], axis=1)
        df.rename(columns={'name': '系统订单状态'}, inplace=True)
        today = datetime.date.today().strftime('%Y.%m.%d')
        df.to_excel('D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}无运单号列表.xlsx'.format(today, match1[team]),
                    sheet_name=match1[team], index=False)
        filePath = ['D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}无运单号列表.xlsx'.format(today, match1[team])]
        print('输出文件成功…………')
        self.e.send('{} 神龙{}无运单号列表.xlsx'.format(today, match1[team]), filePath,
                    emailAdd[team])
    def orderCost(self, team):
        if datetime.datetime.now().day >= 9:
            endDate = datetime.datetime.now().replace(day=1).strftime('%Y-%m-%d')
            startDate = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-01'
            endDate = [endDate, datetime.datetime.now().strftime('%Y-%m-%d')]
            startDate = [startDate, datetime.datetime.now().replace(day=1).strftime('%Y-%m-%d')]
        else:
            endDate = datetime.datetime.now().replace(day=1).strftime('%Y-%m-%d')
            startDate = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-01'
            endDate = [endDate]
            startDate = [startDate]

        match = {'SG': '新加坡',
                  'MY': '马来西亚',
                  'JP': '日本',
                  'HK': '香港',
                  'TW': '台湾',
                  'TH': '泰国'}
        emailAdd = {'SG': 'zhangjing@giikin.com',
                  'MY': 'zhangjing@giikin.com',
                  'JP': 'sunyaru@giikin.com',
                  'HK': 'giikinliujun@163.com',
                  'TW': 'giikinliujun@163.com',
                  'TH': '1845389861@qq.com'}
        filePath = []
        for i in range(len(endDate)):
            print('正在查询' + match[team] + '产品花费表…………')
            sql = '''SELECT s1.`month` AS '月份',
                         s1.area AS '地区',
                         s1.leader AS '负责人',
                         s1.pid AS '产品ID',
                         s1.pname AS '产品名称',
                         s1.cate1 AS '一级品类',
                         s1.cate2 AS '二级品类',
                         
                         s1.cate3 AS '三级品类',
                         s1.orders AS '订单量',
                         s1.orders - s1.gps AS '直发订单量',
                         s1.gps AS '改派订单量',
                         s1.salesRMB / s1.orders AS '客单价',
                         s1.salesRMB / s1.adcost AS 'ROI',
                         
                         s1.orders / s2.orders AS '订单品类占比',
                         
                         s1.cgcost_zf / s1.salesRMB AS '直发采购/销售额',
                         s1.adcost / s1.salesRMB AS '花费占比',
                         s1.wlcost / s1.salesRMB AS '运费占比',
                         s1.qtcost / s1.salesRMB AS '手续费占比',
                         ( s1.cgcost_zf + s1.adcost + s1.wlcost + s1.qtcost ) / s1.salesRMB AS '总成本占比',
                         s1.salesRMB_yqs / (s1.salesRMB_yqs + s1.salesRMB_yjs) AS '金额签收/完成',
                         s1.salesRMB_yqs / s1.salesRMB AS '金额签收/总计',
                         (s1.salesRMB_yqs + s1.salesRMB_yjs) / s1.salesRMB AS '金额完成占比',
                         s1.yqs / (s1.yqs + s1.yjs) AS '数量签收/完成',
                         (s1.yqs + s1.yjs) / s1.orders AS '数量完成占比',
                         
                         s3.orders AS '昨日订单量'
            
            FROM (
                        SELECT EXTRACT(YEAR_MONTH FROM a.rq) AS `month`,
                                     b.pname AS area,
                                     c.uname AS leader,
                                     a.product_id AS pid,
                                     e.`name` AS pname,
                                     d.ppname AS cate1,
                                     d.pname AS cate2,
                                     d.`name` AS cate3,
            -- 						 GROUP_CONCAT(DISTINCT a.low_price) AS low_price,
                                     SUM(a.orders) AS orders,
                                     SUM(a.yqs) AS yqs,
                                     SUM(a.yjs) AS yjs,
                                     SUM(a.salesRMB) AS salesRMB,
                                     SUM(a.salesRMB_yqs) AS salesRMB_yqs,
                                     SUM(a.salesRMB_yjs) AS salesRMB_yjs,
                                     SUM(a.gps) AS gps,
                                     SUM(a.cgcost_zf) AS cgcost_zf,
                                     SUM(a.adcost) AS adcost,
                                     SUM(a.wlcost) AS wlcost,
                                     SUM(a.qtcost) AS qtcost		 
                        FROM gk_order_day a
                            LEFT JOIN dim_currency_lang b ON a.currency_lang_id = b.id
                            LEFT JOIN dim_area c on c.id = a.area_id
                            LEFT JOIN dim_cate d on d.id = a.third_cate_id
                            LEFT JOIN gk_product e on e.id = a.product_id
                        WHERE a.rq >= '{startDate}'
                            AND a.rq < '{endDate}'
                            AND b.pcode = '{team}'
                            AND c.uname = '王冰'
                            AND a.beform <> 'mf'
                            AND c.uid <> 10099  -- 过滤翼虎
                        GROUP BY EXTRACT(YEAR_MONTH FROM a.rq), b.pname, c.uname, a.product_id
                     ) s1
                     
                      LEFT JOIN
                        
                     (
                        SELECT EXTRACT(YEAR_MONTH FROM a.rq) AS `month`,
                                     b.pname AS area,
                                     c.uname AS leader,
                                     d.ppname AS cate1,
                                     SUM(a.orders) AS orders
                        FROM gk_order_day a
                            LEFT JOIN dim_currency_lang b ON a.currency_lang_id = b.id
                            LEFT JOIN dim_area c on c.id = a.area_id
                            LEFT JOIN dim_cate d on d.id = a.third_cate_id
                        WHERE a.rq >= '{startDate}'
                            AND a.rq < '{endDate}'
                            AND b.pcode = '{team}'
                            AND c.uname = '王冰'
                            AND a.beform <> 'mf'
                            AND c.uid <> 10099  
                        GROUP BY EXTRACT(YEAR_MONTH FROM a.rq), b.pname, c.uname, d.ppname
                     ) s2  ON s1.`month`=s2.`month` AND s1.area=s2.area AND s1.leader=s2.leader AND s1.cate1=s2.cate1
                    
                      LEFT JOIN
                    
                     (
                      SELECT EXTRACT(YEAR_MONTH FROM a.rq) AS `month`,
                                     b.pname AS area,
                                     c.uname AS leader,
                                     a.product_id AS pid,
                                     SUM(a.orders) AS orders
                        FROM gk_order_day a
                            LEFT JOIN dim_currency_lang b ON a.currency_lang_id = b.id
                            LEFT JOIN dim_area c on c.id = a.area_id
                        WHERE a.rq = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                                    AND b.pcode = '{team}'
                                    AND c.uname = '王冰'
                                    AND a.beform <> 'mf'
                                    AND c.uid <> 10099  
                        GROUP BY EXTRACT(YEAR_MONTH FROM a.rq), b.pname, c.uname, a.product_id
                     ) s3 ON s1.area=s3.area AND s1.leader=s3.leader AND s1.pid=s3.pid
            WHERE s1.orders > 0
            ORDER BY s1.orders DESC
            '''.format(team=team, startDate=startDate[i], endDate=endDate[i])
            df = pd.read_sql_query(sql=sql, con=self.engine2)
            print('正在输出' + match[team] + '产品花费表…………')
            columns = ['订单品类占比', '直发采购/销售额', '花费占比', '运费占比', '手续费占比', '总成本占比',
                      '金额签收/完成', '金额签收/总计', '金额完成占比', '数量签收/完成', '数量完成占比']
            for column in columns:
                df[column] = df[column].fillna(value=0)
                df[column] = df[column].apply(lambda x: format(x, '.2%'))
            today = datetime.date.today().strftime('%Y.%m.%d')
            if i == 0:
                df.to_excel('D:\\Users\\Administrator\\Desktop\\输出文件\\{} {}上月产品花费表.xlsx'.format(today, match[team]),
                            sheet_name=match[team], index=False)
                filePath.append('D:\\Users\\Administrator\\Desktop\\输出文件\\{} {}上月产品花费表.xlsx'.format(today, match[team]))
            else:
                df.to_excel('D:\\Users\\Administrator\\Desktop\\输出文件\\{} {}本月产品花费表.xlsx'.format(today, match[team]),
                            sheet_name=match[team], index=False)
                filePath.append('D:\\Users\\Administrator\\Desktop\\输出文件\\{} {}本月产品花费表.xlsx'.format(today, match[team]))
        self.e.send(match[team] + '产品花费表', filePath,
                    emailAdd[team])
    def sltg_HaiWaiCang(self, team):
        match = {'slgat': '港台',
                 'sltg': '泰国',
                 'slxmt': '新马',
                 'slzb': '直播团队',
                 'slyn': '越南',
                 'slrb': '日本'}
        sql = '''SELECT 订单编号,运单号,产品id,产品名称,规格中文,数量,qb.订单状态 FROM
        (SELECT a.order_number `订单编号`, a.waybill_number `运单号`,a.goods_id `产品id`,a.goods_name `产品名称`,a.op_id `规格`,a.quantity `数量`,a.order_status `订单状态` 
        FROM 全部订单_sltg a INNER JOIN (SELECT DISTINCT upper(`Tracking Number`) `Tracking Number` FROM 海外仓库存_时丰) b ON a.waybill_number = b.`Tracking Number`) qb 
        INNER JOIN 全部订单规格_sltg b ON qb.`订单编号` = b.`订单号`;'''
        print('正在查询海外仓订单…………')
        df = pd.read_sql_query(sql=sql, con=self.engine1, chunksize=100)
        print('正在写入excel…………')
        df.to_excel('D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}海外仓.xlsx'.format(today, match[team]),
                    sheet_name=match[team], index=False)
        print('输出文件成功…………')

if __name__ == '__main__':
    m = MysqlControl()
    # sql = r'SELECT * FROM logistics_status.出货明细 LIMIT 0,10'
    # # db = m.creatMyOrder('sltg')
    # # m.connectOrder('slgat')
    # # 无运单号查询
    # for team in ['sltg', 'slrb', 'slgat', 'slxmt']:
    #     m.noWaybillNumber(team)

    # match = {'SG': '新加坡',
    #          'MY': '马来西亚',
    #          'JP': '日本',
    #          'HK': '香港',
    #          'TW': '台湾',
    #          'TH': '泰国'}
    # # 产品花费表
    # for team in match.keys():
    #     m.orderCost(team)
    # messagebox.showinfo("提示！！！", "当前查询已完成--->>> 请前往（ 输出文件 ）查看")
    team = 'sltg'
    m.sltg_HaiWaiCang(team)
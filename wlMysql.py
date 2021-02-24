import pandas as pd
from sqlalchemy import create_engine
from settings import Settings
from emailControl import EmailControl
import datetime
class WlMysql(Settings):
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
    def writeSqlWl(self, dataFrame):
        # print(dataFrame)
        dataFrame.to_sql('dim_wl', con=self.engine1, index=False, if_exists='replace')
    def wlInto(self, team, dfColumns):
        columns = list(dfColumns)
        columns = ', '.join(columns)
        sql = 'REPLACE INTO {}wl({}, 添加时间) SELECT *, NOW() 添加时间 FROM dim_wl; '.format(team, columns)
        # print(sql)
        try:
            pd.read_sql_query(sql=sql, con=self.engine1, chunksize=100)
        except Exception as e:
            print('插入失败：', str(Exception) + str(e))
    def exportOrder(self, team):
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
                IF(ISNULL(b.出货时间), g.出货时间, b.出货时间) 出货时间, IF(ISNULL(c.标准物流状态), b.物流状态, c.标准物流状态) 物流状态, c.`物流状态代码` 物流状态代码,
                b.状态时间, 上线时间, 系统订单状态, IF(ISNULL(d.订单编号), 系统物流状态, '已退货') 系统物流状态, IF(ISNULL(d.订单编号), NULL, '已退货') 退货登记,
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
                    LEFT JOIN {0}wl g ON a.运单编号 = g.运单编号
                    WHERE a.系统订单状态 IN ('已审核', '待发货', '已转采购', '已发货', '已收货', '已完成', '已退货(销售)', 
                    '已退货(物流)', '已退货(不拆包物流)', '待发货转审核') 
                    ORDER BY a.`下单时间`;'''.format(team)
        df = pd.read_sql_query(sql=sql, con=self.engine1)
        # if team == 'slxmt':
        # print('正在获取单独物流状态…………')
        # sql = 'SELECT * FROM slxmtwl;'
        # df1 = pd.read_sql_query(sql=sql, con=self.engine1)
        # print('正在合并物流出货时间…………')
        # df = pd.merge(left=df, right=df1, left_on='运单编号', right_on='运单编号', how='left')
        # df = df.drop(labels=['id', '订单编号_y', '物流状态_y', '状态时间_y'], axis=1)
        # df.rename(columns={'出货时间_x': '出货时间', '添加时间_x': '添加时间'}, inplace=True)

        # df = pd.read_sql_query(sql=sql, con=self.engine1)
        # else:
        #     df = pd.read_sql_query(sql=sql, con=self.engine1)
        today = datetime.date.today().strftime('%Y.%m.%d')
        print('正在写入excel…………')
        df.to_excel('D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}签收表.xlsx'.format(today, match[team]),
                    sheet_name=match[team], index=False)
        print('----已写入excel')
        filePath = ['D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}签收表.xlsx'.format(today, match[team])]
        print('输出文件成功…………')
        self.e.send('{} 神龙{}签收表.xlsx'.format(today, match[team]), filePath,
                    emailAdd[team])

    def creatMyOrderOne(self, team):
        match = {'slgat': '"神龙家族-港澳台"',
                 'sltg': '"神龙家族-泰国"',
                 'slxmt': '"神龙家族-新加坡", "神龙家族-马来西亚"',
                 'slzb': '"神龙家族-直播团队"',
                 'slyn': '"神龙家族-越南"',

                 'slrb': '"神龙家族-日本团队"'}
        yesterday = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-10' + ' 23:59:59'
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
    def creatMyOrderTwo(self, team):
        match = {'slgat': '"神龙家族-港澳台"',
                'sltg': '"神龙家族-泰国"',
                'slxmt': '"神龙家族-新加坡", "神龙家族-马来西亚"',
                'slzb': '"神龙家族-直播团队"',
                'slyn': '"神龙家族-越南"',
                'slrb': '"神龙家族-日本团队"'}
        yesterday = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-20' + ' 23:59:59'
        # yesterday = '2020-09-27 23:59:59'
        last_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-10'
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
    def creatMyOrderThree(self, team):
        match = {'slgat': '"神龙家族-港澳台"',
                'sltg': '"神龙家族-泰国"',
                'slxmt': '"神龙家族-新加坡", "神龙家族-马来西亚"',
                'slzb': '"神龙家族-直播团队"',
                'slyn': '"神龙家族-越南"',
                'slrb': '"神龙家族-日本团队"'}
        yesterday = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m-%d') + ' 23:59:59'
        # yesterday = '2020-09-27 23:59:59'
        last_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-20'
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
    def creatMyOrderThou(self, team):
        match = {'slgat': '"神龙家族-港澳台"',
                'sltg': '"神龙家族-泰国"',
                'slxmt': '"神龙家族-新加坡", "神龙家族-马来西亚"',
                'slzb': '"神龙家族-直播团队"',
                'slyn': '"神龙家族-越南"',
                'slrb': '"神龙家族-日本团队"'}
        yesterday = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m') + '-10' + ' 23:59:59'
        # yesterday = '2020-09-27 23:59:59'
        last_month = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m-%d')
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
    def creatMyOrderFire(self, team):
        match = {'slgat': '"神龙家族-港澳台"',
                'sltg': '"神龙家族-泰国"',
                'slxmt': '"神龙家族-新加坡", "神龙家族-马来西亚"',
                'slzb': '"神龙家族-直播团队"',
                'slyn': '"神龙家族-越南"',
                'slrb': '"神龙家族-日本团队"'}
        yesterday = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m') + '-20' + ' 23:59:59'
        # yesterday = '2020-09-27 23:59:59'
        last_month = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m') + '-10'
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
    def creatMyOrderSix(self, team):
        match = {'slgat': '"神龙家族-港澳台"',
                'sltg': '"神龙家族-泰国"',
                'slxmt': '"神龙家族-新加坡", "神龙家族-马来西亚"',
                'slzb': '"神龙家族-直播团队"',
                'slyn': '"神龙家族-越南"',
                'slrb': '"神龙家族-日本团队"'}
        yesterday = (datetime.datetime.now().replace(day=30)).strftime('%Y-%m-%d') + ' 23:59:59'
        # yesterday = '2020-09-30 23:59:59'
        last_month = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m') + '-20'
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

if __name__ == '__main__':
    team = 'slxmt'
    w = WlMysql()
    w.exportOrder(team)
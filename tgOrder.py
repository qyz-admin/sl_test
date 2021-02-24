import pandas as pd
from sqlalchemy import create_engine
from settings import Settings
from emailControl import EmailControl
from tkinter import messagebox
import datetime
import xlwings
import numpy as np
import pandas.io.formats.excel

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
    # 各团队全部订单表-函数
    def tgOrderQuan(self, team):
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
        yesterday = (datetime.datetime.now().replace(month=11, day=3)).strftime('%Y-%m-%d')
        # yesterday = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        # yesterday = '2020-08-25'
        print(yesterday)
        last_month = (datetime.datetime.now().replace(month=11, day=2)).strftime('%Y-%m-%d')
        # last_month = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
        # last_month = '2020-08-15'
        print(last_month)
        sql = '''SELECT a.id,
            a.order_number order_number,
            dim_area.name area_id,
            a.sale_id main_id,
            a.ship_phone ship_phone,
            a.ship_zip ship_zip,
            a.amount amount,
            tg_order_status.name order_status,
            UPPER(a.waybill_number) waybill_number,
            dim_payment.pay_name pay_type,
            a.addtime addtime,
            a.uptime update_time,
            a.product_id goods_id, 
            a.qty quantity,
            dim_trans_way.all_name logistics_id,
            '' op_id,
            CONCAT(gk_sale.product_id,'#' ,gk_sale.product_name) goods_name, 
            IF(a.second=0,'直发','改派') secondsend_status,
            IF(a.low_price=0,'否','是') low_price
FROM 
        gk_order a left join dim_area ON dim_area.id = a.area_id 
                    left join dim_payment on dim_payment.id = a.payment_id
                    left join gk_sale on gk_sale.product_id = a.product_id 
                    left join dim_trans_way on dim_trans_way.id = a.logistics_id
                    left join tg_order_status on tg_order_status.id = a.order_status
WHERE 
    a.rq >= '{}' AND a.rq <= '{}'
    AND dim_area.name IN ({});'''.format(last_month, yesterday, match[team])
        print('正在获取最近 3 天订单…………')
        try:
            df = pd.read_sql_query(sql=sql, con=self.engine2)
            print('----已获取近一周订单')
            print(df)
            print('正在写入缓存表中…………')
            df.to_sql('tem_tg', con=self.engine3, index=False, if_exists='replace')
            # df.to_sql('tem_copy1', con=self.engine3, index=False, if_exists='replace')
        except Exception as e:
            print('更新缓存失败：', str(Exception) + str(e))
        print('++++更新缓存完成++++')
        print('正在写入全部订单表中…………')
        sql = 'REPLACE INTO 全部订单_{} SELECT *, NOW() 添加时间 FROM tem_tg;'.format(team)
        # sql = 'REPLACE INTO 全部订单3_{} SELECT *, NOW() 添加时间 FROM tem_copy1;'.format(team)
        pd.read_sql_query(sql=sql, con=self.engine3, chunksize=100)
        # df = pd.read_sql_query(sql=sql, con=self.engine3)

        # now_yesterday = (datetime.datetime.now().replace(month=11, day=1)).strftime('%Y-%m-%d') + ' 23:59:59'
        # last_yesterday = (datetime.datetime.now().replace(month=10, day=30)).strftime('%Y-%m-%d') + ' 00:00:00'
        # sql = '''SELECT order_number FROM 全部订单_{} q
        # WHERE q.addtime>= '{}' AND q.addtime<= '{}';'''.format(team, last_yesterday, now_yesterday)
        # df1 = pd.read_sql_query(sql=sql, con=self.engine3)
        # print(df1)

        print('----已写入全部订单表中')
    # 全部订单查询函数（备用）
    def queryOrder(self, team):
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
        # yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + ' 23:59:59'
        yesterday = '2020-10-28'
        # last_month = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m-%d') + ' 00:00:00'
        last_month = '2020-10-27'
        sql = '''SELECT a.id,
            a.order_number order_number,
            dim_area.name area_id,
            a.sale_id main_id,
            a.ship_phone ship_phone,
            a.ship_zip ship_zip,
            a.amount amount,
            tg_order_status.name order_status,
            UPPER(a.waybill_number) waybill_number,
            dim_payment.pay_name pay_type,
            a.addtime addtime,
            a.uptime update_time,
            a.product_id goods_id, 
            a.qty quantity,
            dim_trans_way.all_name logistics_id,
            '' op_id,
            gk_sale.sale_name goods_name, 
            IF(a.second=0,'直发','改派') secondsend_status,
            IF(a.low_price=0,'否','是') low_price
FROM 
        gk_order a left join dim_area ON dim_area.id = a.area_id 
                    left join dim_payment on dim_payment.id = a.payment_id
                    left join gk_sale on gk_sale.product_id = a.product_id 
                    left join dim_trans_way on dim_trans_way.id = a.logistics_id
                    left join tg_order_status on tg_order_status.id = a.order_status
WHERE 
    a.rq >= '{}' AND a.rq <= '{}'
    AND dim_area.name IN ({});'''.format(last_month, yesterday, match[team])
        print('正在获取近一周订单…………')
        try:
            df = pd.read_sql_query(sql=sql, con=self.engine2)
            print('----已获取近一周订单')
            # df.to_sql('全部订单缓存表', con=self.engine3, index=False, if_exists='replace')
            print('正在写入缓存表中…………')
            df.to_sql('tem_copy1', con=self.engine3, index=False, if_exists='replace')
            print(df)
        except Exception as e:
            print('更新缓存失败：', str(Exception) + str(e))
        print('++++更新缓存完成++++')
        print('正在写入全部订单表中…………')
        sql = 'REPLACE INTO 全部订单3_{} SELECT *, NOW() 添加时间 FROM tem_copy1;'.format(team)
        df = pd.read_sql_query(sql=sql, con=self.engine3, chunksize=100)
        print(df)
        print('----已写入全部订单表中')
        today = datetime.date.today().strftime('%Y.%m.%d')
        df.to_excel('D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}全部订单查询表.xlsx'.format(today, match1[team]),
                    sheet_name=match[team], index=False)
        print('----已写入excel')
        return '写入完成'

    def OrderQuan(self, team):
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
        # yesterday = (datetime.datetime.now()).strftime('%Y-%m-%d') + ' 23:59:59'
        # yesterday = (datetime.datetime.now().replace(month=5, day=31)).strftime('%Y-%m-%d')
        # yesterday = '2020-08-25'
        # print(yesterday)
        # last_month = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m-%d')
        # last_month = (datetime.datetime.now().replace(month=5, day=27)).strftime('%Y-%m-%d')
        # last_month = '2020-08-15'
        # print(last_month)
        sql = '''SELECT IFNULL(ql.币种,'合计') 币种,IFNULL(ql.年月,'合计') 年月,IFNULL(ql.产品名称,'合计') 产品名称,IFNULL(ql.物流方式,'合计') 物流方式,IFNULL(ql.旬,'合计') 旬,签收,拒收,在途,未发货,未上线,已退货,理赔,自发头程丢件,已发货,已完成,全部,ql.签收 / ql.已完成 AS 完成签收, ql.签收 / ql.全部 AS 总计签收, ql.已完成 / ql.全部 AS 完成占比 ,ql.已发货 / ql.已完成 AS '已完成/已发货' , ql.已退货 / ql.全部 AS 退货率  FROM
        (SELECT qq.币种,qq.年月,qq.产品名称,qq.物流方式, qq.旬,sum(签收) 签收,sum(拒收) 拒收,sum(在途) 在途,sum(未发货) 未发货,sum(未上线) 未上线,sum(已退货) 已退货,sum(理赔) 理赔,sum(自发头程丢件) 自发头程丢件,sum(已发货) 已发货,sum(已完成) 已完成,sum(全部) 全部 FROM
        (SELECT q.币种,q.年月,q.产品名称,q.物流方式, q.旬,已签收 签收,拒收,在途,未发货,未上线,已退货,理赔,自发头程丢件,已发货,已完成 FROM
        (SELECT 币种,年月,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已签收 FROM d1               
        WHERE d1.最终状态 IN ('已签收') GROUP BY 币种,年月,产品名称,物流方式,旬 ORDER BY 币种,年月) q 
        LEFT JOIN
        (SELECT 币种,年月,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 拒收 FROM d1                
        WHERE d1.最终状态 IN ('拒收') GROUP BY 币种,年月,产品名称,物流方式,旬 ORDER BY 币种,年月) j 
        ON q.`币种` = j.`币种` AND q.`年月` = j.`年月` AND q.`产品名称` = j.`产品名称` AND q.`物流方式` = j.`物流方式` AND q.`旬` = j.`旬`
        LEFT JOIN
        (SELECT 币种,年月,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 在途 FROM d1                
        WHERE d1.最终状态 IN ('在途') GROUP BY 币种,年月,产品名称,物流方式,旬 ORDER BY 币种,年月) zz
        ON  q.`币种` = zz.`币种` AND q.`年月` = zz.`年月`AND q.`产品名称` = zz.`产品名称`AND q.`物流方式` = zz.`物流方式`AND q.`旬` = zz.`旬`
        LEFT JOIN
        (SELECT 币种,年月,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 未发货 FROM d1               
        WHERE d1.最终状态 IN ('未发货') GROUP BY 币种,年月,产品名称,物流方式,旬 ORDER BY 币种,年月) wf
        ON  wf.`币种` = q.`币种` AND wf.`年月` = q.`年月`AND wf.`产品名称` = q.`产品名称`AND wf.`物流方式` = q.`物流方式`AND wf.`旬` = q.`旬`
        LEFT JOIN
        (SELECT 币种,年月,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 未上线 FROM d1               
        WHERE d1.最终状态 IN ('未上线') GROUP BY 币种,年月,产品名称,物流方式,旬 ORDER BY 币种,年月) ws
        ON  ws.`币种` = q.`币种` AND ws.`年月` = q.`年月`AND ws.`产品名称` = q.`产品名称`AND ws.`物流方式` = q.`物流方式`AND ws.`旬` = q.`旬`
        LEFT JOIN
        (SELECT 币种,年月,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已退货 FROM d1               
        WHERE d1.最终状态 IN ('已退货') GROUP BY 币种,年月,产品名称,物流方式,旬 ORDER BY 币种,年月) th
        ON  q.`币种` = th.`币种` AND q.`年月` = th.`年月`AND q.`产品名称` = th.`产品名称`AND q.`物流方式` = th.`物流方式`AND q.`旬` = th.`旬`
        LEFT JOIN
        (SELECT 币种,年月,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 理赔 FROM d1                
        WHERE d1.最终状态 IN ('理赔') GROUP BY 币种,年月,产品名称,物流方式,旬 ORDER BY 币种,年月) lp
        ON  lp.`币种` = q.`币种` AND lp.`年月` = q.`年月`AND lp.`产品名称` = q.`产品名称`AND lp.`物流方式` = q.`物流方式`AND lp.`旬` = q.`旬`
        LEFT JOIN
        (SELECT 币种,年月,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 自发头程丢件 FROM d1                
        WHERE d1.最终状态 IN ('自发头程丢件') GROUP BY 币种,年月,产品名称,物流方式,旬 ORDER BY 币种,年月) zf
        ON  zf.`币种` = q.`币种` AND zf.`年月` = q.`年月`AND zf.`产品名称` = q.`产品名称`AND zf.`物流方式` = q.`物流方式`AND zf.`旬` = q.`旬`
        LEFT JOIN
        (SELECT 币种,年月,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已发货 FROM d1               
        WHERE d1.最终状态 IN ('已签收','拒收','理赔','已退货','在途','未上线') GROUP BY 币种,年月,产品名称,物流方式,旬 ORDER BY 币种,年月) fh
        ON  fh.`币种` = q.`币种` AND fh.`年月` = q.`年月`AND fh.`产品名称` = q.`产品名称`AND fh.`物流方式` = q.`物流方式`AND fh.`旬` = q.`旬`
        LEFT JOIN
        (SELECT 币种,年月,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已完成 FROM d1               
        WHERE d1.最终状态 IN ('已签收','拒收','理赔','已退货') GROUP BY 币种,年月,产品名称,物流方式,旬 ORDER BY 币种,年月) wc
        ON  wc.`币种` = q.`币种` AND wc.`年月` = q.`年月`AND wc.`产品名称` = q.`产品名称`AND wc.`物流方式` = q.`物流方式`AND wc.`旬` = q.`旬` ORDER BY 币种,年月) qq
        LEFT JOIN
        (SELECT 币种,年月,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 全部 FROM d1                
        WHERE d1.最终状态 IN ('已签收','拒收','理赔','已退货','在途','未上线','未发货') GROUP BY 币种,年月,产品名称,物流方式,旬 ORDER BY 币种,年月) qb
        ON  qb.`币种` = qq.`币种` AND qb.`年月` = qq.`年月`AND qb.`产品名称` = qq.`产品名称`AND qb.`物流方式` = qq.`物流方式`AND qb.`旬` = qq.`旬`
        GROUP BY 年月,产品名称,物流方式,旬 with rollup) ql;'''
        print('正在获取-' + match1[team] + '品类签收率…………')
        df = pd.read_sql_query(sql=sql, con=self.engine1)
        print('----已获' + match1[team] + '品类签收率')
        df['退货率'] = df['退货率'].fillna(value=0)
        df['完成签收'] = df['完成签收'].apply(lambda x: format(x, '.2%'))
        df['总计签收'] = df['总计签收'].apply(lambda x: format(x, '.2%'))
        df['完成占比'] = df['完成占比'].apply(lambda x: format(x, '.2%'))
        df['已完成/已发货'] = df['已完成/已发货'].apply(lambda x: format(x, '.2%'))
        df['退货率'] = df['退货率'].apply(lambda x: format(x, '.2%'))
        print(df)
        print('正在写入EXECL中…………')
        today = datetime.date.today().strftime('%Y.%m.%d')

        set_dir = r'D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}---签收率.xlsx'.format(today, match1[team])
        pandas.io.formats.excel.header_style = None
        writer = pd.ExcelWriter(set_dir)
        df.to_excel(writer, 'Sheet1')
        # 设置格式
        workbook1 = writer.book
        worksheets = writer.sheets
        worksheet1 = worksheets['Sheet1']
        # 设置特定单元格的宽度
        worksheet1.set_column("N:N", 20)
        worksheet1.set_column("A:A", 12)
        worksheet1.set_column("H:H", 9)
        worksheet1.set_column("L:L", 9)


        df.to_excel('D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}---签收率.xlsx'.format(today, match1[team]),
                        sheet_name=match[team], index=False)
        print('----已写入excel')

if __name__ == '__main__':
    m = MysqlControl()
    m.tgOrderQuan('slgat')
    # m.queryOrder('sltg')
    # messagebox.showinfo("提示！！！", "当前查询已完成--->>> 请前往（ 输出文件 ）查看")
import pandas as pd
import os
import zipfile
from sqlalchemy import create_engine
from settings import Settings
from emailControl import EmailControl
from openpyxl import load_workbook  #可以向不同的sheet写入数据
import datetime
import xlwings as xw
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
        # print(dfColumns)
        sql = 'REPLACE INTO {}wl({}, 添加时间) SELECT *, NOW() 添加时间 FROM dim_wl; '.format(team, columns)
        # print(sql)
        try:
            pd.read_sql_query(sql=sql, con=self.engine1, chunksize=100)
        except Exception as e:
            print('插入失败：', str(Exception) + str(e))
    def SqlWl(self, dataFrame):
        # print(dataFrame)
        dataFrame.to_sql('d1', con=self.engine1, index=False, if_exists='replace')
    def SqlInto(self, team, dfColumns):
        columns = list(dfColumns)
        columns = ', '.join(columns)
        sql = 'REPLACE INTO {}wl({}, 添加时间) SELECT *, NOW() 添加时间 FROM d1;'.format(team, columns)
        # print(sql)
        try:
            pd.read_sql_query(sql=sql, con=self.engine1, chunksize=100)
        except Exception as e:
            print('插入失败：', str(Exception) + str(e))

    # 团队花费明细查询（停用）
    def sl_tem_costT(self, team):
        match = {'slgat_zqsb': '港台',
                 'sltg_zqsb': '泰国',
                 'slxmt_zqsb': '新马',
                 'slrb_zqsb_rb': '日本'}
        emailAdd = {'slgat': 'giikinliujun@163.com',
                    'sltg': '1845389861@qq.com',
                    'slxmt': 'zhangjing@giikin.com',
                    'slzb': '直播团队',
                    'slyn': '越南',
                    'slrb': 'sunyaru@giikin.com'}
        if datetime.datetime.now().day >= 9:
            end_Date = (datetime.datetime.now()).strftime('%Y%m')
            print(end_Date)
            start_Date = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y%m')
            print(start_Date)
        else:
            end_Date = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y%m')
            print(end_Date)
            start_Date = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y%m')
            print(start_Date)
        # 总花费明细表---查询
        listT = []                                                  # 查询sql 存放池
        list_value = ['总表成本', '直发成本']                       # 生成的工作表的表名
        list_value_name = ['月 （总）详细花费数据…………', '月 （直发）详细花费数据…………']    # 打印进度需要
        sql2 = '''SELECT *		
                    FROM (
                        SELECT sl_zong.币种,
                                IFNULL(sl_zong.年月,'合计') 年月,
                                IFNULL(sl_zong.父级分类,'合计') 父级分类,
                                IFNULL(sl_zong.二级分类,'合计') 二级分类,
                                IFNULL(sl_zong.三级分类,'合计') 三级分类,
                                IFNULL(sl_zong.产品id,'合计') 产品id,
                                IFNULL(sl_zong.产品名称,'合计') 产品名称,
                                IFNULL(sl_zong.物流方式,'合计') 物流方式,
                                IFNULL(sl_zong.旬,'合计') 旬,
                                SUM(sl_zong.订单量) 订单量,
                                IFNULL(SUM(sl_zong_zf.`直发订单量`),0) 直发订单量,
                                (SUM(sl_zong.订单量) - IFNULL(SUM(sl_zong_zf.`直发订单量`),0)) AS 改派订单量,
                                IFNULL(SUM(sl_zong_zhifa.`已签收订单量`),0) 签收订单量,
                                IFNULL(SUM(sl_zong_jushou.`拒收订单量`),0) 拒收订单量,
                                SUM(sl_zong.总成本) / SUM(sl_zong.销售额)  AS '采购/销售额',
                                IFNULL(SUM(sl_zong_zf.`直发成本`),0) / SUM(sl_zong.销售额)  AS '直发采购/销售额',
                                SUM(sl_zong.物流运费) / SUM(sl_zong.销售额)  AS '运费占比',
                                SUM(sl_zong.手续费) / SUM(sl_zong.销售额)  AS '手续费占比',
                                IFNULL(SUM(sl_zong_zhifa.`已签收销售额`),0) / (IFNULL(SUM(sl_zong_zhifa.`已签收销售额`),0) + IFNULL(SUM(sl_zong_jushou.`拒收销售额`),0)) AS '金额签收/完成',
                                IFNULL(SUM(sl_zong_zhifa.`已签收销售额`),0) / SUM(sl_zong.销售额) AS '金额签收/总计',
                                (IFNULL(SUM(sl_zong_zhifa.`已签收销售额`),0) + IFNULL(SUM(sl_zong_jushou.`拒收销售额`),0)) / SUM(sl_zong.销售额) AS '金额完成占比',
                                IFNULL(SUM(sl_zong_zhifa.`已签收订单量`),0) / (IFNULL(SUM(sl_zong_zhifa.`已签收订单量`),0) + IFNULL(SUM(sl_zong_jushou.`拒收订单量`),0)) AS '数量签收/完成',
                                (IFNULL(SUM(sl_zong_zhifa.`已签收订单量`),0) + IFNULL(SUM(sl_zong_jushou.`拒收订单量`),0)) / SUM(sl_zong.订单量) AS '数量完成占比'
                        FROM (
                                SELECT 币种,
                                        年月,
                                        父级分类,
                                        二级分类,
                                        三级分类,
                                        产品id,
                                        CONCAT(产品id, '#' ,产品名称) 产品名称,
                                        物流方式,
                                        旬,
                                        COUNT(`订单编号`) 订单量,
                                        SUM(`价格RMB`) 销售额,
                                        SUM(`成本价`) 总成本,
                                        SUM(`物流花费`) 物流运费,
                                        SUM(`打包花费`) 打包花费,
                                        SUM(`其它花费`) 手续费
                                FROM  {0} sl_cx
                                WHERE sl_cx.`币种` = '{1}'
                                    AND sl_cx.`年月` >= '{2}'
                                    AND sl_cx.`年月` <= '{3}'
                                    AND sl_cx.`系统订单状态`!="已删除"
                                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                                ORDER BY 币种,年月
                            ) sl_zong
                    LEFT JOIN
                            (   SELECT 币种,
                                        年月,
                                        父级分类,
                                        二级分类,
                                        三级分类,
                                        产品id,
                                        CONCAT(产品id, '#' ,产品名称) 产品名称,
                                        物流方式,
                                        旬,
                                        COUNT(`订单编号`) 直发订单量,
                                        SUM(`价格RMB`) 销售额,
                                        SUM(`成本价`) 直发成本,
                                        SUM(`物流花费`) 物流运费,
                                        SUM(`打包花费`) 打包花费,
                                        SUM(`其它花费`) 手续费
                                FROM  {0} sl_cx_zf
                                WHERE sl_cx_zf.`币种` = '{1}' 
                                    AND sl_cx_zf.`年月` >= '{2}'
                                    AND sl_cx_zf.`年月` <= '{3}'
                                    AND sl_cx_zf.`系统订单状态`!="已删除"
                                    AND sl_cx_zf.`是否改派` = "直发"
                                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                                ORDER BY 币种,年月
                        ) sl_zong_zf
                            ON sl_zong_zf.`币种` = sl_zong.`币种` 
                                AND sl_zong_zf.`年月` = sl_zong.`年月`
                                AND sl_zong_zf.`父级分类` = sl_zong.`父级分类` 
                                AND sl_zong_zf.`二级分类` = sl_zong.`二级分类` 
                                AND sl_zong_zf.`三级分类` = sl_zong.`三级分类` 
                                AND sl_zong_zf.`产品id` = sl_zong.`产品id`
                                AND sl_zong_zf.`产品名称` = sl_zong.`产品名称`
                                AND sl_zong_zf.`物流方式` = sl_zong.`物流方式`
                                AND sl_zong_zf.`旬` = sl_zong.`旬` 
                    LEFT JOIN
                            (   SELECT 币种,
                                        年月,
                                        父级分类,
                                        二级分类,
                                        三级分类,
                                        产品id,
                                        CONCAT(产品id, '#' ,产品名称) 产品名称,
                                        物流方式,
                                        旬,
                                        COUNT(`订单编号`) 已签收订单量,
                                        SUM(`价格RMB`) 已签收销售额,
                                        SUM(`成本价`) 已签收成本,
                                        SUM(`物流花费`) 已签收物流运费,
                                        SUM(`打包花费`) 已签收打包花费,
                                        SUM(`其它花费`) 已签收手续费
                                FROM  {0} sl_cx_zhifa
                                WHERE sl_cx_zhifa.`币种` = '{1}' 
                                    AND sl_cx_zhifa.`年月` >= '{2}'
                                    AND sl_cx_zhifa.`年月` <= '{3}'
                                    AND sl_cx_zhifa.`系统订单状态`!="已删除"
                                    AND sl_cx_zhifa.`最终状态` = "已签收"
                                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                                ORDER BY 币种,年月
                        ) sl_zong_zhifa
                        ON sl_zong_zhifa.`币种` = sl_zong.`币种` 
                                AND sl_zong_zhifa.`年月` = sl_zong.`年月`
                                AND sl_zong_zhifa.`父级分类` = sl_zong.`父级分类` 
                                AND sl_zong_zhifa.`二级分类` = sl_zong.`二级分类` 
                                AND sl_zong_zhifa.`三级分类` = sl_zong.`三级分类` 
                                AND sl_zong_zhifa.`产品id` = sl_zong.`产品id`
                                AND sl_zong_zhifa.`产品名称` = sl_zong.`产品名称`
                                AND sl_zong_zhifa.`物流方式` = sl_zong.`物流方式`
                                AND sl_zong_zhifa.`旬` = sl_zong.`旬` 
                    LEFT JOIN
                            (   SELECT 币种,
                                        年月,
                                        父级分类,
                                        二级分类,
                                        三级分类,
                                        产品id,
                                        CONCAT(产品id, '#' ,产品名称) 产品名称,
                                        物流方式,
                                        旬,
                                        COUNT(`订单编号`) 拒收订单量,
                                        SUM(`价格RMB`) 拒收销售额,
                                        SUM(`成本价`) 拒收成本,
                                        SUM(`物流花费`) 拒收物流运费,
                                        SUM(`打包花费`) 拒收打包花费,
                                        SUM(`其它花费`) 拒收手续费
                                FROM  {0} sl_cx_jushou
                                WHERE sl_cx_jushou.`币种` = '{1}' 
                                    AND sl_cx_jushou.`年月` >= '{2}'
                                    AND sl_cx_jushou.`年月` <= '{3}'
                                    AND sl_cx_jushou.`系统订单状态`!="已删除"
                                    AND sl_cx_jushou.`最终状态` = "拒收"
                                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                                ORDER BY 币种,年月
                        ) sl_zong_jushou
                        ON sl_zong_jushou.`币种` = sl_zong.`币种` 
                                AND sl_zong_jushou.`年月` = sl_zong.`年月`
                                AND sl_zong_jushou.`父级分类` = sl_zong.`父级分类` 
                                AND sl_zong_jushou.`二级分类` = sl_zong.`二级分类` 
                                AND sl_zong_jushou.`三级分类` = sl_zong.`三级分类` 
                                AND sl_zong_jushou.`产品id` = sl_zong.`产品id`
                                AND sl_zong_jushou.`产品名称` = sl_zong.`产品名称`
                                AND sl_zong_jushou.`物流方式` = sl_zong.`物流方式`
                                AND sl_zong_jushou.`旬` = sl_zong.`旬` 
                    GROUP BY sl_zong.年月,sl_zong.父级分类,sl_zong.产品名称,sl_zong.物流方式,sl_zong.旬
                    with rollup
                    ) sl_zong_wl
                    WHERE sl_zong_wl.`旬` = '合计';'''.format(team, match[team], start_Date, end_Date)
        listT.append(sql2)
        # 直发花费明细表---查询
        sql3 = '''SELECT *
                FROM (
                    SELECT sl_zong.币种,
                        IFNULL(sl_zong.年月,'合计') 年月,
                        IFNULL(sl_zong.父级分类,'合计') 父级分类,
                        IFNULL(sl_zong.二级分类,'合计') 二级分类,
                        IFNULL(sl_zong.三级分类,'合计') 三级分类,
                        IFNULL(sl_zong.产品id,'合计') 产品id,
                        IFNULL(sl_zong.产品名称,'合计') 产品名称,
                        IFNULL(sl_zong.物流方式,'合计') 物流方式,
                        IFNULL(sl_zong.旬,'合计') 旬,
                        SUM(sl_zong.直发订单量) 直发订单量,
                        IFNULL(SUM(sl_zong_zhifa.`已签收订单量`),0) 签收订单量,
                        IFNULL(SUM(sl_zong_jushou.`拒收订单量`),0) 拒收订单量,
                        SUM(sl_zong.直发成本) / SUM(sl_zong.销售额) AS '直发采购/销售额',
                        SUM(sl_zong.物流运费) / SUM(sl_zong.销售额) AS '运费占比',
                        SUM(sl_zong.手续费) / SUM(sl_zong.销售额) AS '手续费占比',
                        IFNULL(SUM(sl_zong_zhifa.`已签收销售额`),0) / (IFNULL(SUM(sl_zong_zhifa.`已签收销售额`),0) + IFNULL(SUM(sl_zong_jushou.`拒收销售额`),0)) AS '金额签收/完成',
                        IFNULL(SUM(sl_zong_zhifa.`已签收销售额`),0) / SUM(sl_zong.销售额) AS '金额签收/总计',
                        (IFNULL(SUM(sl_zong_zhifa.`已签收销售额`),0) + IFNULL(SUM(sl_zong_jushou.`拒收销售额`),0)) / SUM(sl_zong.销售额) AS '金额完成占比',
                        IFNULL(SUM(sl_zong_zhifa.`已签收订单量`),0) / (IFNULL(SUM(sl_zong_zhifa.`已签收订单量`),0) + IFNULL(SUM(sl_zong_jushou.`拒收订单量`),0)) AS '数量签收/完成',
                        (IFNULL(SUM(sl_zong_zhifa.`已签收订单量`),0) + IFNULL(SUM(sl_zong_jushou.`拒收订单量`),0)) / SUM(sl_zong.直发订单量) AS '数量完成占比'
                    FROM (
                        SELECT 币种,
                                年月,
                                父级分类,
                                二级分类,
                                三级分类,
                                产品id,
                                CONCAT(产品id, '#' ,产品名称) 产品名称,
                                物流方式,
                                旬,
                                COUNT(`订单编号`) 直发订单量,
                                SUM(`价格RMB`) 销售额,
                                SUM(`成本价`) 直发成本,
                                SUM(`物流花费`) 物流运费,
                                SUM(`打包花费`) 打包花费,
                                SUM(`其它花费`) 手续费
                        FROM  {0} sl_cx
                        WHERE sl_cx.`币种` = '{1}'
                            AND sl_cx.`年月` >= '{2}'
                            AND sl_cx.`年月` <= '{3}'
                            AND sl_cx.`系统订单状态`!="已删除"
                            AND sl_cx.`是否改派` = "直发"
                        GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                        ORDER BY 币种,年月
                        ) sl_zong
                    LEFT JOIN
                        (SELECT 币种,
                                年月,
                                父级分类,
                                二级分类,
                                三级分类,
                                产品id,
                                CONCAT(产品id, '#' ,产品名称) 产品名称,
                                物流方式,
                                旬,
                                COUNT(`订单编号`) 已签收订单量,
                                SUM(`价格RMB`) 已签收销售额,
                                SUM(`成本价`) 已签收成本,
                                SUM(`物流花费`) 已签收物流运费,
                                SUM(`打包花费`) 已签收打包花费,
                                SUM(`其它花费`) 已签收手续费
                        FROM  {0}	sl_cx_zhifa
                        WHERE sl_cx_zhifa.`币种` = '{1}'
                            AND sl_cx_zhifa.`年月` >= '{2}'
                            AND sl_cx_zhifa.`年月` <= '{3}'
                            AND sl_cx_zhifa.`系统订单状态`!="已删除"
                            AND sl_cx_zhifa.`是否改派` = "直发"
                            AND sl_cx_zhifa.`最终状态` = "已签收"
                        GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                        ORDER BY 币种,年月
                        ) sl_zong_zhifa
                        ON sl_zong_zhifa.`币种` = sl_zong.`币种` 
                            AND sl_zong_zhifa.`年月` = sl_zong.`年月`
                            AND sl_zong_zhifa.`父级分类` = sl_zong.`父级分类` 
                            AND sl_zong_zhifa.`二级分类` = sl_zong.`二级分类` 
                            AND sl_zong_zhifa.`三级分类` = sl_zong.`三级分类` 
                            AND sl_zong_zhifa.`产品id` = sl_zong.`产品id`
                            AND sl_zong_zhifa.`产品名称` = sl_zong.`产品名称`
                            AND sl_zong_zhifa.`物流方式` = sl_zong.`物流方式`
                            AND sl_zong_zhifa.`旬` = sl_zong.`旬` 
                    LEFT JOIN
                        (SELECT 币种,
                                年月,
                                父级分类,
                                二级分类,
                                三级分类,
                                产品id,
                                CONCAT(产品id, '#' ,产品名称) 产品名称,
                                物流方式,
                                旬,
                                COUNT(`订单编号`) 拒收订单量,
                                SUM(`价格RMB`) 拒收销售额,
                                SUM(`成本价`) 拒收成本,
                                SUM(`物流花费`) 拒收物流运费,
                                SUM(`打包花费`) 拒收打包花费,
                                SUM(`其它花费`) 拒收手续费
                        FROM  {0} sl_cx_jushou
                        WHERE sl_cx_jushou.`币种` = '{1}'
                            AND sl_cx_jushou.`年月` >= '{2}'
                            AND sl_cx_jushou.`年月` <= '{3}'
                            AND sl_cx_jushou.`系统订单状态`!="已删除"
                            AND sl_cx_jushou.`是否改派` = "直发"
                            AND sl_cx_jushou.`最终状态` = "拒收"
                        GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                        ORDER BY 币种,年月
                        ) sl_zong_jushou
                    ON sl_zong_jushou.`币种` = sl_zong.`币种` 
                        AND sl_zong_jushou.`年月` = sl_zong.`年月`
                        AND sl_zong_jushou.`父级分类` = sl_zong.`父级分类` 
                        AND sl_zong_jushou.`二级分类` = sl_zong.`二级分类` 
                        AND sl_zong_jushou.`三级分类` = sl_zong.`三级分类` 
                        AND sl_zong_jushou.`产品id` = sl_zong.`产品id`
                        AND sl_zong_jushou.`产品名称` = sl_zong.`产品名称`
                        AND sl_zong_jushou.`物流方式` = sl_zong.`物流方式`
                        AND sl_zong_jushou.`旬` = sl_zong.`旬` 
                GROUP BY sl_zong.年月,sl_zong.父级分类,sl_zong.产品名称,sl_zong.物流方式,sl_zong.旬
                with rollup
                ) sl_zong_wl
                WHERE sl_zong_wl.`旬` = '合计';'''.format(team, match[team], start_Date, end_Date)
        listT.append(sql3)
        listTValue = []                                             # 查询sql的结果 存放池
        for i, sql in enumerate(listT):
            print('正在获取 ' + match[team] + start_Date[4:6] + '-' + end_Date[4:6] + list_value_name[i])
            df = pd.read_sql_query(sql=sql, con=self.engine1)
            df.drop(labels=['旬'], axis=1, inplace=True)            # 去掉多余的旬列表
            columns = list(df.columns)                              # 获取数据的标题名，转为列表
            columns_value = ['采购/销售额', '直发采购/销售额', '运费占比', '手续费占比', '金额签收/完成', '金额签收/总计', '金额完成占比', '数量签收/完成', '数量完成占比']
            for column_val in columns_value:
                if column_val in columns:
                    df[column_val] = df[column_val].fillna(value=0)
                    df[column_val] = df[column_val].apply(lambda x: format(x, '.2%'))
            listTValue.append(df)
        today = datetime.date.today().strftime('%Y.%m.%d')
        filePath = r'D:\Users\Administrator\Desktop\\输出文件\{} {}产品花费明细111表.xlsx'.format(today, match[team])
        if os.path.exists(filePath):                                 # 判断是否有需要的表格
            print("正在使用文件......")
            filePath = filePath
        else:                                                        # 判断是否无需要的表格，进行初始化创建
            print("正在创建文件......")
            df0 = pd.DataFrame([])                                   # 创建空的dataframe数据框
            df0.to_excel(filePath, index=False)                      # 备用：可以向不同的sheet写入数据（创建新的工作表并进行写入）
            filePath = filePath
        print('正在写入excel…………')
        writer = pd.ExcelWriter(filePath, engine='openpyxl')         # 初始化写入对象
        book = load_workbook(filePath)                               # 可以向不同的sheet写入数据（对现有工作表的追加）
        writer.book = book                                           # 将数据写入excel中的sheet2表,sheet_name改变后即是新增一个sheet
        listTValue[0].to_excel(excel_writer=writer, sheet_name=list_value[0], index=False)
        listTValue[1].to_excel(excel_writer=writer, sheet_name=list_value[1], index=False)
        writer.save()
        writer.close()
        print('输出文件成功…………')
        # self.e.send('{} 神龙{}签收表.xlsx'.format(today, match[team]), filePath,
        #             emailAdd[team])
    # 团队订单签收率查询(停用)
    def OrderQuan(self, team, tem):
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
        # yesterday = (datetime.datetime.now()).strftime('%Y-%m-%d') + ' 23:59:59'
        yesterday = (datetime.datetime.now().replace(month=11, day=6)).strftime('%Y-%m')
        print(yesterday)
        # last_month = (datetime.datetime.now().replace(day=1)).strftime('%Y-%m-%d')
        last_month = (datetime.datetime.now().replace(month=11, day=1)).strftime('%Y-%m')
        print(last_month)
        listT = []  # 查询sql的结果 存放池
        if team == 'sltg' or team == 'slrb':
            sql = '''SELECT IFNULL(ql.币种,'合计') 币种,IFNULL(ql.年月,'合计') 年月,IFNULL(ql.是否改派,'合计') 是否改派,IFNULL(ql.父级分类,'合计') 父级分类,IFNULL(ql.产品名称,'合计') 产品名称,IFNULL(ql.物流方式,'合计') 物流方式,IFNULL(ql.旬,'合计') 旬,签收,拒收,在途,未发货,未上线,已退货,理赔,自发头程丢件,已发货,已完成,全部,
        ql.签收 / ql.已完成 AS 完成签收, ql.签收 / ql.全部 AS 总计签收, ql.已完成 / ql.全部 AS 完成占比 ,ql.已发货 / ql.已完成 AS '已完成/已发货' , ql.已退货 / ql.全部 AS 退货率,'' 已发货占比,'' 已完成占比,'' 全部占比 FROM
(SELECT qq.币种,qq.年月,qq.是否改派,qq.父级分类,qq.产品名称,qq.物流方式, qq.旬,sum(签收) 签收,sum(拒收) 拒收,sum(在途) 在途,sum(未发货) 未发货,sum(未上线) 未上线,sum(已退货) 已退货,sum(理赔) 理赔,sum(自发头程丢件) 自发头程丢件,sum(已发货) 已发货,sum(已完成) 已完成,sum(全部) 全部 FROM
(SELECT q.币种,q.年月,q.是否改派,q.父级分类,q.产品名称,q.物流方式, q.旬,已签收 签收,拒收,在途,未发货,未上线,已退货,理赔,自发头程丢件,已发货,已完成 FROM
(SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已签收 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('已签收')
GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) q
LEFT JOIN
(SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 拒收 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('拒收') 
GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) j
ON q.`币种` = j.`币种` AND q.`年月` = j.`年月` AND q.`产品名称` = j.`产品名称` AND q.`物流方式` = j.`物流方式` AND q.`旬` = j.`旬` AND q.`父级分类` = j.`父级分类` AND q.`是否改派` = j.`是否改派`
LEFT JOIN
(SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 在途 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('在途') 
GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) zz
ON  q.`币种` = zz.`币种` AND q.`年月` = zz.`年月`AND q.`产品名称` = zz.`产品名称`AND q.`物流方式` = zz.`物流方式`AND q.`旬` = zz.`旬` AND q.`父级分类` = zz.`父级分类` AND q.`是否改派` = zz.`是否改派`
LEFT JOIN
(SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 未发货 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('未发货') 
GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) wf
ON  wf.`币种` = q.`币种` AND wf.`年月` = q.`年月`AND wf.`产品名称` = q.`产品名称`AND wf.`物流方式` = q.`物流方式`AND wf.`旬` = q.`旬` AND q.`父级分类` = wf.`父级分类` AND q.`是否改派` = wf.`是否改派`
LEFT JOIN
(SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 未上线 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('未上线')
GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) ws
ON  ws.`币种` = q.`币种` AND ws.`年月` = q.`年月`AND ws.`产品名称` = q.`产品名称`AND ws.`物流方式` = q.`物流方式`AND ws.`旬` = q.`旬`AND q.`父级分类` = ws.`父级分类` AND q.`是否改派` = ws.`是否改派`
LEFT JOIN
(SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已退货 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('已退货') 
GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) th
ON  q.`币种` = th.`币种` AND q.`年月` = th.`年月`AND q.`产品名称` = th.`产品名称`AND q.`物流方式` = th.`物流方式`AND q.`旬` = th.`旬`AND q.`父级分类` = th.`父级分类`AND q.`是否改派` = th.`是否改派`
LEFT JOIN
(SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 理赔 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('理赔') 
GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) lp
ON  lp.`币种` = q.`币种` AND lp.`年月` = q.`年月`AND lp.`产品名称` = q.`产品名称`AND lp.`物流方式` = q.`物流方式`AND lp.`旬` = q.`旬`AND q.`父级分类` = lp.`父级分类`AND q.`是否改派` = lp.`是否改派`
LEFT JOIN
(SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 自发头程丢件 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('自发头程丢件') 
GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) zf
ON  zf.`币种` = q.`币种` AND zf.`年月` = q.`年月`AND zf.`产品名称` = q.`产品名称`AND zf.`物流方式` = q.`物流方式`AND zf.`旬` = q.`旬`AND q.`父级分类` = zf.`父级分类`AND q.`是否改派` = zf.`是否改派`
LEFT JOIN
(SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已发货 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货','在途','未上线') 
GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) fh
ON  fh.`币种` = q.`币种` AND fh.`年月` = q.`年月`AND fh.`产品名称` = q.`产品名称`AND fh.`物流方式` = q.`物流方式`AND fh.`旬` = q.`旬`AND q.`父级分类` = fh.`父级分类`AND q.`是否改派` = fh.`是否改派`
LEFT JOIN
(SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已完成 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货') 
GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) wc
ON  wc.`币种` = q.`币种` AND wc.`年月` = q.`年月`AND wc.`产品名称` = q.`产品名称`AND wc.`物流方式` = q.`物流方式`AND wc.`旬` = q.`旬`AND q.`父级分类` = wc.`父级分类`AND q.`是否改派` = wc.`是否改派`
ORDER BY 币种,年月) qq
LEFT JOIN
(SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 全部 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货','在途','未上线','未发货')
GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) qb
ON  qb.`币种` = qq.`币种` AND qb.`年月` = qq.`年月`AND qb.`产品名称` = qq.`产品名称`AND qb.`物流方式` = qq.`物流方式`AND qb.`旬` = qq.`旬`AND qq.`父级分类` = qb.`父级分类`AND qq.`是否改派` = qb.`是否改派`
GROUP BY 年月,是否改派,父级分类,产品名称,物流方式,旬
with rollup) ql
where ql.`年月`>= '{1}' AND ql.`年月` <= '{2}';'''.format(team, last_month, yesterday)
            sql2 = '''SELECT IFNULL(ql.币种,'合计') 币种,IFNULL(ql.年月,'合计') 年月,IFNULL(ql.父级分类,'合计') 父级分类,IFNULL(ql.产品名称,'合计') 产品名称,IFNULL(ql.物流方式,'合计') 物流方式,IFNULL(ql.旬,'合计') 旬,签收,拒收,在途,未发货,未上线,已退货,理赔,自发头程丢件,已发货,已完成,全部,ql.签收 / ql.已完成 AS 完成签收, ql.签收 / ql.全部 AS 总计签收, ql.已完成 / ql.全部 AS 完成占比 ,ql.已发货 / ql.已完成 AS '已完成/已发货' , ql.已退货 / ql.全部 AS 退货率,'' 已发货占比,'' 已完成占比,'' 全部占比 FROM
(SELECT qq.币种,qq.年月,qq.父级分类,qq.产品名称,qq.物流方式, qq.旬,sum(签收) 签收,sum(拒收) 拒收,sum(在途) 在途,sum(未发货) 未发货,sum(未上线) 未上线,sum(已退货) 已退货,sum(理赔) 理赔,sum(自发头程丢件) 自发头程丢件,sum(已发货) 已发货,sum(已完成) 已完成,sum(全部) 全部 FROM
(SELECT q.币种,q.年月,q.父级分类,q.产品名称,q.物流方式, q.旬,已签收 签收,拒收,在途,未发货,未上线,已退货,理赔,自发头程丢件,已发货,已完成 FROM
(SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已签收 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('已签收')
GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) q
LEFT JOIN
(SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 拒收 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('拒收') 
GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) j
ON q.`币种` = j.`币种` AND q.`年月` = j.`年月` AND q.`产品名称` = j.`产品名称` AND q.`物流方式` = j.`物流方式` AND q.`旬` = j.`旬` AND q.`父级分类` = j.`父级分类` 
LEFT JOIN
(SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 在途 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('在途') 
GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) zz
ON  q.`币种` = zz.`币种` AND q.`年月` = zz.`年月`AND q.`产品名称` = zz.`产品名称`AND q.`物流方式` = zz.`物流方式`AND q.`旬` = zz.`旬` AND q.`父级分类` = zz.`父级分类` 
LEFT JOIN
(SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 未发货 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('未发货') 
GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) wf
ON  wf.`币种` = q.`币种` AND wf.`年月` = q.`年月`AND wf.`产品名称` = q.`产品名称`AND wf.`物流方式` = q.`物流方式`AND wf.`旬` = q.`旬` AND q.`父级分类` = wf.`父级分类`
LEFT JOIN
(SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 未上线 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('未上线')
GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) ws
ON  ws.`币种` = q.`币种` AND ws.`年月` = q.`年月`AND ws.`产品名称` = q.`产品名称`AND ws.`物流方式` = q.`物流方式`AND ws.`旬` = q.`旬`AND q.`父级分类` = ws.`父级分类` 
LEFT JOIN
(SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已退货 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('已退货') 
GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) th
ON  q.`币种` = th.`币种` AND q.`年月` = th.`年月`AND q.`产品名称` = th.`产品名称`AND q.`物流方式` = th.`物流方式`AND q.`旬` = th.`旬`AND q.`父级分类` = th.`父级分类`
LEFT JOIN
(SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 理赔 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('理赔') 
GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) lp
ON  lp.`币种` = q.`币种` AND lp.`年月` = q.`年月`AND lp.`产品名称` = q.`产品名称`AND lp.`物流方式` = q.`物流方式`AND lp.`旬` = q.`旬`AND q.`父级分类` = lp.`父级分类`
LEFT JOIN
(SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 自发头程丢件 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('自发头程丢件') 
GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) zf
ON  zf.`币种` = q.`币种` AND zf.`年月` = q.`年月`AND zf.`产品名称` = q.`产品名称`AND zf.`物流方式` = q.`物流方式`AND zf.`旬` = q.`旬`AND q.`父级分类` = zf.`父级分类`
LEFT JOIN
(SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已发货 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货','在途','未上线') 
GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) fh
ON  fh.`币种` = q.`币种` AND fh.`年月` = q.`年月`AND fh.`产品名称` = q.`产品名称`AND fh.`物流方式` = q.`物流方式`AND fh.`旬` = q.`旬`AND q.`父级分类` = fh.`父级分类`
LEFT JOIN
(SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已完成 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货') 
GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) wc
ON  wc.`币种` = q.`币种` AND wc.`年月` = q.`年月`AND wc.`产品名称` = q.`产品名称`AND wc.`物流方式` = q.`物流方式`AND wc.`旬` = q.`旬`AND q.`父级分类` = wc.`父级分类`
ORDER BY 币种,年月) qq
LEFT JOIN
(SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 全部 FROM {0}_zqsb 				
WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货','在途','未上线','未发货')
GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
ORDER BY 币种,年月) qb
ON  qb.`币种` = qq.`币种` AND qb.`年月` = qq.`年月`AND qb.`产品名称` = qq.`产品名称`AND qb.`物流方式` = qq.`物流方式`AND qb.`旬` = qq.`旬`AND qq.`父级分类` = qb.`父级分类`
GROUP BY 年月,父级分类,产品名称,物流方式,旬
with rollup) ql
where ql.`年月`>= '{1}' AND ql.`年月` <= '{2}';'''.format(team, last_month, yesterday)
        else:
            sql = '''SELECT IFNULL(ql.币种,'合计') 币种,IFNULL(ql.年月,'合计') 年月,IFNULL(ql.是否改派,'合计') 是否改派,IFNULL(ql.父级分类,'合计') 父级分类,IFNULL(ql.产品名称,'合计') 产品名称,IFNULL(ql.物流方式,'合计') 物流方式,IFNULL(ql.旬,'合计') 旬,签收,拒收,在途,未发货,未上线,已退货,理赔,自发头程丢件,已发货,已完成,全部,ql.签收 / ql.已完成 AS 完成签收, ql.签收 / ql.全部 AS 总计签收, ql.已完成 / ql.全部 AS 完成占比 ,ql.已发货 / ql.已完成 AS '已完成/已发货' , ql.已退货 / ql.全部 AS 退货率,'' 已发货占比,'' 已完成占比,'' 全部占比 FROM
                (SELECT qq.币种,qq.年月,qq.是否改派,qq.父级分类,qq.产品名称,qq.物流方式, qq.旬,sum(签收) 签收,sum(拒收) 拒收,sum(在途) 在途,sum(未发货) 未发货,sum(未上线) 未上线,sum(已退货) 已退货,sum(理赔) 理赔,sum(自发头程丢件) 自发头程丢件,sum(已发货) 已发货,sum(已完成) 已完成,sum(全部) 全部 FROM
                (SELECT q.币种,q.年月,q.是否改派,q.父级分类,q.产品名称,q.物流方式, q.旬,已签收 签收,拒收,在途,未发货,未上线,已退货,理赔,自发头程丢件,已发货,已完成 FROM
                (SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已签收 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('已签收') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) q
                LEFT JOIN
                (SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 拒收 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('拒收')  AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) j
                ON q.`币种` = j.`币种` AND q.`年月` = j.`年月` AND q.`产品名称` = j.`产品名称` AND q.`物流方式` = j.`物流方式` AND q.`旬` = j.`旬` AND q.`父级分类` = j.`父级分类` AND q.`是否改派` = j.`是否改派`
                LEFT JOIN
                (SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 在途 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('在途') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) zz
                ON  q.`币种` = zz.`币种` AND q.`年月` = zz.`年月`AND q.`产品名称` = zz.`产品名称`AND q.`物流方式` = zz.`物流方式`AND q.`旬` = zz.`旬` AND q.`父级分类` = zz.`父级分类` AND q.`是否改派` = zz.`是否改派`
                LEFT JOIN
                (SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 未发货 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('未发货') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) wf
                ON  wf.`币种` = q.`币种` AND wf.`年月` = q.`年月`AND wf.`产品名称` = q.`产品名称`AND wf.`物流方式` = q.`物流方式`AND wf.`旬` = q.`旬` AND q.`父级分类` = wf.`父级分类` AND q.`是否改派` = wf.`是否改派`
                LEFT JOIN
                (SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 未上线 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('未上线')AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) ws
                ON  ws.`币种` = q.`币种` AND ws.`年月` = q.`年月`AND ws.`产品名称` = q.`产品名称`AND ws.`物流方式` = q.`物流方式`AND ws.`旬` = q.`旬`AND q.`父级分类` = ws.`父级分类` AND q.`是否改派` = ws.`是否改派`
                LEFT JOIN
                (SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已退货 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('已退货') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) th
                ON  q.`币种` = th.`币种` AND q.`年月` = th.`年月`AND q.`产品名称` = th.`产品名称`AND q.`物流方式` = th.`物流方式`AND q.`旬` = th.`旬`AND q.`父级分类` = th.`父级分类`AND q.`是否改派` = th.`是否改派`
                LEFT JOIN
                (SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 理赔 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('理赔') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) lp
                ON  lp.`币种` = q.`币种` AND lp.`年月` = q.`年月`AND lp.`产品名称` = q.`产品名称`AND lp.`物流方式` = q.`物流方式`AND lp.`旬` = q.`旬`AND q.`父级分类` = lp.`父级分类`AND q.`是否改派` = lp.`是否改派`
                LEFT JOIN
                (SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 自发头程丢件 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('自发头程丢件') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) zf
                ON  zf.`币种` = q.`币种` AND zf.`年月` = q.`年月`AND zf.`产品名称` = q.`产品名称`AND zf.`物流方式` = q.`物流方式`AND zf.`旬` = q.`旬`AND q.`父级分类` = zf.`父级分类`AND q.`是否改派` = zf.`是否改派`
                LEFT JOIN
                (SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已发货 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货','在途','未上线') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) fh
                ON  fh.`币种` = q.`币种` AND fh.`年月` = q.`年月`AND fh.`产品名称` = q.`产品名称`AND fh.`物流方式` = q.`物流方式`AND fh.`旬` = q.`旬`AND q.`父级分类` = fh.`父级分类`AND q.`是否改派` = fh.`是否改派`
                LEFT JOIN
                (SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已完成 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) wc
                ON  wc.`币种` = q.`币种` AND wc.`年月` = q.`年月`AND wc.`产品名称` = q.`产品名称`AND wc.`物流方式` = q.`物流方式`AND wc.`旬` = q.`旬`AND q.`父级分类` = wc.`父级分类`AND q.`是否改派` = wc.`是否改派`
                ORDER BY 币种,年月) qq
                LEFT JOIN
                (SELECT 币种,年月,是否改派,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 全部 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货','在途','未上线','未发货') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,是否改派,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) qb
                ON  qb.`币种` = qq.`币种` AND qb.`年月` = qq.`年月`AND qb.`产品名称` = qq.`产品名称`AND qb.`物流方式` = qq.`物流方式`AND qb.`旬` = qq.`旬`AND qq.`父级分类` = qb.`父级分类`AND qq.`是否改派` = qb.`是否改派`
                GROUP BY 年月,是否改派,父级分类,产品名称,物流方式,旬
                with rollup) ql;'''.format(team, tem)
            sql2 = '''SELECT IFNULL(ql.币种,'合计') 币种,IFNULL(ql.年月,'合计') 年月,IFNULL(ql.父级分类,'合计') 父级分类,IFNULL(ql.产品名称,'合计') 产品名称,IFNULL(ql.物流方式,'合计') 物流方式,IFNULL(ql.旬,'合计') 旬,签收,拒收,在途,未发货,未上线,已退货,理赔,自发头程丢件,已发货,已完成,全部,ql.签收 / ql.已完成 AS 完成签收, ql.签收 / ql.全部 AS 总计签收, ql.已完成 / ql.全部 AS 完成占比 ,ql.已发货 / ql.已完成 AS '已完成/已发货' , ql.已退货 / ql.全部 AS 退货率,'' 已发货占比,'' 已完成占比,'' 全部占比 FROM
                (SELECT qq.币种,qq.年月,qq.父级分类,qq.产品名称,qq.物流方式, qq.旬,sum(签收) 签收,sum(拒收) 拒收,sum(在途) 在途,sum(未发货) 未发货,sum(未上线) 未上线,sum(已退货) 已退货,sum(理赔) 理赔,sum(自发头程丢件) 自发头程丢件,sum(已发货) 已发货,sum(已完成) 已完成,sum(全部) 全部 FROM
                (SELECT q.币种,q.年月,q.父级分类,q.产品名称,q.物流方式, q.旬,已签收 签收,拒收,在途,未发货,未上线,已退货,理赔,自发头程丢件,已发货,已完成 FROM
                (SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已签收 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('已签收') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) q
                LEFT JOIN
                (SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 拒收 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('拒收') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) j
                ON q.`币种` = j.`币种` AND q.`年月` = j.`年月` AND q.`产品名称` = j.`产品名称` AND q.`物流方式` = j.`物流方式` AND q.`旬` = j.`旬` AND q.`父级分类` = j.`父级分类` 
                LEFT JOIN
                (SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 在途 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('在途') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) zz
                ON  q.`币种` = zz.`币种` AND q.`年月` = zz.`年月`AND q.`产品名称` = zz.`产品名称`AND q.`物流方式` = zz.`物流方式`AND q.`旬` = zz.`旬` AND q.`父级分类` = zz.`父级分类` 
                LEFT JOIN
                (SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 未发货 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('未发货') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) wf
                ON  wf.`币种` = q.`币种` AND wf.`年月` = q.`年月`AND wf.`产品名称` = q.`产品名称`AND wf.`物流方式` = q.`物流方式`AND wf.`旬` = q.`旬` AND q.`父级分类` = wf.`父级分类`
                LEFT JOIN
                (SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 未上线 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('未上线')AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) ws
                ON  ws.`币种` = q.`币种` AND ws.`年月` = q.`年月`AND ws.`产品名称` = q.`产品名称`AND ws.`物流方式` = q.`物流方式`AND ws.`旬` = q.`旬`AND q.`父级分类` = ws.`父级分类` 
                LEFT JOIN
                (SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已退货 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('已退货') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) th
                ON  q.`币种` = th.`币种` AND q.`年月` = th.`年月`AND q.`产品名称` = th.`产品名称`AND q.`物流方式` = th.`物流方式`AND q.`旬` = th.`旬`AND q.`父级分类` = th.`父级分类`
                LEFT JOIN
                (SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 理赔 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('理赔') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) lp
                ON  lp.`币种` = q.`币种` AND lp.`年月` = q.`年月`AND lp.`产品名称` = q.`产品名称`AND lp.`物流方式` = q.`物流方式`AND lp.`旬` = q.`旬`AND q.`父级分类` = lp.`父级分类`
                LEFT JOIN
                (SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 自发头程丢件 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('自发头程丢件') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) zf
                ON  zf.`币种` = q.`币种` AND zf.`年月` = q.`年月`AND zf.`产品名称` = q.`产品名称`AND zf.`物流方式` = q.`物流方式`AND zf.`旬` = q.`旬`AND q.`父级分类` = zf.`父级分类`
                LEFT JOIN
                (SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已发货 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货','在途','未上线') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) fh
                ON  fh.`币种` = q.`币种` AND fh.`年月` = q.`年月`AND fh.`产品名称` = q.`产品名称`AND fh.`物流方式` = q.`物流方式`AND fh.`旬` = q.`旬`AND q.`父级分类` = fh.`父级分类`
                LEFT JOIN
                (SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 已完成 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) wc
                ON  wc.`币种` = q.`币种` AND wc.`年月` = q.`年月`AND wc.`产品名称` = q.`产品名称`AND wc.`物流方式` = q.`物流方式`AND wc.`旬` = q.`旬`AND q.`父级分类` = wc.`父级分类`
                ORDER BY 币种,年月) qq
                LEFT JOIN
                (SELECT 币种,年月,父级分类,CONCAT(产品id, 产品名称) 产品名称,物流方式, 旬,COUNT(最终状态) 全部 FROM {0}_zqsb 				
                WHERE {0}_zqsb.最终状态 IN ('已签收','拒收','理赔','已退货','在途','未上线','未发货') AND {0}_zqsb.`币种` = '{1}'
                GROUP BY 币种,年月,父级分类,产品名称,物流方式,旬
                ORDER BY 币种,年月) qb
                ON  qb.`币种` = qq.`币种` AND qb.`年月` = qq.`年月`AND qb.`产品名称` = qq.`产品名称`AND qb.`物流方式` = qq.`物流方式`AND qb.`旬` = qq.`旬`AND qq.`父级分类` = qb.`父级分类`
                GROUP BY 年月,父级分类,产品名称,物流方式,旬
                with rollup) ql;'''.format(team, tem)
        print('正在获取-' + match1[team] + '品类直发签收率…………')
        df = pd.read_sql_query(sql=sql, con=self.engine1)
        listT.append(df)
        print('正在获取-' + match1[team] + '品类签收率…………')
        df2 = pd.read_sql_query(sql=sql2, con=self.engine1)
        listT.append(df2)
        print('----已获' + match1[team] + '品类签收率')
        listTValue = []  # 查询df的结果 存放池
        for df_val in listT:
            columns = list(df_val.columns)  # 获取数据的标题名，转为列表
            columns_value = ['完成签收', '总计签收', '完成占比', '已完成/已发货', '退货率']
            for column_val in columns_value:
                if column_val in columns:
                    df_val[column_val] = df_val[column_val].fillna(value=0)
                    df_val[column_val] = df_val[column_val].apply(lambda x: format(x, '.2%'))
            print(df_val)
            listTValue.append(df_val)
        print('正在写入EXECL中…………')
        today = datetime.date.today().strftime('%Y.%m.%d')
        df0 = pd.DataFrame(columns=['A', 'B', 'C', 'D'])
        df0.to_excel('D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}---(品类)签收率.xlsx'.format(today, match1[team]),
                     index=False)
        writer = pd.ExcelWriter(
            'D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}---(品类)签收率.xlsx'.format(today, match1[team]))
        listTValue[0].to_excel(excel_writer=writer, sheet_name=match1[team], index=False)
        listTValue[1].to_excel(excel_writer=writer, sheet_name=match1[team] + '直发', index=False)
        writer.save()
        writer.close()
        print('----已写入excel')
        # filePath = ['D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}---签收率.xlsx'.format(today, match1[team])]
        # print('输出文件成功…………')
        # self.e.send('{} 神龙{}签收表.xlsx'.format(today, match[team]), filePath,
        #             emailAdd[team])
if __name__ == '__main__':
    w = WlMysql()
    # team = 'slxmt'
    # for tem in ['新加坡', '马来西亚']:
    #     w.OrderQuan(team, tem)

    # 订单签收率查询
    # match9 = {'slgat_zqsb': '港台',
    #           'sltg_zqsb': '泰国',
    #           'slxmt_zqsb': '新马',
    #           'slrb_zqsb_rb': '日本'}
    # tem = '日本'
    # team = 'slrb_zqsb_rb'
    # w.sl_tem_costT(team)
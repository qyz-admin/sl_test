import pandas as pd
import os
import xlwings
import numpy as np
import datetime
from mysqlControl import MysqlControl
class ExcelControl():
    '''
    excel的导入和整理
    '''
    # __slots__ = ['filePath', 'team', 'sql']
    def __init__(self):
        self.sql = MysqlControl()
        '''用来和数据库通信'''
    def readExcel(self, filePath, team):
        '''
        读取指定团队的指定文件，获取的数据整理格式之后，导入数据库
        :param filePath: excel文件路径
        :param team: 团队的代码
        :return: 无返回值
        '''
        # 文件扩展名
        fileType = os.path.splitext(filePath)[1]
        # 用xlwings来读取excel，因为pd.excel_read总是失败
        app = xlwings.App(visible=False, add_book=False)
        # 不显示excel窗口
        app.display_alerts = False

        if 'xls' in fileType:
            # 打开excel文件，只读方式，不更新公式，连接
            wb = app.books.open(filePath, update_links=False, read_only=True)
            # 遍历每个sheet
            for sht in wb.sheets:
                # 部分不可能是明细表的sheet直接排除，因为这些表的字段结构和明细表可能相似。
                if '问题件' not in sht.name and '录单' not in sht.name and \
                        '历史' not in sht.name and '取件' not in sht.name and \
                        '异常' not in sht.name and sht.api.visible == -1:
                    try:    # 可能会读取sheet内容失败，所以写了这个
                        db = None
                        # 读取sht的所有已使用单元格内容，并转换为pd的DateFrame格式
                        file = sht.used_range.options(pd.DataFrame, header=1,
                                                      numbers=int, index=False).value
                        # print(file)
                        # 如果xlwings的直接转换失败的话。读取单元格值，并转换为db.DateFrame格式
                        if file.empty or sht.name == '宅配':
                            lst = sht.used_range.value
                            file = pd.DataFrame(lst[1:], columns=lst[0])
                            # print(file)
                        db = self.isRightSheet(file, team, sht.name)
                        # print(db)
                    except Exception as e:
                        print('xxxx查看失败：' + sht.name, str(Exception) + str(e))
                    if db is not None and len(db) > 0:
                        print('++++正在导入：' + sht.name + ' 共：' + str(len(db)) + '行', 'sheet共：' + str(
                            sht.used_range.last_cell.row) + '行')
                        # 将返回的dateFrame导入数据库的临时表
                        self.sql.writeSqlReplace(db)
                        print('++++正在更新：' + sht.name + '--->>>到总订单')
                        # print(db)
                        # 将数据库的临时表替换进指定的总表
                        self.sql.replaceInto(team, list(db.columns))
                        print('++++----->>>' + sht.name + '：订单更新完成++++')
                    else:
                        print('----------不用导入(五项条件不足)：' + sht.name)
                else:
                    print('----不用导入：' + sht.name)
            # 关闭excel文件
            wb.close()
            # 退出excel app
            app.quit()
    def isRightSheet(self, df, team, shtName):
        '''
        根据团队，判断DateFrame是否是正确的明细表，并整理数据，输出需要的格式
        :param df: 要判断的DateFrame
        :param team: 哪个团队的数据
        :param shtName: sht名字，有些sheet需要特殊处理
        :return: 可以直接导入数据库的DateFrame
        '''
        math = {'slrb': {'出货时间': [True, ['出货日期', '日期', '出货日', '发货日期', '业务日期', '物流发货日期'], []],
                         '订单编号': [True, ['原单号', '顾客管理号码', '订单编号', '订单号', '内单号', '单号'], []],
                         '运单编号': [True, ['渠道转单号', '系统运单号', '转单号', 'BJT转单号', '系统运单号', '运单编号',
                                         '运单号', '跟踪单号', '改派单号'], []],
                         '物流状态': [True, ['状况', '物流状态', '状态'], []],
                         '状态时间': [True, ['轨迹日期', '状态时间', '时间', '末条信息日期时间', '出货预定日'], []],
                         '航班时间': [False, ['航班起飞时间'], []],
                         '清关时间': [False, ['日本清关时间'], []],
                         '上线时间': [False, ['''上线时间
（即货交地派时间）''', '上线时间（即货交地派时间）', '日本清关时间', '发送操作时间', '清关通行时间'], ['即货交地派时间']],
                         '原运单号': [False, ['原包裹运单号(可含多个)', '原运单号', '转单号'], []]},
                'sltg': {'出货时间': [True, ['提货日期', '发货日期', '接收订单资料日期', '出货时间', '揽收日期', '日期'], []],
                         '订单编号': [True, ['订单号', '新订单号'], []],
                         '运单编号': [True, ['运单号', '新运单号', '转单号', '跟踪单号'], []],
                         '物流状态': [True, ['物流状态', '订单状态', '最新扫描类型'], []],
                         '状态时间': [True, ['状态时间', '時間', '最新扫描时间'], []],
                         '航班时间': [False, ['航班起飞时间', '''国内清关时间
（或航班起飞时间）'''], ['航班起飞时间']],
                         '清关时间': [False, ['''泰国清关时间
（可用到泰国时间代替）''', '泰国清关时间'], ['泰国清关时间']],
                         '上线时间': [False, ['''上线时间
（即货交地派时间）''', '上线时间'], ['货交地派时间']],
                         '原运单号': [False, ['原包裹运单号(可含多个)', '原运单号'], []]},
                'slgat': {'出货时间': [True, ['出货日期', '出货时间', '核重时间', '出库日期', '重出日期', '安排日期',
                                          '收件日期', '业务日期', '出库时间', '发货日期'], []],
                          '订单编号': [True, ['订单编号', '订单号', '订单号码', '客户单号', '内部单号', '原始订单号',
                                          '件號', '件号'], []],
                          '运单编号': [True, ['运单号', '新单号', '提单号', '查件单号', '重出單號', '重出单号', '重出新單號',
                                          '重出新单号', '承运单号', '运单编号', '转单号码', 'SF转单号', '转单号', '转单'], []],
                          '物流状态': [True, ['物流状态', '状态', '运单最新状态', '貨態', '货态', '货态内容',
                                          '新单号货态'], []],
                          '状态时间': [True, ['最新状态时间', '最新货态日期', '末条时间', '运单最新状态时间', '状态时间',
                                          '最终状态时间', '最终货态时间', '新货态日期', '最新状态', '签收时间',
                                          '时间'], []],
                          '航班时间': [False, ['航班起飞时间', '''国内清关时间
（或航班起飞时间）''', '起飞时间'], ['航班起飞时间']],
                          '清关时间': [False, ['''泰国清关时间
（可用到泰国时间代替）''', '清关时间'], ['泰国清关时间']],
                          '上线时间': [False, ['''上线时间
（即货交地派时间）''', '上线时间', '新竹上线时间'], ['货交地派时间']],
                          '原运单号': [False, ['原单号', '原單號', '原始顺丰订单号'], []]},
                'slxmt': {'出货时间': [True, ['出货时间', 'Inbound Datetime'], []],
                        '订单编号': [True, ['订单号', '订单编号', 'Shipper Order Number', 'Shipper Reference Number'], []],
                        '运单编号': [True, ['转单号', '运单号', 'Tracking ID', 'Tracking Id ', 'tracking_id', 'Tracking ID'], []],
                        '物流状态': [True, ['状态', 'Granular Status', 'Status', 'status'], []],
                        '状态时间': [True, ['Last Update/Scan', 'Last Delivery Date', 'Last Delivey Date', '日期', 'Latest Service End Time',
                                        'Last Valid Delivery Attempt Datetime',
                                        'Last Valid Delivery Attempt Date', 'Last Valid'], []],
                        '航班时间': [False, [], []],
                        '清关时间': [False, [], []],
                        '上线时间': [False, ['提取时间', 'Inbound Date'], []],
                        '原运单号': [False, [], []]}
                }
        necessary = 0
        # 初始化字段是否是必须的字段计数
        unnecessary = 0
        # 初始化字段是否是非必须的字段计数
        needDrop = []
        columns = list(df.columns)
        if team == 'slgat':
            if '运单号' in columns and '查件单号' in columns and '订单编号' in columns and '换单号' in columns:
                df.drop(labels=['查件单号'], axis=1, inplace=True)     # 速派7-11的去掉多余的查件单号
            if '运单编号' in columns and '客户单号' in columns and '转单号' in columns:
                df.drop(labels=['转单号'], axis=1, inplace=True)     # 顺航的去掉多余的转单号
            if '运单编号' in columns and '件号' in columns and '转单号' in columns:
                df.drop(labels=['运单编号'], axis=1, inplace=True)   # 立邦的去掉多余的运单编号
            if '新单号' in columns and '承运单号' in columns:
                df.drop(labels=['承运单号'], axis=1, inplace=True)   # 天马的去掉多余的承运单号
        if team == 'slxmt':
            if '出货时间' not in df:
                df.insert(0, '出货时间', '')
            if '订单编号' not in df:
                df.insert(0, '订单编号', '')
            # df['状态时间'] = pd.to_datetime(df['状态时间'])
            # print(df)
            # print(df.columns)
        if team == 'slrb':
            if '内单号' in columns and '转单号' in columns and '原单号' in columns:  # 吉客印神龙直发签收表JP使用
                df.drop(labels=['转单号'], axis=1, inplace=True)
                df.rename(columns={'内单号': '运单编号'}, inplace=True)
            if '单号' in columns and '转单号' in columns and '改派单号' in columns:  # 返品改派签收表使用
                df.drop(labels=['转单号'], axis=1, inplace=True)
            if '运单号' in columns and '转单号' in columns:  # 返品表使用
                # df.drop(labels=['转单号'], axis=1, inplace=True)
                df.rename(columns={'转单号': '原运单号'}, inplace=True)
            if 'BJT转单号' in columns and '跟踪单号' in columns:
                df.drop(labels=['跟踪单号'], axis=1, inplace=True)
                # print(df.columns)
            if '订单号' in columns and '原单号' in columns:
                df.drop(labels=['原单号'], axis=1, inplace=True)
                # print(df.columns)
            if '运单号' in columns:
                df['运单号'] = df['运单号'].str.strip()  # 去掉运单号中的前后空字符串
            if '订单编号' not in df:
                df.insert(0, '订单编号', '')
        if team == 'sltg':
            if '订单号' not in df:
                df.insert(0, '订单号', '')
        if shtName == '宅配':
            # 宅配的原单编号可能出现重复，重复的话，会在A字段列，出现一个新的单号，以 A原运单号 的形式出现，所以如果A字
            # 段列有内容的话，直接替换掉原有的运单编号的列就能得到符合后台的运单编号了。
            df['重出新单号'] = np.where(df['A'].isnull(), df['重出新单号'], df['A'])
        elif shtName == '总明细':
            # 有一个叫做总明细的sheet，里面会重复出现字段，且前面的字段有内容，所以要丢弃后面无用的字段。避免冲突
            columns = list(df.columns)
            for index, column in enumerate(columns):
                if column == '状态' and index > 10:
                    columns[index] = column + str(index)
                    df.columns = columns
                    break
            df.drop(columns=df.columns[10:], axis=1, inplace=True)
            if team == 'slgat':
                if '状态' in columns and '货态' in columns:
                    df.drop(labels=['货态'], axis=1, inplace=True)
        elif shtName == 'LIST':
            # 有个叫LIST的sheet，在系统里所有的运单号和订单编号一样，所以把签收表里面的运单编号，替换成订单编号
            df['渠道转单号'] = df['内单号']
        columns = list(df.columns)
        # print(df)
        # 保留一个列名，后面要用
        for index, column in enumerate(columns):
            if not column:
                # 如果列名为空，肯定不是需要的列，起一个名字，标记，后面要删除
                columns[index] = 'needDrop' + str(index)
                column = 'needDrop' + str(index)
            for k, v in math[team].items():
                # 遍历字段匹配字典
                if column in v[1]:
                    # 如果列名完全匹配需要的字段，则，字段重命名为标准字段名
                    columns[index] = k
                    if k in columns[:index]:
                        # 如果这个需要的字段，之前出现过，则添加到需要删除的列表里面
                        tem = k + str(columns.index(k, 0, index))
                        columns[columns.index(k, 0, index)] = tem
                        needDrop.append(tem)
                        if v[0]:
                            necessary -= 1
                    break
                else:
                    for vs in v[2]:
                        # 模糊匹配，因为担心出错，所以模糊匹配的关键字，没有写。所以这一段，应该不会生效。
                        if vs in column:
                            columns[index] = k
                            if k in columns[:index]:
                                tem = k + str(columns.index(k, 0, index))
                                columns[columns.index(k, 0, index)] = tem
                                needDrop.append(tem)
                                if v[0]:
                                    necessary -= 1
                            break
            if k != columns[index]:
                needDrop.append(columns[index])
            else:
                if v[0]:
                    necessary += 1
                else:
                    unnecessary += 1
        # print(df.columns)
        # print(df)
        # print(needDrop)
        if necessary >= 5:
            df.columns = columns
            df.drop(labels=needDrop, axis=1, inplace=True)
            df.dropna(axis=0, subset=['运单编号'], inplace=True)
            # print(df.columns)
            if team == 'slrb':
                try:
                    df['状态时间'] = df['状态时间'].replace(to_replace=0, value=datetime.datetime(1990, 1, 1, 0, 0))
                    df['状态时间'] = df['状态时间'].replace(to_replace='客人联系保管', value=(datetime.datetime.now() - datetime.timedelta(days=1)))
                    #df['上线时间'] = df['上线时间'].replace(to_replace=0, value=datetime.datetime(1990, 1, 1, 0, 0))
                    df['状态时间'] = df['状态时间'].fillna(value=datetime.datetime(1990, 1, 1, 0, 0))
                    df['物流状态'] = df['物流状态'].fillna(value='未上线')
                    # df['状态时间'] = df['状态时间'].str.strip()
                    df['订单编号'] = df['订单编号'].str.replace('原内单号：', '')
                    df['订单编号'] = df['订单编号'].str.replace('原单:', '')
                    # print(df['出货时间'])
                    # print(df['运单编号'])
                except Exception as e:
                    print('----修改状态时间失败：', str(Exception) + str(e))
            if shtName in ['新竹']:
                df['订单编号'] = df['订单编号'].str.replace('原单:', '')
            if shtName in ['全家']:
                df['订单编号'] = df['订单编号'].str.replace('原单:', '')
                df['运单编号'] = df['订单编号']
            df['运单编号'] = df['运单编号'].astype(str)
            df['运单编号'] = df['运单编号'].replace(to_replace=r'\.0$', regex=True, value=r'')
            if '原运单号' in columns:
                df['原运单号'] = df['原运单号'].astype(str)
                df['原运单号'] = df['原运单号'].replace(to_replace=r'\.0$', regex=True, value=r'')
            if team == 'slgat':
                df['状态时间'] = df['状态时间'].replace(to_replace=0, value=datetime.datetime(1990, 1, 1, 0, 0))
                df['状态时间'] = df['状态时间'].fillna(value=datetime.datetime(1990, 1, 1, 0, 0))
                df['订单编号'] = df['订单编号'].astype(str)
                df = df[~(df['订单编号'].str.contains('TW|XM'))]
                df.reset_index(drop=True, inplace=True)
            elif team == 'sltg':
                df['订单编号'] = df['订单编号'].astype(str)
                df = df[~(df['订单编号'].str.contains('BJ|GK|KD|NB|NR|TG|TR|XM'))]
                df.reset_index(drop=True, inplace=True)
            return df
        else:
            return None
    def readReturnOrder(self, team):
        import os
        path = r'D:\Users\Administrator\Desktop\需要用到的文件\退货'
        dirs = os.listdir(path=path)
        for dir in dirs:
            filePath = os.path.join(path, dir)
            print(filePath)
            if dir[:2] != '~$':
                df = pd.read_excel(filePath)
                df.columns = ['订单编号']
                self.sql.writeSqlReplace(df)
                sql = 'INSERT IGNORE INTO {}_return (订单编号，添加时间) SELECT 订单编号, NOW() 添加时间 FROM tem; '.format(team)
                self.sql.replaceInto(team + '_return', list(df.columns))
                print('退货更新文件成功…………')
                os.remove(filePath)
                print('已清除退货文件…………')
if __name__ == '__main__':
    e = ExcelControl()
    match = {'slrb': r'D:\Users\Administrator\Desktop\需要用到的文件\退货',
             'sltg': r'D:\Users\Administrator\Desktop\需要用到的文件\退货',
             'slgat': r'D:\Users\Administrator\Desktop\需要用到的文件\退货',
             'slxmt': r'D:\Users\Administrator\Desktop\需要用到的文件\退货'}
    e.readReturnOrder('slgat')                   # 先退货退款导入 后进行签收表的计算
import os
import re
import datetime
import xlwings
import pandas as pd
from wlMysql import WlMysql
from mysqlControl import MysqlControl

class WlExecl():
    # 物流excel的导入和整理
    def __init__(self):
        self.sql = WlMysql()
        # 用来和数据库通信
    def logisitis(self,filePath, team):
        FileType = os.path.splitext(filePath)[1]
        print(FileType)
        app = xlwings.App(visible=False, add_book=False)
        app.display_alerts = False

        if 'xls' in FileType:
            wl = app.books.open(filePath, update_links=False, read_only=True)
            for sht in wl.sheets:
                if '材积' not in sht.name and '重量' not in sht.name and '发票明细' not in sht.name and sht.api.visible == -1:
                    try:
                        wb = None
                        File = sht.used_range.options(pd.DataFrame, header=1,
                                                      numbers=int, index=False).value
                        if File.empty:
                            wlst = sht.used_range.value
                            File = pd.DataFrame(wlst[1:], columns=wlst[0])
                        gk = os.path.basename(filePath)  # 获取文件名
                        if gk[:6] == 'Giikin':   # 需单独添加日期时间
                            st = re.search(r'\d+', filePath).group() # 获取文件名中的日期
                            # print(st[len(st)-4:-2])  # 获取文件名中的具体日期
                            # print(st[len(st) - 2:])
                            tm = datetime.datetime.strptime('2020-' + st[len(st)-4:-2] + '-' + st[len(st) - 2:], '%Y-%m-%d')
                            # tm = (tm - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                            tm = tm.strftime('%Y-%m-%d')
                            # print(tm)
                            # print(sht.used_range.last_cell.row)
                            # print(sht.used_range.last_cell.column)
                            File.insert(sht.used_range.last_cell.column, '出货时间', tm)
                            print(File)
                            wb = self.isRightShet(File, team, sht.name)
                        else:
                            wb = self.isRightShet(File, team, sht.name)
                        print(wb)
                    except Exception as e:
                        print('xxxx 查看失败：' + sht.name, str(Exception) + str(e))
                    if wb is not None and len(wb) > 0:
                        print('++++ 正在导入：' + sht.name + ' 共：' + str(len(wb)) + '行', 'sheet共：' + str(
                            sht.used_range.last_cell.row) + '行')
                        self.sql.writeSqlWl(wb)
                        print('---- 导入完成----')
                        print('++++ 正在更新：' + sht.name + '--->>>到总订单')
                        self.sql.wlInto(team, list(wb.columns))
                        print('++++' + sht.name + '--->>>订单更新完成++++')
                    else:
                        print('---- 不用导入：' + sht.name)
                else:
                    print('---- 不用导入：' + sht.name)
            wl.close()
        app.quit()
    def isRightShet(self,File,team,shtname):
        # print(File)
        math = {'slxmt': {'出货时间': [True, ['Outbound Time', '出货时间'], []],
                            '运单编号': [True, ['LM Tracking', '运单号'], []],
                         '订单编号': [False, [], []],
                         '物流状态': [False, [], []],
                         '状态时间': [False, [], []]},
                        '上线时间': [False, [], []],
                'sltg': {'出货时间': [True, ['提货日期', '发货日期', '接收订单资料日期', '出货时间'], []],
                           '运单编号': [True, ['运单号', '新运单号', '转单号', '跟踪单号'], []],
                         '订单编号': [False, ['订单号', '新订单号'], []],
                         '物流状态': [False, ['物流状态', '订单状态'], []],
                         '原运单号': [False, ['原包裹运单号(可含多个)', '原运单号'], []]},
                'slgat': {'出货时间': [True, ['出货日期', '出货时间', '核重时间', '重出日期', '安排日期',], []],
                            '运单编号': [True, ['运单号', '新单号', '提单号', '承运单号', '运单编号', '转单号'], []],
                         '订单编号': [False, ['订单编号', '订单号', '内部单号', '原始订单号','件號', '件号'], []],
                         '物流状态': [False, ['物流状态', '状态', '货态', '货态内容','新单号货态'], []],
                         '原运单号': [False, ['原单号', '原單號', '原始顺丰订单号'], []]},
                'slrb': {'出货时间': [True, ['提取时间', 'Inbound Date'], []],
                           '运单编号': [True, ['转单号', '运单号', 'Tracking Id', 'Tracking Id '], []],
                          '订单编号': [False, ['订单号','订单编号', 'Shipper Order Number'], []],
                          '物流状态': [False, ['状态', 'Granular Status', 'Status', 'status'], []],
                            '状态时间': [False, ['日期', 'Latest Service End Time'], []]}
        }
        necessary = 0
        unnecessary = 0
        needDrop = []
        columns = list(File.columns)
        for index, column in enumerate(columns):
            if not column:
                columns[index] = 'needDrop' + str(index)
                column = 'needDrop' + str(index)
            for k, v in math[team].items():
                if column in v[1]:
                    columns[index] = k
                    if k in columns[:index]:
                        tem = k + str(columns.index(k, 0, index))
                        columns[columns.index(k, 0, index)] = tem
                        needDrop.append(tem)
                        if v[0]:
                            necessary -= 1
                    break
                else:
                    for vs in v[2]:
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
        # print(needDrop)
        if necessary >= 2:
            File.columns = columns
            File.drop(labels=needDrop, axis=1, inplace=True)
            File.dropna(axis=0, subset=['运单编号'], inplace=True)
            print(File)
            if shtname in ['新竹']:
                File['订单编号'] = File['订单编号'].str.replace('原单:', '')
            if team == 'slgat':
                File['订单编号'] = File['订单编号'].astype(str)
                File = File[~(File['订单编号'].str.contains('TW|XM'))]
                File.reset_index(drop=True, inplace=True)
            elif team == 'sltg':
                File['订单编号'] = File['订单编号'].astype(str)
                File = File[~(File['订单编号'].str.contains('BJ|GK|KD|NB|NR|TG|TR|XM'))]
                File.reset_index(drop=True, inplace=True)
            return File
        else:
            return None

    # 全部订单查询详情使用
    def queryExecl(self,filePath, team):
        fileType = os.path.splitext(filePath)[1]
        print(fileType)
        app = xlwings.App(visible=False, add_book=False)
        app.display_alerts = False

        if 'xls' in fileType:
            wb = app.books.open(filePath, update_links=False, read_only=True)
            for sht in wb.sheets:
                try:
                    db = None
                    file = sht.used_range.options(pd.DataFrame, header=1,
                                                  numbers=int, index=False).value
                    if file.empty or sht.name == '宅配':
                        lst = sht.used_range.value
                        file = pd.DataFrame(lst[1:], columns=lst[0])
                except Exception as e:
                    print('xxxx查看失败：' + sht.name, str(Exception) + str(e))
                print('++++正在导入：' + sht.name + ' 共：' + str(len(db)) + '行', 'sheet共：' + str(
                    sht.used_range.last_cell.row) + '行')
                # 将返回的dateFrame导入数据库的临时表
                self.sql.writeSqlReplace(db)
                print('++++正在更新：' + sht.name + '--->>>到总订单')
                # 将数据库的临时表替换进指定的总表
                self.sql.replaceInto(team, list(db.columns))
                print('++++----->>>' + sht.name + '：订单更新完成++++')
    def qianshoubiao(self, filePath, team):
        fileType = os.path.splitext(filePath)[1]
        app = xlwings.App(visible=False, add_book=False)
        # 不显示excel窗口
        app.display_alerts = False

        if 'xls' in fileType:
            wb = app.books.open(filePath, update_links=False, read_only=True)
            # 遍历每个sheet
            for sht in wb.sheets:
                if '问题件' not in sht.name and '录单' not in sht.name and \
                        '历史' not in sht.name and '取件' not in sht.name and \
                        '异常' not in sht.name and sht.api.visible == -1:
                    try:
                        db = None
                        file = sht.used_range.options(pd.DataFrame, header=1,
                                                      numbers=int, index=False).value
                        # 如果xlwings的直接转换失败的话。读取单元格值，并转换为db.DateFrame格式
                        if file.empty or sht.name == '宅配':
                            lst = sht.used_range.value
                            file = pd.DataFrame(lst[1:], columns=lst[0])
                        db = file
                    except Exception as e:
                        print('xxxx查看失败：' + sht.name, str(Exception) + str(e))
                    if db is not None and len(db) > 0:
                        print('++++正在导入：' + sht.name + ' 共：' + str(len(db)) + '行', 'sheet共：' + str(
                            sht.used_range.last_cell.row) + '行')
                        # 将返回的dateFrame导入数据库的临时表
                        self.sql.SqlWl(db)
                        print('++++正在更新：' + sht.name + '--->>>到总订单')
                        # 将数据库的临时表替换进指定的总表
                        # self.sql.replaceInto(team, list(db.columns))
                        # print('++++----->>>' + sht.name + '：订单更新完成++++')
                    else:
                        print('----------不用导入(五项条件不足)：' + sht.name)
                else:
                    print('----不用导入：' + sht.name)
            # 关闭excel文件
            wb.close()
            # 退出excel app
            app.quit()
    def wuliubiao(self, filePath, team):
        fileType = os.path.splitext(filePath)[1]
        app = xlwings.App(visible=False, add_book=False)
        # 不显示excel窗口
        app.display_alerts = False
        if 'xls' in fileType:
            wb = app.books.open(filePath, update_links=False, read_only=True)
            # 遍历每个sheet
            for sht in wb.sheets:
                if '问题件' not in sht.name and '录单' not in sht.name and \
                        '历史' not in sht.name and '取件' not in sht.name and \
                        '异常' not in sht.name and sht.api.visible == -1:
                    try:
                        db = None
                        file = sht.used_range.options(pd.DataFrame, header=1,
                                                      numbers=int, index=False).value
                        # 如果xlwings的直接转换失败的话。读取单元格值，并转换为db.DateFrame格式
                        if file.empty or sht.name == '宅配':
                            lst = sht.used_range.value
                            file = pd.DataFrame(lst[1:], columns=lst[0])
                        db = file
                        print(db)
                    except Exception as e:
                        print('xxxx查看失败：' + sht.name, str(Exception) + str(e))
                else:
                    print('----不用导入：' + sht.name)
            # 关闭excel文件
            wb.close()
        # 退出excel app
        app.quit()
if __name__ == '__main__':
    # 无运单号查询---泰国 （下午四点开始运行）
    m = MysqlControl()
    for team in ['sltg']:
        m.noWaybillNumber(team)

    # # 产品花费表200
    # match = {'JP': '日本'}
    # for team in match.keys():
    #     m.orderCost(team)









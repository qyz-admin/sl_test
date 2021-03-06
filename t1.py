import os
from datetime import datetime
from openpyxl import Workbook, load_workbook
from excelControl import ExcelControl
from mysqlControl import MysqlControl
from wlMysql import WlMysql
from wlExecl import WlExecl
# from orderQuery import OrderQuery
import datetime

start: datetime = datetime.datetime.now()
team = 'sltg'
match = {'slrb': r'D:\Users\Administrator\Desktop\需要用到的文件\日本签收表',
         'sltg': r'D:\Users\Administrator\Desktop\需要用到的文件\泰国签收表',
         'slgat': r'D:\Users\Administrator\Desktop\需要用到的文件\港台签收表',
         'slxmt': r'D:\Users\Administrator\Desktop\需要用到的文件\新马签收表'}
'''    msyql 语法:      show processlist;
备注：  港台 需整理的表：香港立邦>(明细再copy一份保存) ； 台湾龟山改派>(copy保存为xlsx格式);
说明：  日本 需整理的表：1、吉客印神龙直发签收表=密码：‘JKTSL’>(明细再copy保存；改派明细不需要);2、直发签收表>(明细再copy保存；3、状态更新需要copy保存);
'''
path = match[team]
dirs = os.listdir(path=path)
e = ExcelControl()
m = MysqlControl()
w = WlMysql()
we = WlExecl()
# qo = OrderQuery()
# 上传退货
e.readReturnOrder(team)
print('退货导入耗时：', datetime.datetime.now() - start)

# ---读取execl文件---
for dir in dirs:
    filePath = os.path.join(path, dir)
    print(filePath)
    if dir[:2] != '~$':
        wb_start = datetime.datetime.now()
        wb = load_workbook(filePath, data_only=True)
        wb.save(filePath)
        print('+++处理表格公式-耗时：', datetime.datetime.now() - wb_start)
        if dir[:6] == 'GIIKIN' or dir[:6] == 'Giikin':
            print('98')
            we.logisitis(filePath, team)
        else:
            print('02')
            e.readExcel(filePath, team)
        print('单表+++导入-耗时：', datetime.datetime.now() - wb_start)
print('导入耗时：', datetime.datetime.now() - start)

# TODO---数据库分段读取---
m.creatMyOrderSl(team)  # 最近五天的全部订单信息
print('------------更新部分：---------------------')
m.creatMyOrderSlTWO(team)   # 最近两个月的更新订单信息
print('处理耗时：', datetime.datetime.now() - start)
m.connectOrder(team)      # 最近两个月的订单信息导出
print('输出耗时：', datetime.datetime.now() - start)




# ---数据库分段读取---
# m.creatMyOrder(team)   # 备用获取最近两个月全部订单信息
# 输出签收率表、(备用)
# tem = '泰国'
# w.OrderQuan(team, tem)
# print('导出耗时：', datetime.datetime.now() - start)


'''
    IDE很多技巧:
    1,  `ctrl + alt + L`，格式化代码
    2,  双击`shift`搜索一切，不管是IDE功能、文件、方法、变量……都能搜索
    3,  `alt+enter`万能键
    4,  `shift+enter`向下换行
    5,  `shift+ctrl`向上换行
    6,  `ctrl+space` 万能提示键，PyCharm的会根据上下文提供补全
    7,  `ctrl+shift+f10`运行当前文件
    8,  `ctrl+w`扩展选取和`ctrl+shift+w`缩减选区, `ctrl+alt+shift+T`重构选区
    9,  `ctrl+q`查注释
'''
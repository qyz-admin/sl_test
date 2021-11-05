import os
from openpyxl import Workbook, load_workbook
from excelControl import ExcelControl
from mysqlControl import MysqlControl
from wlMysql import WlMysql
from wlExecl import WlExecl
from sso_updata import QueryTwo
import datetime

start: datetime = datetime.datetime.now()
team = 'gat'
match1 = {'gat_order_list': '港台'}
match = {'slgat': r'D:\Users\Administrator\Desktop\需要用到的文件\A港台签收表',
         'slgat_hfh': r'D:\Users\Administrator\Desktop\需要用到的文件\A港台签收表',
         'slgat_hs': r'D:\Users\Administrator\Desktop\需要用到的文件\A港台签收表',
         'slrb': r'D:\Users\Administrator\Desktop\需要用到的文件\A日本签收表',
         'slrb_jl': r'D:\Users\Administrator\Desktop\需要用到的文件\A日本签收表',
         'slrb_js': r'D:\Users\Administrator\Desktop\需要用到的文件\A日本签收表',
         'slrb_hs': r'D:\Users\Administrator\Desktop\需要用到的文件\A日本签收表',
         'slsc': r'D:\Users\Administrator\Desktop\需要用到的文件\品牌',
         'gat': r'D:\Users\Administrator\Desktop\需要用到的文件\A港台签收表',
         'sltg': r'D:\Users\Administrator\Desktop\需要用到的文件\A泰国签收表',
         'slxmt': r'D:\Users\Administrator\Desktop\需要用到的文件\A新马签收表',
         'slxmt_t': r'D:\Users\Administrator\Desktop\需要用到的文件\A新马签收表',
         'slxmt_hfh': r'D:\Users\Administrator\Desktop\需要用到的文件\A新马签收表'}
'''    msyql 语法:      show processlist（查看当前进程）;  
                        set global event_scheduler=0;（关闭定时器）;
备注：  港台 需整理的表：香港立邦>(明细再copy一份保存) ； 台湾龟山改派>(copy保存为xlsx格式);
说明：  日本 需整理的表：1、吉客印神龙直发签收表=密码：‘JKTSL’>(明细再copy保存；改派明细不需要);2、直发签收表>(明细再copy保存；3、状态更新需要copy保存);
'''
path = match[team]
dirs = os.listdir(path=path)
e = ExcelControl()
m = MysqlControl()
w = WlMysql()
we = WlExecl()
# sso = QueryTwo()

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
# m.creatMyOrderSl(team)  # 最近五天的全部订单信息

# print('------------更新部分：---------------------')
# if team in ('slsc', 'slrb', 'slrb_jl', 'slrb_js', 'slrb_hs'):
#     m.creatMyOrderSlTWO(team)   # 最近两个月的更新订单信息
#     print('处理耗时：', datetime.datetime.now() - start)
# elif team in ('gat'):
#     team = 'gat_order_list'     # 获取单号表
#     team2 = 'gat_order_list'    # 更新单号表
#     # yy = int((datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y'))  # 2、自动设置时间
#     # mm = int((datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%m'))
#     # begin = datetime.date(yy, mm, 1)
#     # print(begin)
#     # yy2 = int(datetime.datetime.now().strftime('%Y'))
#     # mm2 = int(datetime.datetime.now().strftime('%m'))
#     # dd2 = int(datetime.datetime.now().strftime('%d'))
#     # end = datetime.date(yy2, mm2, dd2)
#     # print(end)
#     begin = datetime.date(2021, 7, 30)       # 1、手动设置时间；若无法查询，切换代理和直连的网络
#     print(begin)
#     end = datetime.date(2021, 8, 2)
#     print(end)
#     print(datetime.datetime.now())
#     print('++++++正在获取 ' + match1[team] + ' 信息++++++')
#     for i in range((end - begin).days):  # 按天循环获取订单状态
#         day = begin + datetime.timedelta(days=i)
#         yesterday = str(day) + ' 23:59:59'
#         last_month = str(day)
#         print('正在更新 ' + match1[team] + last_month + ' 号订单信息…………')
#         searchType = '订单号'      # 运单号，订单号   查询切换
#         sso.orderInfo(searchType, team, team2, last_month)
#     print('更新耗时：', datetime.datetime.now() - start)
#     team = 'gat'
# print('------------导出部分：---------------------')
# m.connectOrder(team)  # 最近两个月的订单信息导出
# print('输出耗时：', datetime.datetime.now() - start)




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
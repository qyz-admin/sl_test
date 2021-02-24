import os
from excelControl import ExcelControl
from mysqlControl import MysqlControl
from wlMysql import WlMysql
from wlExecl import WlExecl
import datetime
start = datetime.datetime.now()
team = 'slrb'
match = {'slrb': r'D:\Users\Administrator\Desktop\查询\日本签收表',
        'sltg': r'D:\Users\Administrator\Desktop\物流表\泰国物流表',
        'slgat': r'D:\Users\Administrator\Desktop\签收表\港台签收表',
        'slxmt': r'D:\Users\Administrator\Desktop\签收表\新马签收表'}
'''
备注说明：1
港台 需整理的表：香港顺航>(出货明细再copy一份保存) ； 台湾龟山改派>(copy保存为xlsx格式);  香港易速配顺丰>(总明细copy保存为xlsx格式);
日本 需整理的表：吉客印神龙直发签收表>(明细再copy一份保存；   改派明细不需要);
'''
path = match[team]
dirs = os.listdir(path=path)
# e = ExcelControl()
# m = MysqlControl()
# w = WlMysql()
# we = WlExecl()
# ---读取execl文件---
for dir in dirs:
    filePath = os.path.join(path, dir)
    print(filePath)
    os.remove(filePath)
    # we.qianshoubiao(filePath, team)
    # we.wuliubiao(filePath, team)
import pandas as pd
import datetime
import re
import numpy as np

filePath = r'D:\Users\Administrator\Desktop\2020.08.05 神龙泰国签收表.xlsx'


# df = pd.DataFrame({'key1':['ab','bb','cb','db'],'key2':[None,'f', None,'h'],'key3':[1.0,"2",3,4]},index=['k','l','m','n',])
# df2 = pd.DataFrame({'key1':['a','B','c','d'],'key2':['e','f','g','H'],'key4':['i','j','K','L']},index = ['p','q','u','v'])
# df = pd.read_excel(filePath)
# start = datetime.datetime.now()
def getWaybillStatus(df):
    if pd.isnull(df['系统物流状态']):
        if df['物流状态'] not in ['已签收', '拒收', '未上线', '自发头程丢件', '已退货', '理赔', '在途']:
            return '空白'
        else:
            return df['物流状态']


df = pd.read_excel(filePath)
print(df)
df.drop(df[(df.运单编号.isna()) & (df.是否改派 == '改派')].index, inplace=True)
print(df)
df['系统物流状态'] = df.apply(lambda x: getWaybillStatus(x), axis=1)
table = pd.pivot_table(df, index=['年月', '团队', '是否改派', '物流方式'],
                       columns=['系统物流状态'],
                       values=['订单编号', '价格'],
                       aggfunc={'订单编号': len, '价格': np.sum},
                       fill_value=0)
print(table)

table['订单编号', '已完成'] = table['订单编号', '已签收'] + table['订单编号', '拒收']
table['价格', '已完成'] = table['价格', '已签收'] + table['价格', '拒收']
table.to_excel(r'D:\Users\Administrator\Desktop\率.xlsx')

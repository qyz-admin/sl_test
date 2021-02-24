import pandas as pd
import numpy as np
import pymssql
import xlwt
import pandas.io.formats.excel
import smtplib
def set_excel(df, set_dir):
    # 输出表格并且设置表格宽度等
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
    
    # 修改几个特殊单元格的内容
    cell_format = workbook1.add_format({'bold': True})
    worksheet1.write('A1', 'Datasource', cell_format)
    cell_format = workbook1.add_format({'bold': True})
    worksheet1.write('B1', 'Company', cell_format)
    # 标题加粗
    format1 = workbook1.add_format({'bold': 1})
    format2 = workbook1.add_format({'left':6})
    worksheet1.conditional_format('B1:N2', {'type':     'cell',
                                    'criteria': '>',
                                    'value':    15,
                                    'format':   format1
                                           })
    # 隐藏线条和第三行
    worksheet1.hide_gridlines(option=2)
    worksheet1.set_row(2, None, None, {'hidden': True})
    # 输出表格
    writer.save()
    writer.close() 
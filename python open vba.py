import xlwings as xl
import time
app = xl.App(visible=False, add_book=False)
app.display_alerts = False
wb = app.books.open('D:/Users/Administrator/Desktop/新版-格式转换(工具表).xlsm')

wb1 = app.books.open('D:/Users/Administrator/Desktop/输出文件/2020.12.28 日本本月产品花费表.xlsx')
# Python调用VBA 的第一种方法
# wb1.activate()
# wb1 = app.books.active
# sht1 = wb1.sheets['直发成本']
# sht1.activate()

wb.macro('花费运行')()
# wb.save()
wb1.save()

wb.close()
wb1.close()
# Python调用VBA 的第二种方法
# macro_run = wb.macro('A')
# macro_run()
 
# time.sleep(2)
app.quit()
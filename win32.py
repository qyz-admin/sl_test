# from win32com.client import Dispatch



# filePath = r'D:\Users\Administrator\Desktop\\输出文件\台湾森鸿头程直111112222发.xlsx'
# xlApp = Dispatch("Excel.Application")
# xlApp.Visible = False
# xlBook = xlApp.Workbooks.Open(filePath)
# xlBook.Save()
# xlBook.Close()


import xlwings
filePath = r'D:\Users\Administrator\Desktop\\输出文件\台湾森鸿头程直111112222发.xlsx'
app = xlwings.App(visible=False, add_book=False)
app.display_alerts = False
wb = app.books.open(filePath, update_links=False, read_only=True, data_only=True)
# wb.Save()
wb.close()

app.quit()

def deleteExcel(self, filePath):
	writer = pd.ExcelWriter(filePath, engine='openpyxl')         # 初始化写入对象
	wb = load_workbook(filePath,data_only=True)
	wb.save(filePath)
import sys
# import pandas as pd
from pandas import DataFrame
from pandas import concat
# sys.path.append('D:\Program Files\Python\Lib\site-packages')
import xlwings as xw
import PySimpleGUI as sg


# 设置相关信息
class option():
    def __init__(self):
        self.pos = 'A2'
        self.label = ['姓名', 'ID']
        self.quality = '数量'


# 分类汇总,并将结果保存在相邻的新建工作表中
def gather(rng_start: str, total: str, params: list):
    '''
    分类汇总,并将结果保存在相邻的新建工作表中\n
    rng_start:起始单元格,应当在"列标签+数据"所在区域的左上角第一个单元格,例如"A1"\n
    total:需要汇总的列的标签,例如"数量"\n
    params:列标签构成的列表,例如["姓名","ID"],即代表,只有姓名和ID都相同,才会进行汇总\n
    '''
    sht = xw.sheets.active  # 获取当前已经打开的sheet
    wb = xw.Book.activate  # 获取当前已打开的workbook

    df = sht.range(rng_start, sht.used_range.shape)
    # 第一行作为标签，后面作为dataframe
    df = DataFrame(df.value[1:], columns=df.value[0])

    # 数据清理(能作为标签的只能是文本，能用来汇总的只能是数字)
    try:
        # 如果标签或统计列中有Nan值，会无法统计
        df_lables = df[params].fillna(' ').astype("str")
        df_results = df[total].fillna(0).astype("float")
        df = concat([df_lables, df_results], axis=1)  # 横向合并标签和汇总值
    except KeyError:
        print('出现错误,请检查关键字是否和表格一致\n已中止\n')
        return 0

    # 分类汇总,注意,如果不加as_index=False，那么params会变成标签列，无法进行切片等操作
    result = df.groupby(by=params, as_index=False).sum()
    # result=result.apply(lambda x: x) # 用于as_index=True时，将groupby的结果的标签转为普通列表

    # 新建sheet表
    try:
        # 新建一个sheet,名字为汇总数据源表的名字+'汇总'
        total_sheet = xw.sheets.add(sht.name + '汇总', after=sht.name)
    except ValueError:
        total_sheet = xw.sheets.add(after=sht.name)

    # 写入数据
    total_sheet.range(rng_start).value = result

    total_sheet.range(rng_start).options(transpose=True).value = [
                                                                     '序号'] + [str(i) for i in range(1, result.shape[
        0] + 1)]  # 写入序号列(写入dateframe时,第一列原本为index,覆盖为序号列)
    # total_sheet.range('A:A').api.delete  #调用API删除A列,备忘

    return '在"{}"表后生成了汇总表"{}"\n已完成'.format(sht.name, total_sheet.name)


# 读取UI的参数
def read_GUI(values: dict):
    print(values)
    opt.pos = values['pos']
    opt.label = values['label']
    opt.quality = values['quality']
    # 将UI中的的参数进行转化，保证同时支持中英文的逗号
    if ',' in opt.label:
        opt.label = opt.label.split(',')
    else:
        opt.label = opt.label.split('，')


# 界面程序
def this_GUI():
    sg.change_look_and_feel('DarkAmber')  # 主题设置
    # 设置界面
    layout = [[sg.Text('分类汇总', size=(25, 1), justification='center')],
              [sg.Text('标签起点', size=(25, 1)), sg.InputText(default_text='A2', size=(10, 1), key='pos')],
              [sg.Text('用于分类的标签，多个标签以逗号隔开', size=(25, 1)),
               sg.InputText(default_text='姓名,ID', size=(15, 1), key='label')],
              [sg.Text('想要汇总的量', size=(25, 1)), sg.InputText(default_text='数量', size=(11, 1), key='quality')],
              [sg.Button('点击进行分类汇总', size=(25, 1), pad=(35, 1))]
              ]

    # 创建一个窗口
    window = sg.Window('分类汇总工具', layout)
    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read()
        if event in (None, 'Cancel'):  # 当点击关闭或取消按钮时
            break
        read_GUI(values)  # 获取GUI界面上的参数

        # 开始运行
        # app=xw.App.activate
        result = gather(opt.pos, opt.quality, opt.label)
        sg.PopupOK(result)  # 完成后出现ok按钮
    window.close()


if __name__ == '__main__':
    # 先打开需要汇总的工作表，然后才能运行
    opt = option()  # 建立设置信息
    this_GUI()  # 运行GUI界面
    print('处理耗时：')
    this_GUI()  # 运行GUI界面

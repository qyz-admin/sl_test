# -*- coding: utf-8 -*-
import requests
import time
import json
import xlwings as xl
class Yixiajia():
    def __init__(self):
        登录网址 = r'http://gwms.giikin.com/admin/login/index.html'
        登录数据 = {
            'uname': 'qiyuanzhang@jikeyin.com',
            'pwd': 'qiyuanzhang123.0',
            'remember': '1'
        }
        self.会话 = requests.session()
        self.会话.get(登录网址)
        re = self.会话.post(登录网址, data=登录数据)
        print(re.text)
        self.lst = [['订单编号', '下架时间', '仓库', '原运单号', '下架类型']]
    def 获取已下架(self, 已下架表保存路径):
        '''
        :param 已下架表保存路径: 已下架表保存路径（不用包含文件名）
        :return: 已下架的所有文件计数
        '''
        count1 = 0
        已下架网址 = r'http://gwms.giikin.com/admin/order/tribgoods'
        已下架数据 = {
        'Referer': r'http://gwms.giikin.com/admin/order/tribgoods',
        'columns[0][data]': "",
        'columns[0][name]': "",
        'columns[0][searchable]': 'true',
        'columns[0][orderable]': 'false',
        'columns[0][search][value]': "",
        'columns[0][search][regex]': 'false',
        'columns[1][data]': 'order_number',
        'columns[1][name]': "",
        'columns[1][searchable]': 'true',
        'columns[1][orderable]': 'true',
        'columns[1][search][value]': "",
        'columns[1][search][regex]': 'false',
        'columns[2][data]': 'stock_type',
        'columns[2][name]': "",
        'columns[2][searchable]': 'true',
        'columns[2][orderable]': 'true',
        'columns[2][search][value]': "",
        'columns[2][search][regex]': 'false',
        'columns[3][data]': 'below_type',
        'columns[3][name]': "",
        'columns[3][searchable]': 'true',
        'columns[3][orderable]': 'true',
        'columns[3][search][value]': "",
        'columns[3][search][regex]': 'false',
        'columns[4][data]': 'goods_name',
        'columns[4][name]': "",
        'columns[4][searchable]': 'true',
        'columns[4][orderable]': 'false',
        'columns[4][search][value]': "",
        'columns[4][search][regex]': 'false',
        'columns[5][data]': 'goods_id',
        'columns[5][name]': "",
        'columns[5][searchable]': 'true',
        'columns[5][orderable]': 'true',
        'columns[5][search][value]': "",
        'columns[5][search][regex]': 'false',
        'columns[6][data]': 'op_id',
        'columns[6][name]': "",
        'columns[6][searchable]': 'true',
        'columns[6][orderable]': 'false',
        'columns[6][search][value]': "",
        'columns[6][search][regex]': 'false',
        'columns[7][data]': 'op_id_trans',
        'columns[7][name]': "",
        'columns[7][searchable]': 'true',
        'columns[7][orderable]': 'false',
        'columns[7][search][value]': "",
        'columns[7][search][regex]': 'false',
        'columns[8][data]': 'waybill_number',
        'columns[8][name]': "",
        'columns[8][searchable]': 'true',
        'columns[8][orderable]': 'true',
        'columns[8][search][value]': "",
        'columns[8][search][regex]': 'false',
        'columns[9][data]': 'goods_name',
        'columns[9][name]': "",
        'columns[9][searchable]': 'true',
        'columns[9][orderable]': 'false',
        'columns[9][search][value]': "",
        'columns[9][search][regex]': 'false',
        'columns[10][data]': 'name',
        'columns[10][name]': "",
        'columns[10][searchable]': 'true',
        'columns[10][orderable]': 'false',
        'columns[10][search][value]': "",
        'columns[10][search][regex]': 'false',
        'columns[11][data]': 'currency_id',
        'columns[11][name]': "",
        'columns[11][searchable]': 'true',
        'columns[11][orderable]': 'true',
        'columns[11][search][value]': "",
        'columns[11][search][regex]': 'false',
        'columns[12][data]': 'area_id',
        'columns[12][name]': "",
        'columns[12][searchable]': 'true',
        'columns[12][orderable]': 'true',
        'columns[12][search][value]': "",
        'columns[12][search][regex]': 'false',
        'columns[13][data]': 'sku',
        'columns[13][name]': "",
        'columns[13][searchable]': 'true',
        'columns[13][orderable]': 'true',
        'columns[13][search][value]': "",
        'columns[13][search][regex]': 'false',
        'columns[14][data]': 'userId',
        'columns[14][name]': "",
        'columns[14][searchable]': 'true',
        'columns[14][orderable]': 'true',
        'columns[14][search][value]': "",
        'columns[14][search][regex]': 'false',
        'columns[15][data]': 'ship_firstname',
        'columns[15][name]': "",
        'columns[15][searchable]': 'true',
        'columns[15][orderable]': 'true',
        'columns[15][search][value]': "",
        'columns[15][search][regex]': 'false',
        'columns[16][data]': 'ship_phone',
        'columns[16][name]': "",
        'columns[16][searchable]': 'true',
        'columns[16][orderable]': 'true',
        'columns[16][search][value]': "",
        'columns[16][search][regex]': 'false',
        'columns[17][data]': 'plcode',
        'columns[17][name]': "",
        'columns[17][searchable]': 'true',
        'columns[17][orderable]': 'true',
        'columns[17][search][value]': "",
        'columns[17][search][regex]': 'false',
        'columns[18][data]': 'amount',
        'columns[18][name]': "",
        'columns[18][searchable]': 'true',
        'columns[18][orderable]': 'true',
        'columns[18][search][value]': "",
        'columns[18][search][regex]': 'false',
        'columns[19][data]': 'quantity',
        'columns[19][name]': "",
        'columns[19][searchable]': 'true',
        'columns[19][orderable]': 'true',
        'columns[19][search][value]': "",
        'columns[19][search][regex]': 'false',
        'columns[20][data]': 'intime',
        'columns[20][name]': "",
        'columns[20][searchable]': 'true',
        'columns[20][orderable]': 'true',
        'columns[20][search][value]': "",
        'columns[20][search][regex]': 'false',
        'columns[21][data]': 'notes',
        'columns[21][name]': "",
        'columns[21][searchable]': 'true',
        'columns[21][orderable]': 'false',
        'columns[21][search][value]': "",
        'columns[21][search][regex]': 'false',
        'columns[22][data]': 'whid',
        'columns[22][name]': "",
        'columns[22][searchable]': 'true',
        'columns[22][orderable]': 'true',
        'columns[22][search][value]': "",
        'columns[22][search][regex]': 'false',
        'columns[23][data]': 'old_billno',
        'columns[23][name]': "",
        'columns[23][searchable]': 'true',
        'columns[23][orderable]': 'true',
        'columns[23][search][value]': "",
        'columns[23][search][regex]': 'false',
        'columns[24][data]': 'ship_zip',
        'columns[24][name]': "",
        'columns[24][searchable]': 'true',
        'columns[24][orderable]': 'true',
        'columns[24][search][value]': "",
        'columns[24][search][regex]': 'false',
        'columns[25][data]': 'ship_state',
        'columns[25][name]': "",
        'columns[25][searchable]': 'true',
        'columns[25][orderable]': 'true',
        'columns[25][search][value]': "",
        'columns[25][search][regex]': 'false',
        'columns[26][data]': 'ship_city',
        'columns[26][name]': "",
        'columns[26][searchable]': 'true',
        'columns[26][orderable]': 'true',
        'columns[26][search][value]': "",
        'columns[26][search][regex]': 'false',
        'columns[27][data]': 'logistics_name',
        'columns[27][name]': "",
        'columns[27][searchable]': 'true',
        'columns[27][orderable]': 'true',
        'columns[27][search][value]': "",
        'columns[27][search][regex]': 'false',
        'columns[28][data]': 'oldBillnoLogistics',
        'columns[28][name]': "",
        'columns[28][searchable]': 'true',
        'columns[28][orderable]': 'true',
        'columns[28][search][value]': "",
        'columns[28][search][regex]': 'false',
        'columns[29][data]': 'category_name',
        'columns[29][name]': "",
        'columns[29][searchable]': 'true',
        'columns[29][orderable]': 'true',
        'columns[29][search][value]': "",
        'columns[29][search][regex]': 'false',
        'columns[30][data]': 'ship_address',
        'columns[30][name]': "",
        'columns[30][searchable]': 'true',
        'columns[30][orderable]': 'true',
        'columns[30][search][value]': "",
        'columns[30][search][regex]': 'false',
        'columns[31][data]': 'tqno',
        'columns[31][name]': "",
        'columns[31][searchable]': 'true',
        'columns[31][orderable]': 'true',
        'columns[31][search][value]': "",
        'columns[31][search][regex]': 'false',
        'columns[32][data]': 'ship_email',
        'columns[32][name]': "",
        'columns[32][searchable]': 'true',
        'columns[32][orderable]': 'true',
        'columns[32][search][value]': "",
        'columns[32][search][regex]': 'false',
        'columns[33][data]': '33',
        'columns[33][name]': "",
        'columns[33][searchable]': 'true',
        'columns[33][orderable]': 'true',
        'columns[33][search][value]': "",
        'columns[33][search][regex]': 'false',
        'order[0][column]': '0',
        'order[0][dir]': 'asc',
        'start': '0',
        'length': '2000',
        'search[value]': "",
        'search[regex]': 'false',
        'extra_search': '3',
        'status': '4',
        'flag': '0',
        'whid': '13',
        'startdate': time.strftime("%y-%m-%d", time.localtime()) + ' 00:00:00',
        'enddate': time.strftime("%y-%m-%d", time.localtime()) + ' 23:59:59',
        'selectstr': "b.stock_type='2' and 1=1"
        }
        # 'selectstr': "b.stock_type='2' and 1=1" b.stock_type  1 是正常下架， 2 是组合库存下架
        # whid 仓库  时丰：13，博佳图：35，百达通：36，跨境一号：37，泰国自建仓：58，超时代：67
        所有仓库 = ['13', '35', '36', '67']
        for 仓库 in 所有仓库:
            已下架数据['whid'] = 仓库
            for a in range(1, 3):
                已下架数据['selectstr'] = "b.stock_type='"+ str(a) + "' and 1=1"
                req = self.会话.post(已下架网址, 已下架数据)
                s = json.loads(req.text[1:])
                print(已下架数据['whid'], "----", 已下架数据['selectstr'], "----", s['recordsTotal'])
                if int(s['recordsTotal']) > 0:
                    count1 += s['recordsTotal']
                    for l in s['data']:
                        self.lst.append([l["order_number"], l["intime"], l["whid"], "'" + l["old_billno"], l["stock_type"]])
        app = xl.App(visible=False, add_book=False)
        wb = app.books.add()
        wb.sheets['sheet1'].range('A1').value = self.lst
        wb.save(已下架表保存路径 + '01_已下架总表.xlsx')
        wb.close()
        app.quit()
        return count1
    def 匹配待审核(self, 待审核表完整路径):
        '''
        :param 待审核表完整路径:
        :return: [正常下架计数，组合库存下架计数]
        '''
        d = {}
        count1 = 0
        count2 = 0
        if len(self.lst) == 1:
            pass
        for i in range(1, len(self.lst)):
            d[self.lst[i][0]] = [self.lst[i][1], self.lst[i][2], self.lst[i][3], self.lst[i][4]]
        app = xl.App(visible=False, add_book=False)
        xlbook = app.books.open(待审核表完整路径)
        sht = xlbook.sheets[0]
        l = sht.used_range.value
        for i in range(1, len(l)):
            if d.get(l[i][0]):
                if d.get(l[i][0])[3] == '正常下架':
                    sht.cells(i + 1, 35).value = '改派备货'
                    count1 += 1
                else:
                    sht.cells(i + 1, 35).value = '改派商品'
                    count2 += 1
                sht.cells(i + 1, 37).value = d.get(l[i][0])[1].split('-')[1]
                sht.cells(i + 1, 38).value = '陆运'
        xlbook.save()
        xlbook.close()
        app.quit()
        return [count1, count2]
if __name__ == '__main__':
    s = Yixiajia()
    print(s.获取已下架('G:\\输出文件\\签收表\\'))
    print(s.匹配待审核('G:\\输出文件\\待审核.xlsx'))
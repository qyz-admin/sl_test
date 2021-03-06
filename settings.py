class Settings():
    def __init__(self):
        self.excelPath = r'D:\Users\Administrator\Desktop\直发表'
        self.mysql1 = {'host': 'localhost',      #数据库地址
                      'user': 'root',           #数据库账户
                      'port': '3306',
                      'password': '123456',     #数据库密码   654321
                      'datebase': 'logistics_status',   #数据库名称
                      'charset': 'utf8'         #数据库编码
                       }
        self.mysql20 = {'host': 'tidb.giikin.com',  # 数据库地址
                       'user': 'shenlongkf',  # 数据库账户
                       'port': '4000',
                       'password': 'SIK87&67asd',  # 数据库密码
                       'datebase': 'gdqs_shenlong',  # 数据库名称
                       'charset': 'utf8'  # 数据库编码
                       }
        self.mysql2 = {'host': 'tidb2.giikin.com',  # 数据库地址
                       'user': 'shenlong_kf',  # 数据库账户
                       'port': '39999',
                       'password': 'SIK87&67asd',  # 数据库密码
                       'datebase': 'gdqs_shenlong',  # 数据库名称
                       'charset': 'utf8'  # 数据库编码
                       }
        self.mysql3 = {'host': 'localhost',      #数据库地址
                      'user': 'root',           #数据库账户
                      'port': '3306',
                      'password': '123456',     #数据库密码
                      'datebase': '订单数据',   #数据库名称
                      'charset': 'utf8'         #数据库编码
                       }
        self.logistics = {
            '时丰': {'apiToken': '6XgVJqpdy3lttVh7DniJtXdLdxzDhHk12RgMurLqM9aCFeimGrNB08cs0233',
                   'apiUrl': r'https://timesoms.com/api/orders/track/'},
            '博佳图': {'apiAccount': 'cc4ea61156676d5e51f85fd1c0588c56',
                    'apiPassword': 'cc4ea61156676d5e51f85fd1c0588c5630dfa67fb8269572f64ab3e1e2dae0bb',
                    'apiUrl': r'http://120.79.190.37/default/svc/wsdl'},
            '超时代': {'apiUrl': r'http://134.175.15.128:8082/selectTrack.htm?documentCode='}}
        self.system = {
            '后台': {'username': 'louweibin@giikin.com',
                   'password': 'wo.1683485'}}
        self.email = {'smtp': 'smtp.163.com',
                      'email': 'qyz1404039293@163.com',      # 密码： qyz04163510
                      'password': 'VTOYQJKMQOCSFSGE'}
        #  https://blog.csdn.net/weixin_36931308/article/details/103767758 （危机处理需要）
        #  LECOGDYYBJUJJBST
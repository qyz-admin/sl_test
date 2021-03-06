import smtplib
import os
import datetime
#发送字符串的邮件
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
#处理多种形态的邮件主体我们需要 MIMEMultipart 类
from email.mime.multipart import MIMEMultipart
#处理图片需要 MIMEImage 类
from email.mime.image import MIMEImage
from settings import Settings
class EmailControl(Settings):
    def __init__(self):
        Settings.__init__(self)
    def send(self, title, filePaths, toAdd):
        toAdd = '{},{}'.format(toAdd, self.email['email'])
        #toAdd = '{}'.format(toAdd)
        m = MIMEMultipart()
        content = '附件内容如下：'
        for filePath in filePaths:
            # print(filePath)
            try:
                excelApart = MIMEApplication(open(filePath, 'rb').read())
                excelApart.add_header('Content-Disposition', 'attachment', filename=os.path.basename(filePath))
                content = content + '\n' + os.path.basename(filePath)
                m.attach(excelApart)
            except Exception as email:
                print('xxxx 邮件建立失败：' + filePath, str(Exception) + str(e))
        m['Subject'] = datetime.datetime.now().strftime('%Y-%m-%d') + ' ' + title
        textApart = MIMEText(content)
        m.attach(textApart)
        m['to'] = toAdd
        # m['Cc'] = 抄送  qyz1404039293@163.com  LECOGDYYBJUJJBST
        m['from'] = self.email['email']

        server = smtplib.SMTP(self.email['smtp'])    # smtp服务器
        print('++++正在连接邮箱服务器中++++')
        server.login(self.email['email'], self.email['password'])  # 登陆需要认证的SMTP服务器，参数为用户名与密码
        server.sendmail(self.email['email'], toAdd.split(','), m.as_string())
        print('邮件发送成功…………')
        server.quit()
    def sendT(self, title, filePaths, toAdd):
        toAdd = '{},{}'.format(toAdd, self.email['email'])
        #toAdd = '{}'.format(toAdd)
        m = MIMEMultipart()
        content = '附件内容如下：'
        for filePath in filePaths:
            # print(filePath)
            try:
                excelApart = MIMEApplication(open(filePath, 'rb').read())
                excelApart.add_header('Content-Disposition', 'attachment', filename=os.path.basename(filePath))
                content = content + '\n' + os.path.basename(filePath)
                m.attach(excelApart)
            except Exception as email:
                print('xxxx 邮件建立失败：' + filePath, str(Exception) + str(e))
        m['Subject'] = (datetime.datetime.now().replace(month=9, day=30)).strftime('%Y-%m-%d') + ' ' + title
        textApart = MIMEText(content)
        m.attach(textApart)
        m['to'] = toAdd
        # m['Cc'] = 抄送  qyz1404039293@163.com  LECOGDYYBJUJJBST
        m['from'] = self.email['email']

        server = smtplib.SMTP(self.email['smtp'])    # smtp服务器
        print('++++正在连接邮箱服务器中++++')
        server.login(self.email['email'], self.email['password'])  # 登陆需要认证的SMTP服务器，参数为用户名与密码
        server.sendmail(self.email['email'], toAdd.split(','), m.as_string())
        print('邮件发送成功…………')
        server.quit()

if __name__ == '__main__':
    today = datetime.date.today().strftime('%Y.%m.%d')
    team = 'sltg'
    match = {'slgat': '港台',
             'sltg': '泰国',
             'slxmt': '新马',
             'slzb': '直播团队',
             'slyn': '越南',
             'slrb': '日本'}
    emailAdd = {'slgat': 'giikinliujun@163.com',
                'sltg': '1845389861@qq.com',
                'slxmt': 'zhangjing@giikin.com',
                'slzb': '直播团队',
                'slyn': '越南',
                'slrb': 'sunyaru@giikin.com'}
    filePath = ['D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}签收表.xlsx'.format(today, match[team])]
    e = EmailControl()
    e.send('{} 神龙{}签收表.xlsx'.format(today, match[team]), filePath, emailAdd[team])
    #  for today in [datetime.date.today().strftime('%Y.%m.%d'), (datetime.datetime.now().replace(month=8, day=31)).strftime('%Y-%m-%d')]:
    #     filePath = ['D:\\Users\\Administrator\\Desktop\\输出文件\\{} 神龙{}签收表.xlsx'.format(today, match[team])]
    #     e = EmailControl()
    #     e.send('{} 神龙{}签收表.xlsx'.format(today, match[team]), filePath, emailAdd[team])
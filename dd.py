#coding:utf-8
import json
import urllib.request

#1、构建url
url = "https://oapi.dingtalk.com/robot/send?access_token=9cf40268bec8f6d53328982b2b6a22d6196dc197bc1a1d6a95e9623ed5a26893"   #url为机器人的webhook

#2、构建一下请求头部
header = {
    "Content-Type": "application/json",
    "Charset": "UTF-8"
}
#3、构建请求数据
data = {
    "msgtype": "text",
    "text": {
        "content": "哈哈，找到你们了！！！"
    },
    "at": {
         # "isAtAll": True     #@全体成员（在此可设置@特定某人）
		# "atMobiles": reminders,
		# "isAtAll": False,    # 不@所有人，如果要@所有人写True并且将上面atMobiles注释掉
		# "atMobiles":18237192897
		# "atMobiles": ["18237192897"]
		"atMobiles": ["13683822640"]
		# "atMobiles": ["18237168998"]
		# "isAtAll": "false"
    }
}

#4、对请求的数据进行json封装
sendData = json.dumps(data)#将字典类型数据转化为json格式
sendData = sendData.encode("utf-8") # python3的Request要求data为byte类型

#5、发送请求
request = urllib.request.Request(url=url, data=sendData, headers=header)
print(request.json()["access_token"])
#6、将请求发回的数据构建成为文件格式

opener = urllib.request.urlopen(request)
#7、打印返回的结果
print(opener.read())

# r = requests.post(url, data=json.dumps(data), headers=headers)
# return r.text
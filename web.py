import requests
import json
r = requests.get("http://gimp.giikin.com/service?service=gorder.customer&action=getProductList&page=1&pageSize=10&productId=508746&productName=&status=&source=&isSensitive=&isGift=&isDistribution=&chooserId=&buyerId=&_token=7dd7c0085722cf49493c5ab2ecbc6234")
rq = r.json()
print(rq)
print(55)
# re = json.loads(rq)
# print(rq['data']['list']['id'])
# print(r['data']['list']['id'])
# print(r['data']['list']['name'])
# print(r['data']['list']['categorys'])
# print(r['data']['list']['status'])
# print(r['data']['list']['price'])
# print(r['data']['list']['createTime'])

for result in rq['data']['list']:
    print(result)



# url：接口地址
# url = "https://www.baidu.com/"
# 请求的数据：以字典形式{key:value}
# data = {"name":"zhangsan","pwd":"a123456"}

# 发送get请求
# res = requests.get(url,data) # 发送带有请求参数的GET请求
# res = requests.get(url)
# 输出响应数据
# print(res) # 输出响应数据中最后的HTTP状态码
# print(res.text) # 输出字符串格式
# print(res.json()) # 输出json格式


# import json
# import requests
#
# url = r'http://gimp.giikin.com/service?service=gorder.customer&action=getOrderList'
# data = {'productName': None,
#         'status': None,
#         'source': None,
#         'isSensitive': None,
#         'isGift': None,
#         'isDistribution': None,
#         'chooserId': None,
#         'buyerId': None,
#         'page': 1,
#         'pageSize': 10,
#         'productId': '508746',
#         '_user': 1343,
#         '_token': '7dd7c0085722cf49493c5ab2ecbc6234'}
# response = requests.get(url, params=data)
# res = response.json()
#
# print(res)




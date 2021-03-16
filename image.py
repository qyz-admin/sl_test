"""
最近在菜鸟教程上面发现了适合程序员用的壁纸
小学生才做选择呢，我全要
制作一个爬虫小程序来完成这个贪婪的想法
同时也分享给大家
"""
import requests
from lxml import etree
import os

def get_message_page():
    global headers
    headers ={
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
    }
    url = 'https://www.runoob.com/w3cnote/17-wallpaper-for-programmer.html'
    response = requests.get(url = url ,headers = headers )
    try:
        if response.status_code == 200:
            print('请求成功!!')
            response.encoding = response.apparent_encoding
            return response.text
    except requests.ConnectionError:
        print('连接失败!!')
        return None

def get_img_page(html):
    tree = etree.HTML(html)
    #title_list = tree.xpath('/html/body/div[3]/div/div[1]/div/div[2]/div/h3/text()')
    img_list = tree.xpath('/html/body/div[3]/div/div[1]/div/div[2]/div/p/a/@href')
    print(img_list)
    image_list = []
    for img in img_list:
        image = 'https:' + img
        image_list.append(image)
    print(image_list)
    if not os.path.exists('./image_data'):
        os.mkdir('./image_data')

    for number,jlist in enumerate(image_list):
        response = requests.get(url = jlist,headers= headers)
        img_path = './image_data/' + str(number) + '.jpg'
        with open(img_path,'wb') as f:
            f.write(response.content)
        print(jlist,'保存成功！！')


def main():
    html = get_message_page()
    get_img_page(html)

if __name__ == '__main__':
    main()

import json
import os
import qrcode
import requests
from concurrent.futures import ThreadPoolExecutor
import time
import cv2 as cv
class Login:
    def __init__(self):
        self.oauthkey = ''
        self.qrcodeUrl = ''
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'})

    def _requests(self, method, url, decode_level=2, retry=10, timeout=15, **kwargs):
        if method in ["get", "post"]:
            for _ in range(retry + 1):
                try:
                    response = getattr(self.session, method)(url, timeout=timeout, **kwargs)
                    return response.json() if decode_level == 2 else response.content if decode_level == 1 else response.content
                except:
                    pass
        return None
    def getQRCode(self):
        req = self._requests('get', "https://passport.bilibili.com/qrcode/getLoginUrl")
        if req and req.get('code') == 0:
            self.oauthkey = req['data']['oauthKey']
            self.qrcodeUrl = req['data']['url']
            print (req['data']['oauthKey'])
            print (req['data']['url'])
            return True
        return False
    @staticmethod
    def showQRCode(url):
        try:
            cv.destroyAllWindows()
        except:
            pass
        qrCode = qrcode.QRCode()
        qrCode.add_data(url)
        qrCode = qrCode.make_image()
        qrCode.save("qrCode.png")
        img = cv.imread("qrCode.png", 1)
        cv.imshow("Login", img)
        cv.waitKey()

    def login(self):
        pool = ThreadPoolExecutor(max_workers=2)
        if self.getQRCode():
            pool.submit(self.showQRCode, self.qrcodeUrl)
            while True:
                time.sleep(1)
                data = {
                    'oauthkey': self.oauthkey,
                    # 'gourl': "https://passport.bilibili.com/account/security"
                    'gourl': self.qrcodeUrl
                }
                req = self._requests('post', 'https://passport.bilibili.com/qrcode/getLoginInfo', data=data)
                print(req)
                print(22)
                if req['data'] == -4:  # 未扫描
                    pass
                elif req['data'] == -2:    # 过期
                    self.getQRCode()
                    pool.submit(self.showQRCode, self.qrcodeUrl)
                elif req['data'] == -1:    # 过期
                    self.getQRCode()
                    pool.submit(self.showQRCode, self.qrcodeUrl)
                elif req['data'] == -5:    # 扫描，等待
                    pass
                else:
                    break
            cookiesRaw = req['data']['url']
            cookiesRaw = cookiesRaw.split('?')[1]
            cookiesRaw = cookiesRaw.split('&')
            # cookiesRaw = (req['data']['url']).split('?')[1].split('&')
            cookies = {}
            for cookie in cookiesRaw:
                key, value = cookie.spilt('=')
                if key != 'gourl' and key != 'Expires':
                    cookies[key] = value
            print(json.dumps(cookies))
            os._exit(0)

if __name__ == '__main__':
    a = Login()
    a.login()
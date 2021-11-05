import time
import datetime
import sys

print (datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))

now_month = '2021.09.26'
# begin = datetime.date(now_month)
print (now_month.replace('.','-'))

print (99089)

Time_day = []
for i in range(1, datetime.datetime.now().month +1):  # 获取当年当前的月份时间
	try:
		daytime = (datetime.datetime.now().replace(month=i)).strftime('%Y-%m') + ((datetime.datetime.now()).strftime('-%d'))
		print(daytime)
		Time_day.append(daytime)
	except Exception as e:
		Time_day.append(daytime)
		print('xxxx时间配置出错,已手动调整：' + str(i) + '月份', str(Exception) + str(e))
		Time_day.append(str(int(datetime.datetime.now().year)) + '-' + str(i) + (datetime.datetime.now().strftime('-%d')))
for i in range(datetime.datetime.now().month + 1, 13):  # 获取往年当前的月份时间
	try:
		daytime = str(int(datetime.datetime.now().year) - 1) + (datetime.datetime.now().replace(month=i)).strftime('-%m') + (
                              (datetime.datetime.now()).strftime('-%d'))
		Time_day.append(daytime)
	except Exception as e:
		print('xxxx时间配置出错失败00：' + str(i) + '月份', str(Exception) + str(e))
		Time_day.append(str(int(datetime.datetime.now().year) - 1) + '-' + str(i) + (datetime.datetime.now().strftime('-%d')))

print(99)
print(datetime.datetime.now().strftime('%Y-%m-%d'))
print(datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1))

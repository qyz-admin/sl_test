import datetime

def main():
	# begin = datetime.date(2014,6,1)
	# end = datetime.date(2014,7,7)
	# for i in range((end - begin).days):
	# 	day = begin + datetime.timedelta(days=i)
	# 	print(str(day))


	# yesterday = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%d') + '-02 23:59:59'
	# print(yesterday)
	# last_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-02'
	# print(last_month)
	yy = int(datetime.datetime.now().strftime('%Y'))
	mm = int(datetime.datetime.now().strftime('%m'))
	dd = int(datetime.datetime.now().strftime('%d'))
	dd2 = int(datetime.datetime.now().strftime('%d'))
	begin = datetime.date(yy,mm,dd)
	# print(begin)
	end = datetime.date(yy,mm,dd2)
	# print(end)
	for i in range((end - begin).days):
		day = begin + datetime.timedelta(days=i+1)
		# print(str(day))
		print(str(day))

if __name__ == '__main__':
	main()

	import datetime
	# try:
	# 	daytime = (datetime.datetime.now().strftime('%Y-%m')) + (datetime.datetime.now().strftime('-%d'))
	# 	print(daytime)

	# 	daytime = (datetime.datetime.now().replace(month=11).strftime('%Y-%m')) + (datetime.datetime.now().strftime('-%d'))
	# 	print(daytime)

	# 	daytime = (datetime.datetime.now().replace(month=10).strftime('%Y-%m')) + (datetime.datetime.now().strftime('-%d'))
	# 	print(daytime)

	# 	daytime = (datetime.datetime.now().replace(month=9).strftime('%Y-%m')) + (datetime.datetime.now().strftime('-%d'))
	# 	print(daytime)

	# 	daytime = (datetime.datetime.now().replace(month=8).strftime('%Y-%m')) + (datetime.datetime.now().strftime('-%d'))
	# 	print(daytime)
	# except Exception as e:
	# 	print('xxxx时间配置出错失败：', str(Exception) + str(e))
	print("999999")
	listT = []
	for i in range(1,13):
		try:
			daytime = (datetime.datetime.now().replace(month=i)).strftime('%Y-%m') + (datetime.datetime.now().strftime('-%d'))
			print(daytime)
			listT.append(daytime)
		except Exception as e:
			print('xxxx时间配置出错失败00：', str(Exception) + str(e))
	for i, sql in enumerate(listT):
		print('正在获取 ' + sql)

	print("9999998888888888")
	now = datetime.datetime.now()
	today_year = now.year
	today_year_months = range(1,now.month+1)

	data_list_todays = []
	for today_year_month in today_year_months:
		# 定义date_list 去年加上今年的每个月
		data_list = '%s-%s' % (today_year, today_year_month)
		#通过函数append，得到今年的列表
		print(data_list)
		data_list_todays.append(data_list)

	print("99999988888888000000000000088")
	now = datetime.datetime.now()
	last_year =  int(datetime.datetime.now().year) -1
	last_year_months = range(now.month+1, 13)


	data_list_lasts = []
	#通过for循环，得到去年的时间夹月份的列表
	#先遍历去年每个月的列表
	for last_year_month in last_year_months:
		# 定义date_list 去年加上去年的每个月
		date_list = '%s-%s' % (last_year, last_year_month)
    	#通过函数append，得到去年的列表
		data_list_lasts.append(date_list)

	print("55555555577777777")
	print(datetime.datetime.now().month+1)
	Time_day = []
	for i in range(1, datetime.datetime.now().month+1):
		try:
			daytime = (datetime.datetime.now().replace(month=i)).strftime('%Y-%m') + (datetime.datetime.now().strftime('-%d'))
			Time_day.append(daytime)
		except Exception as e:
			print('xxxx时间配置出错,已手动调整：'+ str(i) + '月份', str(Exception) + str(e))
			Time_day.append(str(int(datetime.datetime.now().year)) + '-' + str(i) + (datetime.datetime.now().strftime('-%d')))
	for i in range(datetime.datetime.now().month+1, 13):
		try:
			daytime = str(int(datetime.datetime.now().year) -1) + (datetime.datetime.now().replace(month=i)).strftime('-%m') + (datetime.datetime.now().strftime('-%d'))
			Time_day.append(daytime)
		except Exception as e:
			print('xxxx时间配置出错失败00：'+ str(i) + '月份', str(Exception) + str(e))	
			Time_day.append(str(int(datetime.datetime.now().year) -1) + '-' + str(i) + (datetime.datetime.now().strftime('-%d')))
	# for i, sqlT in enumerate(Time_day):
	# 	print('正在获取 ' + sqlT)

	for j in range(0, 12):
		print(j)
		print(Time_day[j])



import datetime
# begin = datetime.date(2014,6,1)
# end = datetime.date(2014,6,7)
# d = begin
# delta = datetime.timedelta(days=1)
# while d <= end:
# print d.strftime("%Y-%m-%d")
print("9999")
t = datetime.datetime.now().strftime('%Y-%m') 
print(t)
Time_da = []
for i in range(datetime.datetime.now().month+1, 13):
	try:
		daytime = str(int(datetime.datetime.now().year) -1) + (datetime.datetime.now().replace(month=i)).strftime('-%m') + (datetime.datetime.now().strftime('-%d'))
		Time_da.append(daytime)
	except Exception as e:
		print('xxxx时间配置出错失败00：'+ str(i) + '月份', str(Exception) + str(e))	
		Time_da.append(str(int(datetime.datetime.now().year) -1) + '-' + str(i) + (datetime.datetime.now().strftime('-%d')))
# for j in range(0, len(Time_da)):
# 	print(j)
# 	print(Time_da[j])
print(Time_da[6])
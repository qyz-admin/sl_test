import datetime
Time_day = []
Time_last = []
for i in range(1, datetime.datetime.now().month + 1):  # 获取当年当前的月份时间
    try:
        daytime = (datetime.datetime.now().replace(month=i)).strftime('%Y-%m') + (
            (datetime.datetime.now()).strftime('-%d'))
        Time_day.append(daytime)
        print(daytime)
    except Exception as e:
        print('xxxx时间配置出错,已手动调整：' + str(i) + '月份', str(Exception) + str(e))
        Time_day.append(
            str(int(datetime.datetime.now().year)) + '-' + str(i) + (datetime.datetime.now().strftime('-%d')))
print(11)
for i in range(datetime.datetime.now().month + 1, 13):  # 获取往年当前的月份时间
    try:
        daytime = str(int(datetime.datetime.now().year) -1) + (datetime.datetime.now().replace(month=i)).strftime('-%m') + (
            (datetime.datetime.now()).strftime('-%d'))
        Time_day.append(daytime)
        print(daytime)
    except Exception as e:
        print('xxxx时间配置出错失败00：' + str(i) + '月份', str(Exception) + str(e))
        Time_day.append(str(int(datetime.datetime.now().year) - 1) + '-' + str(i) + (
            datetime.datetime.now().strftime('-%d')))
# Time_day = ['2021-01-19', '2020-12-19', '2020-12-14', '2020-12-14', '2020-12-14', '2020-12-14', '2020-12-14', '2020-12-14', '2020-12-14', '2020-12-14', '2020-12-14', '2020-12-19']
print(11)

# for j in range(0, 12):
#   print(j)
print(110)
aList = ['123', 'Google', 'Runoob', 'Taobao', 'Facebook'];
 
Time_day.sort();
for j in Time_day:
    print(j)
print(1109) 
print(Time_day[11])
print(Time_day[10])




for tem in ('"神龙家族-港澳台"|slgat', '"红杉家族-港澳台", "红杉家族-港澳台2"|slgat_hs', '"火凤凰-港澳台"|slgat_hfh'):
    tem1 = tem.split('|')[0]
    tem2 = tem.split('|')[1]
    print(tem1)
    print(tem2)

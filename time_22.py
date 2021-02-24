import time
import datetime
import sys
month_last = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m') + '-01'
month_yesterday = datetime.datetime.now().strftime('%Y-%m-%d')
month_begin = '2020-11-01'

print(month_last)

print(month_yesterday)

print(month_begin)

print("55")

print(datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1))


import datetime
from dateutil.relativedelta import relativedelta
 

month_ago = (datetime.datetime.now() - relativedelta(months=2)).strftime('%Y-%m-%d')
print(month_ago)
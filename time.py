import time
import sys
#demo1
import datetime
today = datetime.date.today().strftime('m.d')
print(today)
def process_bar(percent, start_str='', end_str='', total_length=0):
    bar = ''.join(["\033[31m%s\033[0m"%'   '] * int(percent * total_length)) + ''
    bar = '\r' + start_str + bar.ljust(total_length) + ' {:0>4.1f}%|'.format(percent*100) + end_str
    print(bar, end='', flush=True)
 
 
for i in range(101):
    time.sleep(0.1)
    end_str = '100%'
    process_bar(i/100, start_str='', end_str=end_str, total_length=15)
 
#demo2
for i in range(0, 101, 2):
  time.sleep(0.1)
  num = i // 2
  if i == 100:
    process = "\r[%3s%%]: |%-50s|\n" % (i, '|' * num)
  else:
    process = "\r[%3s%%]: |%-50s|" % (i, '|' * num)
  print(process, end='', flush=True)

print("正在下载......")
for i in range(11):
    if i != 10:
        sys.stdout.write("==")
    else:
        sys.stdout.write("== " + str(i*10)+"%/100%")
    sys.stdout.flush()
    time.sleep(0.2)
print("\n" + "下载完成")
# 注：
# sys.stdout.write()方法跟print()方法的区别是 前者打印不换行，后者换行。
# sys.stdout.flush()方法是立即刷新输出的内容 



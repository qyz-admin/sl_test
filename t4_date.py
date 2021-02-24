import matplotlib.pyplot as plt

# 生成柱状图
num_list = [1.5, 0.6, 7.8, 6]
plt.bar(range(len(num_list)), num_list, color='rbgy')
plt.show()


# 生成堆状柱状图
name_list = ['Monday','Tuesday','Friday','Sunday']
num_list = [1.5,0.6,7.8,6]
num_list1 = [1,2,3,1]
plt.bar(range(len(num_list)), num_list, label='boy',fc = 'y')
plt.bar(range(len(num_list)), num_list1, bottom=num_list, label='girl',tick_label = name_list,fc = 'r')
plt.legend()
plt.show()

# 生成堆状柱状图
name_list = ['Monday','Tuesday','Friday','Sunday']
num_list = [1.5,0.6,7.8,6]
num_list1 = [1,2,3,1]
plt.bar(range(len(num_list)), num_list, label='boy',fc = 'y')
plt.bar(range(len(num_list)), num_list1, bottom=num_list, label='girl',tick_label = name_list,fc = 'r')
plt.legend()
plt.show()

# 生成竖状柱状图
name_list = ['Monday', 'Tuesday', 'Friday', 'Sunday']
num_list = [1.5, 0.6, 7.8, 6]
num_list1 = [1, 2, 3, 1]
x = list(range(len(num_list)))
total_width, n = 0.8, 2
width = total_width / n

plt.bar(x, num_list, width=width, label='boy', fc='y')
for i in range(len(x)):
    x[i] = x[i] + width
plt.bar(x, num_list1, width=width, label='girl', tick_label=name_list, fc='r')
plt.legend()
plt.show()

# 生成折线图
import pandas as pd
import numpy as np

df = pd.DataFrame(np.random.rand(15, 4), columns=['a', 'b', 'c', 'd'])
df.plot.area()

# 生成柱状图
import pandas as pd
import numpy as np

df = pd.DataFrame(3 * np.random.rand(5), index=['a', 'b', 'c', 'd', 'e'], columns=['x'])
df.plot.pie(subplots=True)

# 生成箱型图
# 首先导入基本的绘图包
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

#添加成绩表
plt.style.use("ggplot")
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['font.sans-serif']=['SimHei']

# 新建一个空的DataFrame
df=pd.DataFrame()
df["英语"]=[76,90,97,71,70,93,86,83,78,85,81]
df["经济数学"]=[65,95,51,74,78,63,91,82,75,71,55]
df["西方经济学"]=[93,81,76,88,66,79,83,92,78,86,78]
df["计算机应用基础"]=[85,78,81,95,70,67,82,72,80,81,77]
df
plt.boxplot(x=df.values, labels=df.columns, whis=1.5)
plt.show()

# 用pandas自带的画图工具更快
df.boxplot()
plt.show()

# 生成正态分布图
# -*- coding:utf-8 -*-
# Python实现正态分布
# 绘制正态分布概率密度函数
import numpy as np
import matplotlib.pyplot as plt
import math

u = 0  # 均值μ
u01 = -2
sig = math.sqrt(0.2)  # 标准差δ

x = np.linspace(u - 3 * sig, u + 3 * sig, 50)
y_sig = np.exp(-(x - u) ** 2 / (2 * sig ** 2)) / (math.sqrt(2 * math.pi) * sig)
print(x)
print("=" * 20)
print(y_sig)
plt.plot(x, y_sig, "r-", linewidth=2)
plt.grid(True)
plt.show()


import matplotlib.pyplot as plt  
  
name_list = ['Monday','Tuesday','Friday','Sunday']  
num_list = [1.5,0.6,7.8,6]  
num_list1 = [1,2,3,1]  
x =list(range(len(num_list)))  
total_width, n = 0.8, 2  
width = total_width / n  
  
plt.bar(x, num_list, width=width, label='boy',fc = 'y')  
for i in range(len(x)):  
    x[i] = x[i] + width  
plt.bar(x, num_list1, width=width, label='girl',tick_label = name_list,fc = 'r')  
plt.legend()  
plt.show() 

import matplotlib.pyplot as plt
x_values = [1, 2, 3, 4, 5]
y_values = [1, 4, 9, 16, 25]
plt.scatter(x_values, y_values, s=100)
# 设置图表标题并给坐标轴加上标签
plt.title("Square Numbers", fontsize=24)
plt.xlabel("Value", fontsize=14)
plt.ylabel("Square of Value", fontsize=14)
# 设置刻度标记的大小
plt.tick_params(axis='both', which='major', labelsize=14)
plt.show()
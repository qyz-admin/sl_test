# 返回结构体
import ctypes
from ctypes import *

path = r'D:\Twwot.dll'
dll = ctypes.WinDLL(path)

#定义结构体
class StructPointer(ctypes.Structure):  #Structure在ctypes中是基于类的结构体
    _fields_ = [("name", ctypes.c_char * 20), #定义一维数组
                ("age", ctypes.c_int),
                ("arr", ctypes.c_int * 3),   #定义一维数组
                ("arrTwo", (ctypes.c_int * 3) * 2)] #定义二维数组


vl = 'B#EB94B7ECA5B0EA9C8E'
st = 'EA9CB8E8B7A3E8AB8A'
# h = "                                                   "
# h = create_string_buffer(30)
h = c_int(30)
tem = c_int(9425)
j = c_int(438769994)

m = 'EDC-MNO-ML'
ver = 2

print(dll.gk_decrypt2)

print(88)
#设置导出函数返回类型
# dll.gk_decrypt2.restype = ctypes.POINTER(StructPointer)  # POINTER(StructPointer)表示一个结构体指针
# dll.gk_decrypt2.argtypes=[pointer(c_int), c_char_p, c_int, c_int]
#调用导出函数
p = dll.gk_decrypt2(st, byref(h), byref(tem), byref(j))

print(p)
print(h)
print(tem)
print(j)

for y in range(len(h)):
    print(h.raw[y])
print(h.value)


print(889955)

ph = c_char_p(p)
print(ph)
# print(ph.value)

# print(8899)
# s = "朱孝梅"
# c_s = c_wchar_p(s)
# print(c_s)
# print(9988)
# print(c_s.value)


i = c_int()


# print(p.contents.name.decode())  #p.contents返回要指向点的对象   #返回的字符串是utf-8编码的数据，需要解码
# print(p.contents.age)
# print(p.contents.arr[0]) #返回一维数组第一个元素
# print(p.contents.arr[:]) #返回一维数组所有元素
# print(p.contents.arrTwo[0][:]) #返回二维数组第一行所有元素
# print(p.contents.arrTwo[1][:]) #返回二维数组第二行所有元素
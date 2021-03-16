from lxml import etree
  
html = '<html><body><h1>This is a test</h1></body></html>'
# 将html转换成_Element对象
_element = etree.HTML(html)
print ( _element)
# 通过xpath表达式获取h1标签中的文本
text = _element.xpath('//h1/text()')
print ('result is: ' + text[0])

print (99)
text = _element.xpath('//h1/text()')
for tt in text:
	print ('result is: ' + tt)

print (9900)
# encoding=utf8
 
from lxml import etree
 
html = '<html><body><h1>This <a>is a </a>test</h1></body></html>'
_element = etree.HTML(html)
# 先找到h1对象，然后通过etree.tostring方法找到h1对象中的所有文本
_h = _element.xpath('//h1')
# 注意，xpath方法返回的是一个列表，我们需要的是列表中的第一个元素：代表h1标签的_Element对象
result = etree.tostring(_h[0], method='text')
print ('result is: ' + text[0])
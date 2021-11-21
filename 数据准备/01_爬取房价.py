# Author:peng_xinjie
# -*- coding = utf-8 -*-
# @Time:2021/11/21 2:30
# @File:01_爬取房价.py
# @Software:PyCharm

# ?utm_source=baidu&utm_medium=ppc&utm_term=%E9%93%BE%E5%AE%B6%E7%BD%91&utm_content=%E5%93%81%E7%89%8C&utm_campaign=%E9%87%8D%E5%BA%86_%E6%88%90%E4%BA%A4_%E7%9F%AD%E8%AF%AD
# 'gulouqu3', 'taijiangqu1', 'jinanqu1', 'maweiqu1', 'cangshanqu1',

import re
import asyncio
import aiohttp   #原生协程提高爬取速率
import parsel
import pymongo
client = pymongo.MongoClient('localhost', 27017)
db = client['house_prices']  #这里改成你所要创建的数据库名字，也可以是你已有的数据库
urls = []
namelist = ['minhouxian', 'lianjiangxian', 'pingtanxian', 'fuqingshi', 'changlequ1']
for a in range(1, 101):
    for b in range(1, 4):
        for name in namelist:
            url = "https://fz.lianjia.com/ershoufang/{}/pg{}lc{}/".format(name,a, b)
            urls.append(url)
async def get_page(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"}
    async with aiohttp.ClientSession() as session:
        async with await session.get(url, headers=headers) as d:  #因为异步爬虫遇到同步操作会停止所以这里没有用requset.get()方法,这里还有一个挂起操作。
            v = await d.text()
            c = parsel.Selector(v) #创建parsel.Selector对象,用于css选择器解析
            p = c.css(".clear.LOGCLICKDATA")
            for i in p:
                标题 = i.css(".title a::text").get().replace(" ", "-")
                地址 = "-".join(i.css(".positionInfo a::text").getall())
                房源信息 = i.css(".houseInfo ::text").get().split("|")
                房间数 = 房源信息[0]
                面积 = float(房源信息[1].replace("平米", ""))
                朝向 = 房源信息[2].strip()
                装修方式 = 房源信息[3].strip()
                楼层 = 房源信息[-2].strip()
                楼型 = 房源信息[-1].strip()
                单价 = i.css(".unitPrice ::text").get().replace("单价", "").replace("元/平米", "")
                res = re.findall(r'\d+', 单价)
                num = res[0] + res[1]
                总价 = str(int(int(num)*面积/10000))+"万"
                print("正在爬取", 标题)
                result = {
                    'zl_标题': 标题,
                    'zl_地址': 地址,
                    'zl_房源信息': {
                        'zl_房间数': 房间数,
                        'zl_面积(平方米)': 面积,
                        'zl_朝向': 朝向,                        #封装成字典形式方便插入操作
                        'zl_装修方式': 装修方式,
                        'zl_楼层': 楼层,
                        'zl_楼型': 楼型
                    },
                    'zl_单价(元/平米)': 单价,
                    'zl_总价(万元)': 总价
                }
                #print(result)
                db['house_prices_of_fz'].insert(result)  #这里改成你所要创建的集合名字，也可以是你已有的集合
tasks=[]
for url in urls:
    c=get_page(url)
    task=asyncio.ensure_future(c)
    tasks.append(task)  #把多个task任务放在列表里方便协程操作
loop=asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait(tasks))
loop.close()
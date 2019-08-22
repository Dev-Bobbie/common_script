#!/usr/bin/python3
# -*- coding: utf-8 -*-

'''
系统 IO库非常弱

twisted 异步IO

python 3.4

asyncio 库 异步io
asynchttp http异步IO

async/await 语法 专门操作 异步函数

并行计算 非常弱
golang 语言，并行计算领域非常牛逼

'''


# from multiprocessing.dummy import Pool
import gevent.monkey
gevent.monkey.patch_all()
from gevent.pool import Pool


import requests
from lxml import etree


from queue import Queue
import requests,time
from lxml import etree
now = lambda:time.time()

class QiushiSpider(object):

    def __init__(self):
        self.urlQueue = Queue()
        self.base_url = "https://www.qiushibaike.com/8hr/page/{}/"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36"
        }

        self.pool = Pool(5)

        pass

    # 生产 url 函数
    def get_url_list(self):
        for page in range(1,14):
            self.urlQueue.put(self.base_url.format(page))

    def exec_task(self):

        # 1. 从 urlQueue 获取 url
        url = self.urlQueue.get()

        # 2. 发起请求获取响应并且获取  html
        response = requests.get(url,headers = self.headers)

        html = response.text

        # 3. 从 html 中 提取数据
        eroot = etree.HTML(html)
        texts = eroot.xpath('//div["recommend-article"]/ul/li/div/a/text()')
        for item in texts:
            # 4. 保存数据
            print(item)

        self.urlQueue.task_done()
        pass

    def exec_task_finished(self,ret):
        # print("任务执行完成回调")
        self.pool.apply_async(self.exec_task,callback=self.exec_task_finished)
        pass

    def run(self):

        self.get_url_list()


        for i in range(5):
            # 让线程池执行任务
            # 1. 第一个参数执行 具体任务代码函数
            # 2. callback 当任务执行完成以后回调函数
            self.pool.apply_async(self.exec_task,callback=self.exec_task_finished)

        # 需要让祝线程挂起
        self.urlQueue.join()
        pass

if __name__ == '__main__':
    spider = QiushiSpider()
    start = now()
    spider.run()
    print(now()-start)
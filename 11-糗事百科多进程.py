#!/usr/bin/python3
# -*- coding: utf-8 -*-

# import threading
# from queue import Queue

from multiprocessing import Process
from multiprocessing import JoinableQueue as Queue

import requests
from lxml import etree



class QiushiSpider(object):

    def __init__(self):
        self.urlQueue = Queue()
        self.htmlQueue = Queue()
        self.itemsQueue = Queue()

        self.base_url = "https://www.qiushibaike.com/8hr/page/{}/"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36"
        }
        pass

    # 生产 url 函数
    def get_url_list(self):
        for page in range(1,14):
            self.urlQueue.put(self.base_url.format(page))

    # 即是生产者（html）又是消费者url
    def parse_url(self):
        while True:
            # 从url队列中获取
            url = self.urlQueue.get()

            # 网络请求 生成 html
            response = requests.get(url,headers = self.headers)
            html = response.text

            self.htmlQueue.put(html)

            self.urlQueue.task_done()

    # 解析html,即是生产者（数据）又是消费者 html
    def parse_html(self):
        while True:
            html = self.htmlQueue.get()

            eroot = etree.HTML(html)
            texts = eroot.xpath('//div[@class="content"]/span/text()')
            for item in texts:
                self.itemsQueue.put(item)

            self.htmlQueue.task_done()

    # 消费 数据
    def save_item(self):
        while True:
            item = self.itemsQueue.get()
            # 保存数据
            print(item)
            self.itemsQueue.task_done()

    def run(self):

        tasks = []
        get_url_list_task = Process(target=self.get_url_list)
        tasks.append(get_url_list_task)

        for i in range(5):
            parse_url_task = Process(target=self.parse_url)
            tasks.append(parse_url_task)

        for i in range(3):
            parse_html_task = Process(target=self.parse_html)
            tasks.append(parse_html_task)

        for i in range(2):
            save_item_task = Process(target=self.save_item)
            tasks.append(save_item_task)

        for task in tasks:
            # task.setDaemon(True)
            # daemon 当主进程结束，子进程响应的结束
            task.daemon = True
            task.start()



        # 需要让祝线程挂起
        self.urlQueue.join()
        self.htmlQueue.join()
        self.itemsQueue.join()


        pass

if __name__ == '__main__':
    spider = QiushiSpider()
    spider.run()
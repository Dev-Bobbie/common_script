#!/usr/bin/python3
# -*- coding: utf-8 -*-

'''
队列
1. 线程安全
2. api
'''

from queue import Queue

q = Queue()


print("unfinished_tasks:",q.unfinished_tasks)

# 内容被添加到队列中,通过 unfinished_tasks + 1
q.put("abc")

print("qsize:",q.qsize())
print("unfinished_tasks:",q.unfinished_tasks)

# 从队列中获取内容，但是 unfinished_tasks 不做任何变化
q.get()

print("qsize:",q.qsize())
print("unfinished_tasks:",q.unfinished_tasks)

# unfinished_tasks - 1
q.task_done()

print("unfinished_tasks:",q.unfinished_tasks)

# join unfinished_tasks 的值如果不为 0 就挂起，直到 unfinished_tasks 为 0 程序结束
q.join()
import threading
import pymongo
from pymongo.collection import Collection
from bson import json_util as jsonb
import asyncio,time

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

except ImportError:
    pass

client = pymongo.MongoClient(host='192.168.33.11',port=27018,connect=False)
db = client['douyin']


def gen_fun(x):
    for i in x:
        task_info = {}
        task_info['share_id'] = i.replace('\n', '')
        task_info['task_type'] = 'share_id'
        print('当前保存的task为%s:' % task_info)
        yield task_info

def save_task(task):
    task_collections = Collection(db,'douyin_task')
    task_collections.insert({'share_id':task['share_id']},task,True)

def get_task(task_type):
    task_collections = Collection(db,'douyin_task')
    task = task_collections.find_one_and_delete({'task_type':task_type})
    return task

def delete_task(task):
    pass

def find_all_task():
    task_collections = Collection(db, 'douyin_task')
    task = jsonb.dumps(list(task_collections.find()))
    return task

def save_data(item):
    data_collections = Collection(db,'douyin_data')
    data_collections.insert(item)


if __name__ == '__main__':

    with open('100wshare_id.txt','r',encoding='utf-8') as f:
        f_read = f.readlines()
        start_time = time.time()
        loop = asyncio.get_event_loop()
        future_tasks = []
        fss = gen_fun(f_read)
        for fs in fss:
            task = loop.run_in_executor(None, save_task, fs)
            future_tasks.append(task)
        loop.run_until_complete(asyncio.wait(future_tasks))
        print("last time:{}".format(time.time()-start_time))


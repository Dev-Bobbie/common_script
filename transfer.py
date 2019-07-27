from pymongo import MongoClient
import pymysql,json
import threading

sqlClient = pymysql.connect(host='192.168.33.11',port=3306,user='root',password='mysql',db='test',charset='utf8')
remoteClient = MongoClient("192.168.33.11",27018)
sqlcursor = sqlClient.cursor()

rmtCollection = remoteClient['yase']["items"]
cursor = rmtCollection.find()
cursorLock = threading.Lock()
resultList = list()
resultLock = threading.Lock()

def work():
    global cursor,cursorLock,resultList,resultLock
    with cursorLock:
        r = next(cursor,None)
    while r:
        r.pop("_id")
        with resultLock:
            resultList.append(r)
        with cursorLock:
            r = next(cursor,None)

def collect():
    global resultList,resultLock,sqlClient
    cursor = sqlClient.cursor()
    import time
    while True:
        tempList = list()
        with resultLock:
            tempList = resultList.copy()
            resultList = list()
        if len(tempList):
            try:
                sql = """INSERT INTO yase (`{}`) VALUES ({})""".format('`,`'.join([i for i in tempList[0].keys()]),','.join(["%s"for i in range(len(tempList[0].keys()))]))
                param = []
                for temp in tempList:
                    tmp_list = []
                    for i in tempList[0].keys():
                        tmp_list.append(temp[i])
                    param.append(tmp_list)
                    
                cursor.executemany(sql,param)
                sqlClient.commit()
            except Exception as e:
                print(e)
                sqlClient.rollback()

        time.sleep(0.1)


if __name__=="__main__":
    threads = list()
    thread_collect = threading.Thread(target=collect)
    thread_collect.start()
    threads.append(thread_collect)
    for i in range(20):
        thread = threading.Thread(target=work)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

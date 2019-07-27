from pymongo import MongoClient
import pymysql,json
import threading
import asyncio
import time
import aiomysql

remoteClient = MongoClient("192.168.33.11",27018)
rmtCollection = remoteClient['yase']["items"]
cursor = rmtCollection.find()

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


async def consumer(tempList,pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                sql = """INSERT INTO yase (`{}`) VALUES ({})""".format('`,`'.join([i for i in tempList[0].keys()]),','.join(["%s"for i in range(len(tempList[0].keys()))]))
                param = []
                for temp in tempList:
                    tmp_list = []
                    for i in tempList[0].keys():
                        tmp_list.append(temp[i])
                    param.append(tmp_list)
                await cur.executemany(sql,param)
            except Exception as e:
                print(e)


async def main(loop):
    pool = await aiomysql.create_pool(host="192.168.33.11",port=3306,user="root",
                            password="mysql",db="test",loop=loop,
                            charset = "utf8",autocommit = True)
    fss = []
    for _ in cursor:
        _.pop("_id")
        fss.append(_)

    k = [fss[i:i+1000] for i in range(0,len(fss),1000)]
    for s in k:
        asyncio.ensure_future(consumer(s,pool))

    

if __name__=="__main__":
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(main(loop))
    loop.run_forever()


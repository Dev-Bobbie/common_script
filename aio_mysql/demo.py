import asyncio

from aiomysql.sa import create_engine

from model import user,metadata

async def go(loop):
    engine = await create_engine(user='root', db='test_mysql',
                                 host='127.0.0.1', password='123456', loop=loop)
    async with engine.acquire() as conn:
        await conn.execute(user.insert().values(user_name='user_name01', pwd='123456', real_name='real_name01'))
        await conn.execute('commit')

        async for row in conn.execute(user.select()):
            print(row.user_name, row.pwd)

    engine.close()
    await engine.wait_closed()


loop = asyncio.get_event_loop()
loop.run_until_complete(go(loop))

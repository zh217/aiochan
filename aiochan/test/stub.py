import asyncio

from aiochan import *


async def sample():
    a = Chan(name='a')
    b = Chan(name='b')
    c = Chan(name='c')
    out = combine_latest(a, b, c, buffer=1)
    print('put', a, 1)
    await a.put(1)
    assert [1, None, None] == await out.get()
    a.close()
    print('put', b, 2)
    await b.put(2)
    assert [1, 2, None] == await out.get()
    print('put', c, 3)
    await c.put(3)
    assert [1, 2, 3] == await out.get()
    b.close()
    c.close()
    assert out.closed

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(sample())

import asyncio
import random

from aiochan import *


async def boring(msg):
    for i in range(5):
        print(msg, i)
        await timeout(random.random()).get()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(boring('boring'))

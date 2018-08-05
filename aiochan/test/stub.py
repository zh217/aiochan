import asyncio
from aiochan import *


async def sample():
    c = Chan()
    pass


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(sample())

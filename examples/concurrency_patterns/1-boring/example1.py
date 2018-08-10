import random

from aiochan import *


async def boring(msg):
    for i in range(5):
        print(msg, i)
        await timeout(random.random()).get()


async def main():
    await boring('boring')

if __name__ == '__main__':
    run_in_thread(main())

import random

from aiochan import *


async def boring(msg):
    for i in range(5):
        print(msg, i)
        await timeout(random.random()).get()


async def main():
    # run asynchronously
    go(boring('boring'))

    # program will exit immediately

if __name__ == '__main__':
    run_in_thread(main())

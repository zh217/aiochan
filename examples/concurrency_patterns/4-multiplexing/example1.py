import random

from aiochan import *


async def boring(msg):
    c = Chan()

    async def work():
        i = 0
        while True:
            await c.put(f'{msg} {i}')
            await timeout(random.random()).get()
            i += 1

    go(work())
    return c


def fan_in(input1, input2):
    c = Chan()

    async def work1():
        while True:
            await c.put(await input1.get())

    go(work1())

    async def work2():
        while True:
            await c.put(await input2.get())

    go(work2())

    return c


async def main():
    c = fan_in(await boring('Joe'), await boring('Ann'))

    for i in range(10):
        print(await c.get())

    print("You're boring: I'm leaving.")


if __name__ == '__main__':
    run_in_thread(main())

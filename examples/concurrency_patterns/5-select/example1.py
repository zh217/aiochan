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

    async def work():
        while True:
            s, _ = await select(input1, input2)
            await c.put(s)

    go(work())

    return c


async def main():
    c = fan_in(await boring('Joe'), await boring('Ann'))

    for i in range(10):
        print(await c.get())

    print("You're boring: I'm leaving.")


if __name__ == '__main__':
    run_in_thread(main())

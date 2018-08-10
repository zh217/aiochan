import random

from aiochan import *


async def boring(msg, quit):
    c = Chan()

    async def work():
        i = 0
        while True:
            _, ch = await select((c, f'{msg} {i}'), quit)
            if ch is quit:
                return
            i += 1

    go(work())
    return c


async def main():
    quit = Chan()
    c = await boring('Joe', quit)

    for _ in range(10):
        print(await c.get())

    await quit.put(True)
    print('EXIT')


if __name__ == '__main__':
    run_in_thread(main())

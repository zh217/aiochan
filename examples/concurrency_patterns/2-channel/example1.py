import random

from aiochan import *


async def boring(msg, c):
    i = 0
    while True:
        await c.put(f'{msg} {i}')
        await timeout(random.random()).get()
        i += 1


async def main():
    # unbuffered

    c = Chan()

    go(boring('boring!', c))

    for i in range(5):
        print('You say: %s' % (await c.get()))

    print("You're boring: I'm leaving.")


if __name__ == '__main__':
    run_in_thread(main())

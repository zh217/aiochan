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


async def main():
    # unbuffered

    joe = await boring('Joe')
    ann = await boring('Ann')

    for i in range(5):
        print(await joe.get())  # Joe and Ann are blocking each other.
        print(await ann.get())  # Waiting for a message to read.

    print("You're boring: I'm leaving.")


if __name__ == '__main__':
    run_in_thread(main())

import random
import collections

from aiochan import *

Message = collections.namedtuple('Message', 'str wait')


async def boring(msg):
    c = Chan()
    wait_for_it = Chan()

    async def work():
        i = 0
        while True:
            await c.put(Message(f'{msg} {i}', wait_for_it))
            await timeout(random.random()).get()
            await wait_for_it.get()
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
        msg1 = await c.get()
        print(msg1.str)

        msg2 = await c.get()
        print(msg2.str)

        await msg1.wait.put(True)
        await msg2.wait.put(True)

    print("You're boring: I'm leaving.")


if __name__ == '__main__':
    run_in_thread(main())

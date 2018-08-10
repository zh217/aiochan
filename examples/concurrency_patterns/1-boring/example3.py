import random

from aiochan import *


async def boring(msg):
    i = 0
    while True:
        print(msg, i)
        await timeout(random.random()).get()
        i += 1


async def main():
    # run asynchronously
    go(boring('boring'))

    print("I'm listening.")
    await timeout(2).get()
    print("You're boring: I'm leaving.")

    # program will exit immediately


if __name__ == '__main__':
    run_in_thread(main())

from aiochan import *


async def pass_on(left, right):
    value = await left.get()
    await right.put(1 + value)
    print(f'Left[{value}] Right[{value + 1}]')


async def main():
    n = 6
    left = None
    rightmost = Chan()
    right = rightmost
    for _ in range(n):
        left = Chan()
        go(pass_on(left, right))
        right = left

    print('Coroutines are waiting')

    async def giver(c):
        print('Give Gopher1 the initial value')
        await c.put(1)

    go(giver(left))
    print('Final value: ' + str(await rightmost.get()))


if __name__ == '__main__':
    run_in_thread(main())

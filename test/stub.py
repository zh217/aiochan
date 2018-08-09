from aiochan import *


async def sample():
    c = Chan()
    d = c.debounce(0.1)

    async def worker():
        await c.put(1)
        await c.put(2)
        await c.put(3)
        await timeout(0.2).get()
        await c.put(4)
        await c.put(5)
        await c.put(6)
        await timeout(0.2).get()
        c.close()

    go(worker())

    assert [3, 6] == await d.collect()

if __name__ == '__main__':
    go_thread(sample())

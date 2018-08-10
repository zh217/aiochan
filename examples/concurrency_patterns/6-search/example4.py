from aiochan import *
import time
import random


def fake_search(kind):
    async def searcher(query):
        await timeout(random.uniform(0, 0.1)).get()
        return f'{kind} result for {query}'

    return searcher


async def first(query, *replicas):
    c = Chan()

    async def search_replica(replica):
        await c.put(await replica(query))

    for replica in replicas:
        go(search_replica(replica))

    return await c.get()


async def main():
    start = time.time()
    result = await first('aiochan', fake_search('replica 1'), fake_search('replica 2'))
    elapsed = time.time() - start

    print(result)
    print(f'{elapsed * 1000} ms')


if __name__ == '__main__':
    run_in_thread(main())

from aiochan import *
import time
import random


def fake_search(kind):
    async def searcher(query):
        await timeout(random.uniform(0, 0.1)).get()
        return f'{kind} result for {query}'

    return searcher


web1 = fake_search('web')
web2 = fake_search('web')
image1 = fake_search('image')
image2 = fake_search('image')
video1 = fake_search('video')
video2 = fake_search('video')


async def first(query, *replicas):
    c = Chan()

    async def search_replica(replica):
        await c.put(await replica(query))

    for replica in replicas:
        go(search_replica(replica))

    return await c.get()


async def duckduckgo(query):
    c = Chan()

    async def worker(*searchers):
        await c.put(await first(query, *searchers))

    go(worker(web1, web2))
    go(worker(image1, image2))
    go(worker(video1, video2))

    tout = timeout(0.08)

    results = []

    for _ in range(3):
        r, ch = await select(c, tout)
        if ch is tout:
            print('timed out')
            return results
        else:
            results.append(r)

    return results


async def main():
    start = time.time()
    results = await duckduckgo('aiochan')
    elapsed = time.time() - start
    for result in results:
        print(result)
    print(f'{elapsed * 1000} ms')


if __name__ == '__main__':
    run_in_thread(main())

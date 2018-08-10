from aiochan import *
import time
import random


def fake_search(kind):
    async def searcher(query):
        await timeout(random.uniform(0, 0.1)).get()
        return f'{kind} result for {query}'

    return searcher


web = fake_search('web')
image = fake_search('image')
video = fake_search('video')


async def duckduckgo(query):
    c = Chan()

    async def worker(searcher):
        await c.put(await searcher(query))

    go(worker(web))
    go(worker(image))
    go(worker(video))

    results = []

    for _ in range(3):
        r = await c.get()
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

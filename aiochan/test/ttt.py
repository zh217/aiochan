import asyncio
import aiochan as ac

if __name__ == '__main__':
    async def worker(inp, tag):
        async for v in inp:
            print('%s received %s' % (tag, v))


    async def main():
        dup = ac.from_range(5).dup()
        inputs = [ac.Chan(name='inp%s' % i) for i in range(3)]
        dup.tap(*inputs)

        for idx, c in enumerate(inputs):
            ac.go(worker(c, 'worker%s' % idx))
        await asyncio.sleep(1)


    ac.run(main())

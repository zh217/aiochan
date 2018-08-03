import asyncio

from .channel import Chan


def go(f, *args, _loop=None, **kwargs):
    loop = _loop or asyncio.get_event_loop()
    ch = Chan(loop=loop)
    if asyncio.iscoroutinefunction(f):
        async def worker():
            res = await f(*args, **kwargs)
            if res is not None:
                ch.put_nowait(res)
            ch.close()

        loop.create_task(worker())
    elif callable(f):
        def worker():
            res = f(*args, **kwargs)
            if res is not None:
                ch.put_nowait(res)
            ch.close()

        loop.call_soon(worker)
    else:
        def worker():
            res = f
            if res is not None:
                ch.put_nowait(res)
            ch.close()

        loop.call_soon(worker)
    return ch

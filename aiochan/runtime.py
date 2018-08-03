import asyncio
import queue
from .channel import Chan


def pipe_threadsafe(c1, c2):
    async def worker():
        async for v in c1:
            c2.loop.call_soon_threadsafe(c2.put_nowait, c2, v, immediate_only=False)
        c2.loop.call_soon_threadsafe(c2.close)

    c1.loop.create_task(worker())


class ThreadRunner:
    def __init__(self, coro_fn, loop=None, in_buffer_size=None, out_buffer_size=None):
        self.loop = loop or asyncio.new_event_loop()
        self._in_chan = Chan(loop=loop)
        self._out_chan = Chan(loop=loop)
        self._in_q = queue.Queue(maxsize=in_buffer_size)
        self._out_q = queue.Queue(maxsize=out_buffer_size)
        self._out_chan.to_queue(self._out_q)
        self._coro_fn = coro_fn
        self._run = False

    def run(self):
        assert not self._run
        self._run = True

        async def queuer():
            while True:
                item = self._in_q.get()
                if item is None:
                    self._in_chan.close()
                    break
                else:
                    self._in_chan.put_nowait(item, immediate_only=False)

        self.loop.run_forever()

        asyncio.run_coroutine_threadsafe(self._coro_fn(self._in_chan, self._out_chan), loop=self.loop)
        asyncio.run_coroutine_threadsafe(queuer(), loop=self.loop)

    def stop(self):
        self.loop.stop()

    def __call__(self):
        self.run()

    def __enter__(self):
        self.run()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def send(self, item, block=True, timeout=None):
        self._in_q.put(item, block=block, timeout=timeout)

    def recv(self, block=True, timeout=None):
        return self._out_q.get(block=block, timeout=timeout)

    @property
    def out(self):
        return self._out_q

    def __iter__(self):
        def item_gen():
            while True:
                item = self._out_q.get()
                if item is None:
                    break
                else:
                    yield item

        return iter(item_gen())

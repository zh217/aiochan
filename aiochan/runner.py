import asyncio
import queue
import threading
import functools
from .channel import Chan


def pipe_interthread(c1: Chan, c2: Chan) -> None:
    """

    :param c1:
    :param c2:
    :return:
    """

    async def worker():
        async for v in c1:
            c2.loop.call_soon_threadsafe(c2.put_nowait, c2, v, immediate_only=False)
        c2.loop.call_soon_threadsafe(c2.close)

    c1.loop.create_task(worker())


class ThreadRunner:
    """

    """

    def __init__(self, coro_fn, loop=None, in_buffer_size=None, out_buffer_size=None):
        loop = loop or asyncio.new_event_loop()
        self.loop = loop
        self._in_chan = Chan(loop=loop)
        self._out_chan = Chan(loop=loop)
        self._in_q = queue.Queue(maxsize=in_buffer_size or 0)
        self._out_q = queue.Queue(maxsize=out_buffer_size or 0)
        self._out_chan.to_queue(self._out_q)

        async def coro_fn_(inc, ouc):
            await coro_fn(inc, ouc)
            inc.close()
            ouc.close()

        self._coro_fn = coro_fn_
        self._run = False
        self._thread = None
        self._queue_thread = None

    def start(self):
        """

        :return:
        """
        assert not self._run
        self._run = True

        def queuer(in_q, in_chan):
            while True:
                item = in_q.get()
                if item is None:
                    in_chan.loop.call_soon_threadsafe(functools.partial(in_chan.close))
                    break
                else:
                    in_chan.loop.call_soon_threadsafe(
                        functools.partial(in_chan.put_nowait, item, immediate_only=False))

        def starter(loop):
            loop.run_until_complete(self._coro_fn(self._in_chan, self._out_chan))

        t = threading.Thread(target=starter, args=(self.loop,))
        t.start()
        self._thread = t

        tq = threading.Thread(target=queuer, args=(self._in_q, self._in_chan))
        tq.start()
        self._queue_thread = tq

        return self

    def stop(self):
        """

        :return:
        """
        self._in_q.put(None)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def send(self, item, block=True, timeout=None):
        """

        :param item:
        :param block:
        :param timeout:
        :return:
        """
        self._in_q.put(item, block=block, timeout=timeout)

    def recv(self, block=True, timeout=None):
        """

        :param block:
        :param timeout:
        :return:
        """
        return self._out_q.get(block=block, timeout=timeout)

    @property
    def out(self):
        """

        :return:
        """
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

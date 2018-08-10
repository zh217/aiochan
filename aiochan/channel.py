import asyncio
import collections
import functools
import itertools
import numbers
import operator
import queue
import random
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from . import buffers
from ._util import FnHandler, SelectFlag, SelectHandler

_buf_types = {'f': buffers.FixedLengthBuffer,
              'd': buffers.DroppingBuffer,
              's': buffers.SlidingBuffer,
              'p': buffers.PromiseBuffer}

__all__ = ('Chan', 'select', 'merge', 'from_iter', 'from_range', 'zip_chans', 'combine_latest', 'tick_tock', 'timeout',
           'Mux', 'Dup', 'Pub', 'go', 'go_thread')

MAX_OP_QUEUE_SIZE = 1024
"""
The maximum pending puts or pending takes for a channel.

Usually you should leave this option as it is. If you find yourself receiving exceptions due to put/get queue size
exceeding limits, you should consider using appropriate :mod:`aiochan.buffers` when creating the channels.
"""

MAX_DIRTY_SIZE = 256
"""
The size of cancelled operations in put/get queues before a cleanup is triggered (an operation can only become cancelled
due to the :meth:`aiochan.channel.select` or operations using it, or in other words, there is no direct user control of
cancellation).
"""


class Chan:
    """
    A channel, the basic construct in CSP-style concurrency.

    Channels can be used as async generators using the ``async for`` construct for async iteration of the values.

    Channels can be used as context managers using the ``with`` construct: when exiting the context, the channel
    will be closed.

    :param buffer: if a :meth:`aiochan.buffers.AbstractBuffer` is given, then it will be used as the buffer. In this
            case `buffer_size` has no effect.

            If an integer is given, then a :meth:`aiochan.buffers.FixedLengthBuffer` will be created with the integer
            value as the buffer size and used.

            If the a string value of `f`, `d`, `s` or `p` is given, a :meth:`aiochan.buffers.FixedLengthBuffer`,
            :meth:`aiochan.buffers.DroppingBuffer`,  :meth:`aiochan.buffers.SlidingBuffer` or
            :meth:`aiochan.buffers.PromiseBuffer` will be created and used, with size given by the parameter
            `buffer_size`.
    :param buffer_size: see the doc for `buffer`.
    :param loop: the asyncio loop that should be used when scheduling and creating futures. If `None`, will use the
            current loop. If the special string value `"no_loop"` is given, then will not use a loop at all. Even
            in this case the channel can operate if you use only :meth:`aiochan.channel.Chan.get_nowait` and
            :meth:`aiochan.channel.Chan.put_nowait`.
    :param name: used to provide more friendly debugging outputs.
    """
    __slots__ = ('loop', '_buf', '_gets', '_puts', '_closed', '_dirty_puts', '_dirty_gets', '_name')

    _count = 0

    def __init__(self,
                 buffer=None,
                 buffer_size=None,
                 *,
                 loop=None,
                 name=None):
        self._name = name or '_unk' + '_' + str(self.__class__._count)
        if loop == 'no_loop':
            self.loop = None
        else:
            self.loop = loop or asyncio.get_event_loop()
        try:
            self._buf = _buf_types[buffer](buffer_size)
        except KeyError:
            if isinstance(buffer, numbers.Integral):
                self._buf = buffers.FixedLengthBuffer(buffer)
            else:
                self._buf = buffer

        self._gets = collections.deque()
        self._puts = collections.deque()
        self._closed = False
        self._dirty_puts = 0
        self._dirty_gets = 0
        self.__class__._count += 1

    def _notify_dirty(self, is_put):
        if is_put:
            self._dirty_puts += 1
            # print('notified put', self._dirty_puts)
        else:
            self._dirty_gets += 1
            # print('notified get', self._dirty_gets)

    def _dispatch(self, f, value=None):
        if f is None:
            return
        elif asyncio.isfuture(f):
            f.set_result(value)
        elif asyncio.iscoroutinefunction(f):
            self.loop.create_task(f(value))
        else:
            f(value)
            # self.loop.call_soon(functools.partial(f, value))

    def _clean_gets(self):
        self._gets = collections.deque(g for g in self._gets if g.active)
        self._dirty_gets = 0

    def _clean_puts(self):
        self._puts = collections.deque(p for p in self._puts if p[0].active)
        self._dirty_puts = 0

    # noinspection PyRedundantParentheses
    def _put(self, val, handler):
        if val is None:
            raise TypeError('Cannot put None on a channel')

        if self.closed or not handler.active:
            return (not self.closed,)

        # case 1: buffer available, and current buffer and then drain buffer
        if self._buf and self._buf.can_add:
            # print('put op: buffer')
            handler.commit()
            self._buf.add(val)
            while self._gets and self._buf.can_take:
                getter = self._gets.popleft()
                if getter.active:
                    self._dispatch(getter.commit(), self._buf.take())
            return (True,)

        getter = None
        while True:
            try:
                g = self._gets.popleft()
                if g and g.active:
                    getter = g
                    break
            except IndexError:
                self._dirty_gets = 0
                break

        # case 2: no buffer and pending getter, dispatch immediately
        if getter is not None:
            # print('put op: dispatch immediate to getter')
            handler.commit()
            self._dispatch(getter.commit(), val)
            return (True,)

        # case 3: no buffer, no pending getter, queue put op if put is blockable
        if handler.blockable:
            # print('put op: queue put')
            if self._dirty_puts >= MAX_DIRTY_SIZE:
                self._clean_puts()
            assert len(self._puts) < MAX_OP_QUEUE_SIZE, \
                'No more than ' + str(MAX_OP_QUEUE_SIZE) + ' pending puts are ' + \
                'allowed on a single channel. Consider using a windowed buffer.'
            handler.queue(self, True)
            self._puts.append((handler, val))
            return None

    # noinspection PyRedundantParentheses
    def _get(self, handler):
        if not handler.active:
            return None

        # case 1: buffer has content, return buffered value and drain puts queue
        if self._buf and self._buf.can_take:
            # print('get op: get from buffer')
            handler.commit()
            val = self._buf.take()
            while self._buf.can_add:
                try:
                    putter = self._puts.popleft()
                    if putter[0].active:
                        self._buf.add(putter[1])
                        self._dispatch(putter[0].commit(), True)
                except IndexError:
                    self._dirty_puts = 0
                    break
            return (val,)

        putter = None
        while True:
            try:
                p = self._puts.popleft()
                if p[0].active:
                    putter = p
                    break
            except IndexError:
                self._dirty_puts = 0
                break

        # case 2: we have a putter immediately available
        if putter is not None:
            # print('get op: get immediate from putter')
            handler.commit()
            self._dispatch(putter[0].commit(), True)
            return (putter[1],)

        # case c: we are closed and no buffer
        if self.closed:
            if handler.active and handler.commit():
                return (None,)
            else:
                return None

        # case 3: cannot deal with getter immediately: queue if blockable
        if handler.blockable:
            # print('get op: queue get op')
            if self._dirty_gets >= MAX_DIRTY_SIZE:
                self._clean_gets()
            assert len(self._gets) < MAX_OP_QUEUE_SIZE, \
                'No more than ' + str(MAX_OP_QUEUE_SIZE) + ' pending gets ' + \
                'are allowed on a single channel'
            handler.queue(self, False)
            self._gets.append(handler)
            return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __aiter__(self):
        return ChanIterator(self)

    def __repr__(self):
        return 'Chan<' + self._name + str(id(self)) + '>'

    def put(self, val):
        """
        **Coroutine**. Put a value into the channel.

        :param val: value to put into the channel. Cannot be `None`.
        :return: Awaitable of `True` if the op succeeds before the channel is closed, `False` if the op is applied to a
                 then-closed channel.
        """
        ft = self.loop.create_future()
        ret = self._put(val, FnHandler(ft, blockable=True))
        if ret is not None:
            ft = self.loop.create_future()
            ft.set_result(ret[0])
        return ft

    def put_nowait(self, val, cb=None, *, immediate_only=True):
        """
        Put `val` into the channel synchronously.

        If `immediate_only` is `True`, the operation will not be queued if it cannot complete immediately.

        When `immediate_only` is `False`, `cb` can be optionally provided, which will be called when the put op
        eventually completes, with a single argument`True` or `False` depending on whether the channel is closed
        at the time of completion of the put op. `cb` cannot be supplied when `immediate_only` is `True`.

        Returns `True` if the put succeeds immediately, `False` if the channel is already closed, `None` if the
        operation is queued.
        """
        if immediate_only:
            assert cb is None, 'cb must be None if immediate_only is True'
            ret = self._put(val, FnHandler(None, blockable=False))
            if ret:
                return ret[0]
            else:
                return None

        ret = self._put(val, FnHandler(cb, blockable=True))
        if ret is None:
            return None

        if cb is not None:
            self._dispatch(cb, ret[0])
        return ret[0]

    def add(self, *vals):
        """
        Convenient method for putting many elements to the channel. The put semantics is the same
        as :meth:`aiochan.channel.Chan.put_nowait` with `immediate_only=False`.

        Note that this method can potentially overflow the channel's put queue, so it is only suitable for
        adding small number of elements.

        :param vals: values to add, none of which can be `None`.
        :return: `self`
        """
        for v in vals:
            self.put_nowait(v, immediate_only=False)
        return self

    def get(self):
        """
        **Coroutine**. Get a value of of the channel.

        :return: An awaitable holding the obtained value, or of `None` if the channel is closed before succeeding.
        """
        ft = self.loop.create_future()
        ret = self._get(FnHandler(ft, blockable=True))
        if ret is not None:
            ft = self.loop.create_future()
            ft.set_result(ret[0])
        return ft

    def get_nowait(self, cb=None, *, immediate_only=True):
        """
        try to get a value from the channel but do not wait.
        :type self: Chan
        :param self:
        :param cb: a callback to execute, passing in the eventual value of the get operation, which is None
        if the channel becomes closed before a value is available. Cannot be supplied when immediate_only is True.
        Note that if cb is supplied, it will be executed even when the value IS immediately available and returned
        by the function.
        :param immediate_only: do not queue the get operation if it cannot be completed immediately.
        :return: the value if available immediately, None otherwise
        """
        if immediate_only:
            assert cb is None, 'cb must be None if immediate_only is True'
            ret = self._get(FnHandler(None, blockable=False))
            if ret:
                return ret[0]
            else:
                return None

        ret = self._get(FnHandler(cb, blockable=True))

        if ret is not None:
            if cb is not None:
                self._dispatch(cb, ret[0])
            return ret[0]

        return None

    def close(self):
        """
        Close the channel.

        After this method is called, further puts to this channel will complete immediately without doing anything.
        Further gets will yield values in pending puts or buffer. After pending puts and buffer are both drained,
        gets will complete immediately with *None* as the result.

        Closing an already closed channel is an no-op.

        :return: `self`
        """
        if self._closed:
            return self
        while True:
            try:
                getter = self._gets.popleft()
                if getter.active:
                    val = self._buf.take() if self._buf and self._buf.can_take else None
                    self._dispatch(getter.commit(), val)
            except IndexError:
                self._dirty_gets = 0
                break
        self._closed = True
        return self

    @property
    def closed(self):
        """
        :return: whether this channel is already closed.
        """
        return self._closed

    async def _pipe_worker(self, out):
        async for v in self:
            if not await out.put(v):
                break
        out.close()

    def async_apply(self, f=_pipe_worker, out=None):
        """
        Apply a coroutine function to values in the channel, giving out an arbitrary number of results into the output
        channel and return the output value.

        :param out: the `out` channel giving to the coroutine function `f`. If `None`, a new channel with no buffer
                  will be created.
        :param f: a coroutine function taking two channels, `inp` and `out`. `inp` is the current channel and `out` is
                  the given or newly created out channel. The coroutine function should take elements
                  from `inp`, do its processing, and put the processed values into `out`.  When, how often and whether
                  values are put into `out`, and when or whether `out` is ever closed, is up to the coroutine.

                  If `f` is not given, an identity coroutine function which will just pass the values along and close
                  `out` when `inp` is closed is used.
        :return: the `out` channel.
        """
        if out is None:
            out = Chan()
        self.loop.create_task(f(self, out))
        return out

    def async_pipe(self, n, f, out, *, close=True):
        """
        Asynchronously apply the coroutine function `f` to each value in the channel, and pipe the results to `out`.
        The results will be processed in unspecified order but will be piped into `out` in the order of their inputs.

        If `f` involves slow or blocking operation, consider using `parallel_pipe`.

        If ordering is not important, consider using `async_pipe_unordered`.

        :param n: how many coroutines to spawn for processing.
        :param f: a coroutine function accepting one input value and returning one output value. S
                  hould never return `None`.
        :param out: the output channel. if `None`, one without buffer will be created and used.
        :param close: whether to close the output channel when the input channel is closed.
        :return: the output channel.
        """
        if out is None:
            out = Chan()

        jobs = Chan(n, loop=self.loop)
        results = Chan(n, loop=self.loop)

        async def job_in():
            async for v in self:
                res = Chan('p')
                await jobs.put((v, res))
                await results.put(res)
            jobs.close()
            results.close()

        async def worker():
            async for v, res in jobs:
                r = await f(v)
                await res.put(r)

        async def job_out():
            async for rc in results:
                r = await rc.get()
                if not await out.put(r):
                    break
            if close:
                out.close()

        self.loop.create_task(job_out())
        self.loop.create_task(job_in())
        for _ in range(n):
            self.loop.create_task(worker())

        return out

    def async_pipe_unordered(self, n, f, out, *, close=True):
        """
        Asynchronously apply the coroutine function `f` to each value in the channel, and pipe the results to `out`.
        The results will be put into `out` in an unspecified order: whichever result completes first will be given
        first.

        If `f` involves slow or blocking operation, consider using `parallel_pipe_unordered`.

        If ordering is not important, consider using `async_pipe`.

        :param n: how many coroutines to spawn for processing.
        :param f: a coroutine function accepting one input value and returning one output value.
                  Should never return `None`.
        :param out: the output channel. if `None`, one without buffer will be created and used.
        :param close: whether to close the output channel when the input channel is closed.
        :return: the output channel.
        """
        if out is None:
            out = Chan()

        pending = n

        async def work():
            nonlocal pending
            async for v in self:
                r = await f(v)
                await out.put(r)
            pending -= 1
            if pending == 0 and close:
                out.close()

        for _ in range(n):
            self.loop.create_task(work())

        return out

    def parallel_pipe(self, n, f, out=None, mode='thread', close=True, **kwargs):
        """
        Apply the plain function `f` to each value in the channel, and pipe the results to `out`.
        The function `f` will be run in a pool executor with parallelism `n`.
        The results will be put into `out` in an unspecified order: whichever result completes first will be given
        first.

        Note that even in the presence of GIL, `thread` mode is usually sufficient for achieving the greatest
        parallelism: the overhead is much lower than `process` mode, and many blocking or slow operations (e.g. file
        operations, network operations, `numpy` computations) actually release the GIL.

        If `f` involves no blocking or slow operation, consider using `async_pipe_unordered`.

        If ordering is important, consider using `parallel_pipe`.

        :param n: the parallelism of the pool executor (number of threads or number of processes).
        :param f: a plain function accepting one input value and returning one output value. Should never return `None`.
        :param out: the output channel. if `None`, one without buffer will be created and used.
        :param mode: if `thread`, a `ThreadPoolExecutor` will be used; if `process`, a `ProcessPoolExecutor` will be
                     used. Note that in the case of `process`, `f` should be a top-level function.
        :param close: whether to close the output channel when the input channel is closed.
        :param kwargs: theses will be given to the constructor of the pool executor.
        :return: the output channel.
        """
        assert mode in ('thread', 'process')
        if out is None:
            out = Chan()

        if mode == 'thread':
            executor = ThreadPoolExecutor(max_workers=n, **kwargs)
        else:
            executor = ProcessPoolExecutor(max_workers=n, **kwargs)

        results = Chan(n, loop=self.loop)

        async def job_in():
            async for v in self:
                res = self.loop.create_future()

                def wrapper(_res):
                    def put_result(rft):
                        r = rft.result()
                        self.loop.call_soon_threadsafe(functools.partial(_res.set_result, r))

                    return put_result

                ft = executor.submit(f, v)
                ft.add_done_callback(wrapper(res))
                await results.put(res)
            results.close()
            executor.shutdown(wait=False)

        async def job_out():
            async for rc in results:
                r = await rc
                if not await out.put(r):
                    break
            if close:
                out.close()

        self.loop.create_task(job_out())
        self.loop.create_task(job_in())

        return out

    def parallel_pipe_unordered(self, n, f, out=None, mode='thread', close=True, **kwargs):
        """
        Apply the plain function `f` to each value in the channel, and pipe the results to `out`.
        The function `f` will be run in a pool executor with parallelism `n`.
        The results will be processed in unspecified order but will be piped into `out` in the order of their inputs.

        Note that even in the presence of GIL, `thread` mode is usually sufficient for achieving the greatest
        parallelism: the overhead is much lower than `process` mode, and many blocking or slow operations (e.g. file
        operations, network operations, `numpy` computations) actually release the GIL.

        If `f` involves no blocking or slow operation, consider using `async_pipe`.

        If ordering is not important, consider using `parallel_pipe_unordered`.

        :param n: the parallelism of the pool executor (number of threads or number of processes).
        :param f: a plain function accepting one input value and returning one output value. Should never return `None`.
        :param out: the output channel. if `None`, one without buffer will be created and used.
        :param mode: if `thread`, a `ThreadPoolExecutor` will be used; if `process`, a `ProcessPoolExecutor` will be
                     used. Note that in the case of `process`, `f` should be a top-level function.
        :param close: whether to close the output channel when the input channel is closed.
        :param kwargs: theses will be given to the constructor of the pool executor.
        :return: the output channel.
        """
        assert mode in ('thread', 'process')
        if out is None:
            out = Chan()

        if mode == 'thread':
            executor = ThreadPoolExecutor(max_workers=n, **kwargs)
        else:
            executor = ProcessPoolExecutor(max_workers=n, **kwargs)

        activity = 1

        def finisher():
            nonlocal activity
            activity -= 1
            if activity == 0 and close:
                out.close()

        async def job_in():
            async for v in self:
                nonlocal activity
                activity += 1
                ft = executor.submit(f, v)

                def put_result(rft):
                    r = rft.result()

                    def putter():
                        out.put_nowait(r, immediate_only=False)
                        finisher()

                    self.loop.call_soon_threadsafe(putter)

                ft.add_done_callback(put_result)
            executor.shutdown(wait=False)
            finisher()

        self.loop.create_task(job_in())

        return out

    async def collect(self, n=None):
        """
        **Coroutine**. Collect the elements in the channel into a list and return the list.

        :param n: if given, will take at most `n` elements from the channel, otherwise take until channel is closed.
        :return: an awaitable containing the collected values.
        """
        result = []
        if n is None:
            async for v in self:
                result.append(v)
        else:
            for _ in range(n):
                r = await self.get()
                if r is None:
                    break
                else:
                    result.append(r)
        return result

    def to_queue(self, q=None):
        """
        Put elements from the channel onto the given queue. Useful for inter-thread communication.

        :param q: the queue. If `None`, a `queue.Queue` will be constructed.
        :return: the queue `q`.
        """
        if q is None:
            q = queue.Queue()

        async def worker():
            async for v in self:
                q.put(v)
            q.put(None)

        self.loop.create_task(worker())

        return q

    def to_iterable(self, buffer_size=None):
        """
        Return an iterable containing the values in the channel.

        This method is a convenience provided expressly for inter-thread usage. Typically, we will have an
        asyncio loop on a background thread producing values, and this method can be used as an escape hatch to
        transport the produced values back to the main thread.

        If your workflow consists entirely of operations within the asyncio loop, you should use the channel as an
        async generator directly: ``async for val in ch: ...``.

        This method should be called on the thread that attempts to use the values in the iterable, not on the
        thread on which operations involving the channel is run. The `loop` argument to the channel
        **must** be explicitly given, and should be the loop on which the channel is intended to be used.

        :param buffer_size: buffering between the iterable and the channel.
        :return: the iterable.
        """
        q = self.to_queue(queue.Queue(maxsize=buffer_size))

        def item_gen():
            while True:
                item = q.get()
                if item is None:
                    break
                else:
                    yield item

        return item_gen()

    def map(self, f, *, out=None, close=True):
        """
        Returns a channel containing `f(v)` for values `v` from the channel.

        :param close: whether `out` should be closed when there are no more values to be produced.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param f: a coroutine function receiving one element and returning one element. Cannot return `None`.
        :return: the output channel.
        """

        async def worker(inp, o):
            async for v in inp:
                if not await o.put(f(v)):
                    break
            if close:
                o.close()

        return self.async_apply(worker, out)

    def filter(self, p, *, out=None, close=True):
        """
        Returns a channel containing values `v` from the channel for which `p(v)` is true.

        :param close: whether `out` should be closed when there are no more values to be produced.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param p: a coroutine function receiving one element and returning whether this value should be kept.
        :return: the output channel.
        """

        async def worker(inp, o):
            async for v in inp:
                if p(v):
                    if not await o.put(v):
                        break
            if close:
                o.close()

        return self.async_apply(worker, out)

    def take(self, n, *, out=None, close=True):
        """
        Returns a channel containing at most `n` values from the channel.

        :param n: how many values to take.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param close: whether `out` should be closed when there are no more values to be produced.
        :return: the output channel.
        """

        async def worker(inp, o):
            ct = n
            async for v in inp:
                if not await o.put(v):
                    break
                ct -= 1
                if ct == 0:
                    break
            if close:
                o.close()

        return self.async_apply(worker, out)

    def drop(self, n, *, out=None, close=True):
        """
        Returns a channel containing values from the channel except the first `n` values.

        :param n: how many values to take.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param close: whether `out` should be closed when there are no more values to be produced.
        :return: the output channel.
        """

        async def worker(inp, o):
            ct = n
            async for v in inp:
                if ct > 0:
                    ct -= 1
                    continue
                if not await o.put(v):
                    break
            if close:
                o.close()

        return self.async_apply(worker, out)

    def take_while(self, p, *, out=None, close=True):
        """
        Returns a channel containing values `v` from the channel until `p(v)` becomes false.

        :param p: a coroutine function receiving one element and returning whether this value should be kept.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param close: whether `out` should be closed when there are no more values to be produced.
        :return: the output channel.
        """

        async def worker(inp, o):
            async for v in inp:
                if not p(v):
                    break
                if not await o.put(v):
                    break
            if close:
                o.close()

        return self.async_apply(worker, out)

    def drop_while(self, p, *, out=None, close=True):
        """
        Returns a channel containing values `v` from the channel after `p(v)` becomes false for the first time.

        :param p: a coroutine function receiving one element and returning whether this value should be dropped.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param close: whether `out` should be closed when there are no more values to be produced.
        :return: the output channel.
        """

        async def worker(inp, o):
            async for v in inp:
                if not p(v):
                    await o.put(v)
                    break

            async for v in inp:
                if not await o.put(v):
                    break
            if close:
                o.close()

        return self.async_apply(worker, out)

    def distinct(self, *, out=None, close=True):
        """
        Returns a channel containing distinct values from the channel (consecutive duplicates are dropped).

        :param out: the output channel. If `None`, one with no buffering will be created.
        :param close: whether `out` should be closed when there are no more values to be produced.
        :return: the output channel.
        """

        async def worker(inp, o):
            last = None
            async for v in inp:
                if v != last:
                    last = v
                    if not await o.put(v):
                        break
            if close:
                o.close()

        return self.async_apply(worker, out)

    def reduce(self, f, init=None, *, out=None, close=True):
        """
        Returns a channel containing the single value that is the reduce (i.e. left-fold) of the values in the channel.

        :param f: a coroutine function taking two arguments `accumulator` and `next_value` and returning
                  `new_accumulator`.
        :param init: if given, will be used as the initial accumulator. If not given, the first element in the channel
                     will be used instead.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param close: whether `out` should be closed when there are no more values to be produced.
        :return: the output channel.
        """

        async def worker(inp, o):
            if init is None:
                acc = await inp.get()
                if acc is None:
                    if close:
                        o.close()
                    return
            else:
                acc = init
            async for v in inp:
                acc = f(acc, v)
            await o.put(acc)
            if close:
                o.close()

        return self.async_apply(worker, out)

    def scan(self, f, init=None, *, out=None, close=True):
        """
        Similar to `reduce`, but all intermediate accumulators are put onto the out channel in order as well.

        :param f: a coroutine function taking two arguments `accumulator` and `next_value` and returning
                  `new_accumulator`.
        :param init: if given, will be used as the initial accumulator. If not given, the first element in the channel
                     will be used instead.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param close: whether `out` should be closed when there are no more values to be produced.
        :return: the output channel.
        """

        async def worker(inp, o):
            if init is None:
                acc = await inp.get()
                if acc is None:
                    if close:
                        o.close()
                    return
            else:
                acc = init
            await o.put(acc)
            async for v in inp:
                acc = f(acc, v)
                await o.put(acc)
            if close:
                o.close()

        return self.async_apply(worker, out)

    def debounce(self, seconds, *, out=None, close=True):
        """
        Release elements that has been put into the channel into the output channel if at least `seconds` have passed
        since any previous put operations. If the channel is closed, the last put value will be released immediately
        if it has not been released before.

        :param seconds: time since last put operations that must have passed before release
        :param out: if given, will be used as the output channel
        :param close: close the output channel when the input is closed
        :return: the output channel containing the released values
        """

        async def worker(inp, o):
            tout = Chan()
            last = None
            while True:
                el, c = await select(tout, inp)
                if c is tout:
                    tout = Chan()
                    if last is not None:
                        await o.put(last)
                        last = None
                else:
                    if el is None:
                        if last is not None:
                            await o.put(last)
                        break
                    else:
                        tout = timeout(seconds)
                        last = el
            if close:
                o.close()

        return self.async_apply(worker, out)

    def dup(self):
        """
        Create a :meth:`aiochan.channel.Dup` from the channel

        :return: the duplicator
        """
        return Dup(self)

    def pub(self,
            topic_fn=operator.itemgetter(0),
            buffer=None,
            buffer_size=None):
        """
        Create a :meth:`aiochan.channel.Pub` from the channel

        :return: the publisher
        """
        return Pub(self, topic_fn=topic_fn, buffer=buffer, buffer_size=buffer_size)


def tick_tock(seconds, immediately=True, loop=None):
    """
    Returns a channel that gives out values every `seconds`.

    The channel contains numbers from 1, counting how many ticks have been passed.

    Note that if values are not taken from the returned channel, some ticks will be skipped.

    :param immediately: if true, the first tick occurs immediately, otherwise it occurs after `seconds`.
    :param seconds: time interval of the ticks
    :param loop: you can optionally specify the loop on which the returned channel is intended to be used.
    :return: the tick channel
    """
    loop = loop or asyncio.get_event_loop()
    c = Chan(loop=loop)

    ct = 0

    async def worker():
        nonlocal ct
        if immediately:
            ct += 1
            c.put_nowait(ct, immediate_only=False)
        while True:
            await asyncio.sleep(seconds)
            ct += 1
            if c.closed:
                break
            if len(c._puts) == 0:
                c.put_nowait(ct, immediate_only=False)

    loop.create_task(worker())

    return c


class ChanIterator:
    __slots__ = ('_chan',)

    def __init__(self, chan):
        self._chan = chan

    async def __aiter__(self):
        return self

    async def __anext__(self):
        ret = await self._chan.get()
        if ret is None:
            raise StopAsyncIteration
        return ret


def timeout(seconds, loop=None):
    """
    Returns a channel that closes itself after `seconds`.

    :param seconds: time before the channel is closed
    :param loop: you can optionally specify the loop on which the returned channel is intended to be used.
    :return: the timeout channel
    """
    loop = loop or asyncio.get_event_loop()
    c = Chan(loop=loop)

    async def worker():
        await asyncio.sleep(seconds)
        c.close()

    loop.create_task(worker())

    return c


def from_iter(it, *, loop=None):
    """
    Convert an iterable into a channel.

    The channel will be closed on creation, but gets will succeed until the iterable is exhausted.

    It is ok for the iterable to be unbounded.

    :param it: the iterable to convert.
    :param loop: you can optionally specify the loop on which the returned channel is intended to be used.
    :return: the converted channel.
    """
    c = Chan(buffers.IterBuffer(it), loop=loop)
    c.close()
    return c


def from_range(start=None, end=None, step=None, *, loop=None):
    """
    returns a channel that gives out consecutive numerical values.

    If `start` is `None`, then the count goes from `0` to the maximum number that python can count.

    If `start` and `step` are given, then the values are produced as if by `itertools.count`.

    Otherwise the values are produced as if by `range`.

    :param loop: you can optionally specify the loop on which the returned channel is intended to be used.
    :return: the range channel
    """
    if start is None:
        return from_iter(itertools.count(), loop=loop)
    if end is None and step is not None:
        return from_iter(itertools.count(start, step), loop=loop)
    if step is None:
        if end is None:
            return from_iter(range(start), loop=loop)
        else:
            return from_iter(range(start, end), loop=loop)
    return from_iter(range(start, end, step), loop=loop)


def select(*chan_ops,
           priority=False,
           default=None,
           loop=None):
    """
    Asynchronously completes at most one operation in chan_ops

    :param chan_ops: operations, each is either a channel in which a get operation is attempted, or a tuple
           (chan, val) in which a put operation is attempted.
    :param priority: if True, the operations will be tried serially, else the order is random
    :param default: if not None, do not queue the operations if they cannot be completed immediately, instead return
           a future containing SelectResult(val=default, chan=None).
    :param loop: asyncio loop to run on
    :return: a function containing SelectResult(val=result, chan=succeeded_chan)
    """
    chan_ops = list(chan_ops)
    loop = loop or asyncio.get_event_loop()
    ft = loop.create_future()
    flag = SelectFlag()
    if not priority:
        random.shuffle(chan_ops)
    ret = None

    def set_result_wrap(c):
        def set_result(v):
            ft.set_result((v, c))

        return set_result

    for chan_op in chan_ops:
        if isinstance(chan_op, Chan):
            # getting
            chan = chan_op
            r = chan._get(SelectHandler(set_result_wrap(chan), flag))
            if r is not None:
                ret = (r[0], chan)
                break
        else:
            # putting
            chan, val = chan_op
            # noinspection PyProtectedMember
            r = chan._put(val, SelectHandler(set_result_wrap(chan), flag))
            if r is not None:
                ret = (r[0], chan)
                break
    if ret:
        ft.set_result(ret)
    elif default is not None and flag.active:
        flag.commit(None)
        ft.set_result((default, None))

    return ft


def merge(*inputs, out=None, close=True):
    """
    Merge the elements of the input channels into a single channel containing the individual values from the inputs.

    :param inputs: the input channels
    :param out: the output chan. If `None`, a new unbuffered channel will be used.
    :param close: whether to close `out` when all inputs are closed.
    :return: the ouput channel
    """
    out = out or Chan()

    async def worker(chs):
        while chs:
            v, c = await select(*chs)
            if v is None:
                chs.remove(c)
            else:
                if not await out.put(v):
                    break
        if close:
            out.close()

    out.loop.create_task(worker(set(inputs)))
    return out


def zip_chans(*inputs, out=None, close=True):
    """
    Merge the elements of the input channels into a single channel containing lists of individual values from the
    inputs. The input values are consumed in lockstep.

    :param inputs: the input channels
    :param out: the output chan. If `None`, a new unbuffered channel will be used.
    :param close: whether to close `out` when all inputs are closed.
    :return: the ouput channel
    """
    assert len(inputs)
    out = out or Chan()

    async def worker():
        while True:
            batch = []
            for c in inputs:
                batch.append(await c.get())
            if all(v is None for v in batch):
                out.close()
                break
            await out.put(batch)
        if close:
            out.close()

    out.loop.create_task(worker())

    return out


def combine_latest(*inputs, out, close=True):
    """
    Merge the elements of the input channels into a single channel containing lists of individual values from the
    inputs. The input values are consumed individually and each time a new value is consumed from any inputs, a
    list containing the latest values from all channels will be returned. In the list, channels that has not yet
    returned any values will have their corresponding values set to `None`.

    :param inputs: the input channels
    :param out: the output chan. If `None`, a new unbuffered channel will be used.
    :param close: whether to close `out` when all inputs are closed.
    :return: the ouput channel
    """
    assert len(inputs)
    out = out or Chan()

    async def worker():
        idxs = {c: i for i, c in enumerate(inputs)}
        actives = set(inputs)
        result = [None for _ in inputs]
        while True:
            v, c = await select(*actives)
            if v is None:
                actives.remove(c)
                if not actives:
                    out.close()
                    break
                continue
            result[idxs[c]] = v
            await out.put(result.copy())
        if close:
            out.close()

    out.loop.create_task(worker())

    return out


class Mux:
    """
    A multiplexer: similar to :meth:`aiochan.channel.merge` but allowing finer control.

    Operation modes can be specified individually for each input channel of this multiplexer, in the form of a
    set of keywords `solo`, `mute` or `pause`.

    Channels that are currently in `pause` mode will not be attempted for gets.

    At any moment when a new value is available from any of the inputs, one of the following will happen, in order:

    * if the input has `mute` attribute, its value will be silently dropped,
    * if the input has `solo` attribute, its value will be put onto the output
    * else its value will be put onto the output only if none of the input channels has the `solo` attribute. See the
      documentation for the `solo_mode` parameter for its behaviour when its values are not used.

    Multiplexers can be used as context managers that can be auto-closed on exiting the context.

    :param out: the output chan. If `None`, a new unbuffered channel will be used.
    :param solo_mode: `mute` or `pause`. If `mute`, when there are any solo-mode inputs active, other inputs will
           be muted: their values are taken but silently dropped. If `pause`, other inputs will not be attempted for
           gets at all.
    """
    __slots__ = ('_out', '_chans', '_solo_mode', '_change_chan')

    def __init__(self, out=None, solo_mode='mute'):
        assert solo_mode in ('mute', 'pause')
        out = out or Chan()
        self._change_chan = Chan()
        self._out = out
        self._solo_mode = solo_mode
        self._chans = {}
        solos = set()
        mutes = set()
        reads = set()

        def calc_state():
            nonlocal solos, mutes, reads
            solos = {c for c, v in self._chans.items() if 'solo' in v}
            mutes = {c for c, v in self._chans.items() if 'mute' in v}
            if self._solo_mode == 'pause' and solos:
                reads = solos.copy()
            else:
                reads = {c for c, v in self._chans.items() if 'pause' not in v}
            reads.add(self._change_chan)

        calc_state()

        async def worker():
            while True:
                v, c = await select(*reads)
                if c is self._change_chan:
                    if v is None:
                        break
                    calc_state()
                    continue

                if v is None:
                    self._chans.pop(c, None)
                    calc_state()
                    continue

                if c in solos or (not solos and c not in mutes):
                    if not await self._out.put(v):
                        break

        out.loop.create_task(worker())

    def _changed(self):
        self._change_chan.put_nowait(True, immediate_only=False)

    @property
    def out(self):
        """
        :return: the output channel
        """
        return self._out

    def mix(self, *inputs, modes=()):
        """
        Add channels into the multiplexer. After adding, their values will appear in the output.

        :param inputs: the channels to add
        :param modes: a set containing the attributes of the added channels.
        :return: `self`
        """
        modes = {v for v in modes if v in ('solo', 'mute', 'pause')}
        for ch in inputs:
            self._chans[ch] = modes
        self._changed()
        return self

    def unmix(self, *inputs):
        """
        Remove inputs from the multiplexer

        :param inputs: the inputs to remove
        :return: `self`
        """
        for ch in inputs:
            self._chans.pop(ch, None)
        self._changed()
        return self

    def unmix_all(self):
        """
        Remove all inputs from the multiplexer

        :return: `self`
        """
        self._chans.clear()
        self._changed()
        return self

    def solo_mode(self, mode):
        """
        Set the solo mode of the multiplexer.

        :param mode: `mute` or `pause`.
        :return: `self`
        """
        assert mode in ('mute', 'pause')
        self._solo_mode = mode
        self._changed()
        return self

    def close(self):
        """
        Close the multiplexer

        :return: `self`
        """
        self._change_chan.close()
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Dup:
    """
    A duplicator: takes values from the input, and gives out the same value to all outputs.

    Note that duplication is performed in lockstep: if any of the outputs blocks on put, the whole operation will block.
    Thus the outputs should use some buffering as appropriate for the situation.

    When there are no output channels, values from the input channels are dropped.

    Duplicators can be used as context managers.

    :param inp: the input channel
    """

    __slots__ = ('_in', '_outs', '_close_chan')

    def __init__(self, inp):
        self._in = inp
        self._outs = {}
        self._close_chan = Chan()

        async def worker():
            dchan = Chan(1)
            dctr = 0

            def done(_):
                nonlocal dctr
                dctr -= 1
                if dctr == 0:
                    dchan.put_nowait(True, immediate_only=False)

            while True:
                val, c = await select(self._close_chan, self._in, priority=True)
                if c is self._close_chan:
                    break
                if val is None:
                    for c, will_close in self._outs.items():
                        if will_close:
                            c.close()
                    break
                dctr = len(self._outs)
                for c in self._outs.keys():
                    if not c.put_nowait(val, done, immediate_only=False):
                        done(None)
                        self.untap(c)
                if self._outs:
                    await dchan.get()

        inp.loop.create_task(worker())

    @property
    def inp(self):
        """

        :return: the input channel
        """
        return self._in

    def tap(self, *outs, close=True):
        """
        add channels to the duplicator to receive duplicated values from the input.

        :param outs: the channels to add
        :param close: whether to close the added channels when the input is closed
        :return: `self`
        """
        for ch in outs:
            self._outs[ch] = close
        return self

    def untap(self, *outs):
        """
        remove output channels from the duplicator so that they will no longer receive values from the input.

        :param outs: the channels to remove
        :return: `self`
        """
        for ch in outs:
            self._outs.pop(ch, None)
        return self

    def untap_all(self):
        """
        remove all output channels from the duplicator.

        :return: `self`
        """
        self._outs.clear()
        return self

    def close(self):
        """
        Close the duplicator.

        :return: `self`
        """
        self._close_chan.close()
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Pub:
    """
    A publisher: similar to a duplicator but allowing for topic-based duplication.

    As in the case of duplicators, the duplication process for any particular topic is processed in lockstep: i.e.
    if any particular subscriber blocks on put, the whole operation is blocked. Hence buffers should be used in
    appropriate situations, either globally by setting the `buffer` and `buffer_size` parameters, or individually
    for each subscription channel.

    Publishers can be used as context managers.

    :param inp: the channel to be used as the source of the publication.
    :param topic_fn: a function accepting one argument and returning one result. This will be applied to each value
            as they come in from `inp`, and the results will be used as topics for subscription. `None` topic is
            not allowed. If `topic_fn` is `None`, will assume the values from `inp` are tuples and the first element
            in each tuple is the topic.
    :param buffer: together with `buffer_size`, will be used to determine the buffering of each topic. The acceptable
                   values are the same as for the constructor of :meth:`aiochan.channel.Chan`.
    :param buffer_size: see above
    """

    __slots__ = ('_mults', '_buffer', '_buffer_size')

    def __init__(self, inp, *, topic_fn=operator.itemgetter(0), buffer=None, buffer_size=None):
        self._buffer = buffer
        self._buffer_size = buffer_size
        self._mults = {}

        async def worker():
            while True:
                val = await inp.get()
                if val is None:
                    break

                topic = topic_fn(val)

                try:
                    m = self._mults[topic]
                except KeyError:
                    continue

                if not await m.inp.put(val):
                    self.remove_all_sub(topic)
            self.close()

        inp.loop.create_task(worker())

    def _get_mult(self, topic):
        if topic in self._mults:
            return self._mults[topic]
        else:
            ch = Chan(buffer=self._buffer, buffer_size=self._buffer_size)
            mult = Dup(ch)
            self._mults[topic] = mult
            return mult

    def add_sub(self, topic, *outs, close=True):
        """
        Subscribe `outs` to `topic`.

        :param topic: the topic to subscribe
        :param outs: the subscribing channels
        :param close: whether to close these channels when the input is closed
        :return: `self`
        """
        m = self._get_mult(topic)
        m.tap(*outs, close=close)
        return self

    def remove_sub(self, topic, *outs):
        """
        Stop the subscription of `outs` to `topic`.

        :param topic: the topic to unsubscribe from
        :param outs: the channels to unsubscribe
        :return: `self`
        """
        try:
            m = self._mults[topic]
        except KeyError:
            pass
        else:
            m.untap(*outs)
            # noinspection PyProtectedMember
            if not m._outs:
                self.remove_all_sub(topic)
        return self

    def remove_all_sub(self, topic):
        """
        Stop all subscriptions under a topic

        :param topic: the topic to stop. If `None`, all subscriptions are stopped.
        :return: `self`
        """
        m = self._mults.pop(topic, None)
        m.close()
        return self

    def close(self):
        """
        close the subscription

        :return: `self`
        """
        self._mults.clear()
        for k in list(self._mults.keys()):
            self.remove_all_sub(k)
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def go(coro, loop=None):
    """
    Spawn a coroutine in the specified loop. The loop will stop when the coroutine exits.

    :param coro: the coroutine to spawn.
    :param loop: the event loop to run the coroutine, or the current loop if `None`.
    :return: An awaitable containing the result of the coroutine.
    """
    return asyncio.ensure_future(coro, loop=loop)


def go_thread(coro, loop=None):
    """
    Spawn a coroutine in the specified loop on a background thread.  The loop will stop when the coroutine exits, and
    then the background thread will complete.

    :param coro: the coroutine to spawn.
    :param loop: the event loop to run the coroutine, or a newly created loop if `None`.
    :return: `(loop, thread)`, where `loop` is the loop on which the coroutine is run, `thread` is the thread on which
             the loop is run.
    """
    loop = loop or asyncio.new_event_loop()
    thread = threading.Thread(target=lambda _l, _c: _l.run_until_complete(_c), args=(loop, coro))
    thread.start()
    return loop, thread

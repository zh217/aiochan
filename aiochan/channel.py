import asyncio
import collections
import itertools
import multiprocessing
import multiprocessing.dummy
import numbers
import operator
import queue
import random
import threading

from . import buffers
from ._util import FnHandler, SelectFlag, SelectHandler

_buf_types = {'f': buffers.FixedLengthBuffer,
              'd': buffers.DroppingBuffer,
              's': buffers.SlidingBuffer,
              'p': buffers.PromiseBuffer}

__all__ = ('Chan', 'select', 'merge', 'from_iter', 'from_range', 'zip_chans', 'combine_latest', 'tick_tock', 'timeout',
           'Dup', 'Pub', 'go', 'nop', 'run_in_thread', 'run')

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
            self._close_event = None
        else:
            self.loop = loop or asyncio.get_event_loop()
            self._close_event = asyncio.Event(loop=loop)
        try:
            self._buf = _buf_types[buffer](buffer_size)
        except KeyError:
            if isinstance(buffer, numbers.Integral):
                self._buf = buffers.FixedLengthBuffer(buffer)
            else:
                self._buf = buffer

        self._delivered_immediate = 0
        self._delivered_buffered = 0
        self._delivered_queued = 0
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
        self._check_exhausted()

        if f is None:
            return
        elif asyncio.isfuture(f):
            f.set_result(value)
        elif asyncio.iscoroutinefunction(f):
            self.loop.create_task(f(value))
        else:
            f(value)
            # self.loop.call_soon(functools.partial(f, value))

    def _check_exhausted(self):
        if self._closed and (not len(self._puts)) and (not self._buf or not self._buf.can_take):
            self._close_event.set()

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

        # case 1: buffer available, add to buffer and then drain buffer
        if self._buf and self._buf.can_add:
            # print('put op: buffer')
            handler.commit()
            self._buf.add(val)
            while self._gets and self._buf.can_take:
                getter = self._gets.popleft()
                if getter.active:
                    self._dispatch(getter.commit(), self._buf.take())
                    self._delivered_queued += 1
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
            self._delivered_queued += 1
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
            self._check_exhausted()
            self._delivered_buffered += 1
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
            self._delivered_immediate += 1
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

    def __aiter__(self):
        return ChanIterator(self)

    def __repr__(self):
        return 'Chan<' + self._name + ' ' + str(id(self)) + '>'

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
                    self._delivered_queued += 1
            except IndexError:
                self._dirty_gets = 0
                break
        self._closed = True
        self._check_exhausted()
        return self

    @property
    def closed(self):
        """
        :return: whether this channel is already closed.
        """
        return self._closed

    def join(self):
        """
        **Coroutine**. Wait for the channel to be closed and completed exhausted.

        :return: An awaitable that will yield when the channel becomes both closed and exhausted (i.e., no buffer,
        no pending puts)
        """
        return self._close_event.wait()

    def stats(self):
        """
        Getting the current stats of the channel, useful for determining bottlenecks and debugging back pressure
        in a processing pipeline.

        :return: a `ChanStat` object `cs`, where
                 `cs.state` is `'PENDING_PUTS'`, `'PENDING_GETS'` or `'FLUENT'` according to whether the channel is
                 currently blocked on puts, blocked on gets, or not blocked (either because there is no operation going
                 on or there is buffer available), `cs.buffered`, `cs.queued`, `cs.immediate` count how many values
                 have been delivered according to whether the getter was given a buffered value, the getter was queued,
                 or the getter obtained value immediately from a pending putter.
        """
        if self._puts:
            state = 'PENDING_PUTS'
        elif self._gets:
            state = 'PENDING_GETS'
        else:
            state = 'FLUENT'

        return ChanStat(state=state,
                        buffered=self._delivered_buffered,
                        queued=self._delivered_queued,
                        immediate=self._delivered_immediate)

    async def _pipe_worker(self, out):
        async for v in self:
            if not await out.put(v):
                break
        out.close()

    def async_apply(self, f=_pipe_worker, out=None, buffer=None, buffer_size=None):
        """
        Apply a coroutine function to values in the channel, giving out an arbitrary number of results into the output
        channel and return the output value.

        :param f: a coroutine function taking two channels, `inp` and `out`. `inp` is the current channel and `out` is
                  the given or newly created out channel. The coroutine function should take elements
                  from `inp`, do its processing, and put the processed values into `out`.  When, how often and whether
                  values are put into `out`, and when or whether `out` is ever closed, is up to the coroutine.

                  If `f` is not given, an identity coroutine function which will just pass the values along and close
                  `out` when `inp` is closed is used.
        :param out: the `out` channel giving to the coroutine function `f`. If `None`, a new channel with no buffer
                  will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
        :return: the `out` channel.
        """
        if out is None:
            out = Chan(buffer, buffer_size)
        self.loop.create_task(f(self, out))
        return out

    def async_pipe(self, n, f, out=None, buffer=None, buffer_size=None, *, close=True):
        """
        Asynchronously apply the coroutine function `f` to each value in the channel, and pipe the results to `out`.
        The results will be processed in unspecified order but will be piped into `out` in the order of their inputs.

        If `f` involves slow or blocking operation, consider using `parallel_pipe`.

        If ordering is not important, consider using `async_pipe_unordered`.

        :param n: how many coroutines to spawn for processing.
        :param f: a coroutine function accepting one input value and returning one output value. S
                  hould never return `None`.
        :param out: the output channel. if `None`, one without buffer will be created and used.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
        :param close: whether to close the output channel when the input channel is closed.
        :return: the output channel.
        """
        if out is None:
            out = Chan(buffer, buffer_size)

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

    def async_pipe_unordered(self, n, f, out=None, buffer=None, buffer_size=None, *, close=True):
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
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
        :param close: whether to close the output channel when the input channel is closed.
        :return: the output channel.
        """
        if out is None:
            out = Chan(buffer, buffer_size)

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

    def parallel_pipe(self, n, f, out=None, buffer=None, buffer_size=None, close=True, flatten=False,
                      mode='process', mp_module=multiprocessing, pool_args=None,
                      pool_kwargs=None, error_cb=None, pool_buffer=None):

        """
        Apply the plain function `f` to each value in the channel, and pipe the results to `out`.
        The function `f` will be run in a pool executor with parallelism `n`.
        The results will be put into `out` in in the order that their arguments arrive.

        Note that even in the presence of GIL, `thread` mode is usually sufficient for achieving the greatest
        parallelism: the overhead is much lower than `process` mode, and many blocking or slow operations (e.g. file
        operations, network operations, `numpy` computations) actually release the GIL.

        If `f` involves no blocking or slow operation, consider using `async_pipe_unordered`.

        If ordering is important, consider using `parallel_pipe`.

        :param n: the parallelism of the pool executor (number of threads or number of processes).
        :param f: a plain function accepting one input value and returning one output value. Should never return `None`.
        :param out: the output channel. if `None`, one without buffer will be created and used.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
        :param mode: if `thread`, a `ThreadPoolExecutor` will be used; if `process`, a `Pool` will be used.
        :param close: whether to close the output channel when the input channel is closed.
        :param flatten: if `True`, assume `f` returns sequence and puts individual elements of the sequence
               onto the output channel instead
        :param mp_module: when `mode='process'`, you can optionally pass in a compatible multiprocessing module
                          (for example, `torch.multiprocessing` from pytorch).
        :param pool_args: additional arguments when creating pool
        :param pool_kwargs: additional keyword arguments when creating pool
        :param error_cb: callback in case there is an error
        :param pool_buffer: the number of jobs that can be over-committed to the pool
        :return: the output channel.
        """
        if out is None:
            out = Chan(buffer, buffer_size)

        if pool_args is None:
            pool_args = ()

        if pool_kwargs is None:
            pool_kwargs = {}

        if error_cb is None:
            def error_cb(err):
                def reraise():
                    raise err

                self.loop.call_soon_threadsafe(reraise)

        if pool_buffer is None:
            if flatten:
                pool_buffer = 0
            else:
                pool_buffer = 1

        results_chan = Chan(n, loop=self.loop)

        if mode == 'thread':
            Pool = multiprocessing.dummy.Pool
        else:
            Pool = mp_module.Pool
        pool = Pool(n, *pool_args, **pool_kwargs)

        in_flight = asyncio.Semaphore(n + pool_buffer, loop=self.loop)

        def complete_callback(ft):
            def wrapped(r):
                self.loop.call_soon_threadsafe(ft.set_result, r)

            return wrapped

        async def pipe_in_worker():
            async for data in self:
                await in_flight.acquire()
                ft = self.loop.create_future()
                pool.apply_async(f, (data,), callback=complete_callback(ft), error_callback=error_cb)
                await results_chan.put(ft)

            for i in range(n + pool_buffer):
                await in_flight.acquire()
            pool.close()
            results_chan.close()

        async def order_out_worker():
            async for async_ft in results_chan:
                item = await async_ft
                if flatten:
                    for data in item:
                        await out.put(data)
                else:
                    await out.put(item)
                in_flight.release()
            if close:
                out.close()

        self.loop.create_task(pipe_in_worker())
        self.loop.create_task(order_out_worker())

        return out

    def parallel_pipe_unordered(self, n, f, out=None, buffer=None, buffer_size=None, close=True, flatten=False,
                                mode='process', mp_module=multiprocessing, pool_args=None,
                                pool_kwargs=None, error_cb=None, pool_buffer=None):

        """
        Apply the plain function `f` to each value in the channel, and pipe the results to `out`.
        The function `f` will be run in a pool with parallelism `n`.
        The results will be processed in unspecified order but will be piped into `out` in the order of their inputs.

        Note that even in the presence of GIL, `thread` mode is usually sufficient for achieving the greatest
        parallelism: the overhead is much lower than `process` mode, and many blocking or slow operations (e.g. file
        operations, network operations, `numpy` computations) actually release the GIL.

        If `f` involves no blocking or slow operation, consider using `async_pipe`.

        :param n: the parallelism of the pool executor (number of threads or number of processes).
        :param f: a plain function accepting one input value and returning one output value. Should never return `None`.
        :param out: the output channel. if `None`, one without buffer will be created and used.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
        :param mode: if `thread`, a `ThreadPoolExecutor` will be used; if `process`, a `Pool` will be used.
        :param close: whether to close the output channel when the input channel is closed.
        :param flatten: if `True`, assume `f` returns sequence and puts individual elements of the sequence
               onto the output channel instead
        :param mp_module: when `mode='process'`, you can optionally pass in a compatible multiprocessing module
                          (for example, `torch.multiprocessing` from pytorch).
        :param pool_args: additional arguments when creating pool
        :param pool_kwargs: additional keyword arguments when creating pool
        :param error_cb: callback in case there is an error
        :param pool_buffer: the number of jobs that can be over-committed to the pool
        :return: the output channel.
        """
        if out is None:
            out = Chan(buffer, buffer_size)

        if pool_args is None:
            pool_args = ()

        if pool_kwargs is None:
            pool_kwargs = {}

        if error_cb is None:
            def error_cb(err):
                def reraise():
                    raise err

                self.loop.call_soon_threadsafe(reraise)

        if pool_buffer is None:
            if flatten:
                pool_buffer = 0
            else:
                pool_buffer = 1

        if mode == 'thread':
            Pool = multiprocessing.dummy.Pool
        else:
            Pool = mp_module.Pool
        pool = Pool(n, *pool_args, **pool_kwargs)

        in_flight = asyncio.Semaphore(n + pool_buffer, loop=self.loop)

        def complete_callback(r):
            if flatten:
                for item in r[:-1]:
                    out.put_nowait(item, immediate_only=False)
                out.put_nowait(r[-1], immediate_only=False, cb=lambda _: in_flight.release())
            else:
                out.put_nowait(r, immediate_only=False, cb=lambda _: in_flight.release())

        async def pipe_in_worker():
            async for data in self:
                await in_flight.acquire()
                pool.apply_async(f, (data,), callback=lambda r: self.loop.call_soon_threadsafe(complete_callback, r),
                                 error_callback=error_cb)

            pool.close()

            if close:
                for i in range(n + pool_buffer):
                    await in_flight.acquire()
                out.close()

        self.loop.create_task(pipe_in_worker())

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

    def to_queue(self, q):
        """
        Put elements from the channel onto the given queue. Useful for inter-thread communication.

        To be useful at all, this method should be called before running the asyncio loop::

            loop = asyncio.create_new_loop()
            chan = ac.Chan(loop=loop)
            q = chan.to_queue()
            ac.run_in_thread(some_coro(chan), loop=loop)

            # do something with the queue

        :param q: the queue.
        :return: the queue `q`.
        """

        async def worker():
            async for v in self:
                q.put(v)
            q.put(None)

        def make_task():
            self.loop.create_task(worker())

        self.loop.call_soon_threadsafe(make_task)

        return q

    def to_iterable(self, buffer_size=1):
        """
        Return an iterable containing the values in the channel.

        This method is a convenience provided expressly for inter-thread usage. Typically, we will have an
        asyncio loop on a background thread producing values, and this method can be used as an escape hatch to
        transport the produced values back to the main thread.

        If your workflow consists entirely of operations within the asyncio loop, you should use the channel as an
        async generator directly: ``async for val in ch: ...``.

        To be useful at all, this method should be called before running the asyncio loop::

            loop = asyncio.create_new_loop()
            chan = ac.Chan(loop=loop)
            it = chan.to_iterable()
            ac.run_in_thread(some_coro(chan), loop=loop)

            for item in it:
                # do something with the item

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

    def map(self, f, *, out=None, buffer=None, buffer_size=None, close=True, flatten=False):
        """
        Returns a channel containing `f(v)` for values `v` from the channel.

        :param close: whether `out` should be closed when there are no more values to be produced.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
        :param f: a function receiving one element and returning one element. Cannot return `None`.
        :param flatten: if `True`, assume `f` returns sequence and puts individual elements of the sequence
               onto the output channel instead
        :return: the output channel.
        """

        if flatten:
            async def worker(inp, o):
                async for v in inp:
                    for r in f(v):
                        if not await o.put(r):
                            break
                    if o.closed:
                        break
                if close:
                    o.close()
        else:
            async def worker(inp, o):
                async for v in inp:
                    if not await o.put(f(v)):
                        break
                if close:
                    o.close()

        return self.async_apply(worker, out, buffer=buffer, buffer_size=buffer_size)

    def filter(self, p, *, out=None, buffer=None, buffer_size=None, close=True):
        """
        Returns a channel containing values `v` from the channel for which `p(v)` is true.

        :param close: whether `out` should be closed when there are no more values to be produced.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
        :param p: a function receiving one element and returning whether this value should be kept.
        :return: the output channel.
        """

        async def worker(inp, o):
            async for v in inp:
                if p(v):
                    if not await o.put(v):
                        break
            if close:
                o.close()

        return self.async_apply(worker, out, buffer=buffer, buffer_size=buffer_size)

    def take(self, n, *, out=None, buffer=None, buffer_size=None, close=True):
        """
        Returns a channel containing at most `n` values from the channel.

        :param n: how many values to take.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
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

        return self.async_apply(worker, out, buffer=buffer, buffer_size=buffer_size)

    def drop(self, n, *, out=None, buffer=None, buffer_size=None, close=True):
        """
        Returns a channel containing values from the channel except the first `n` values.

        :param n: how many values to take.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
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

        return self.async_apply(worker, out, buffer=buffer, buffer_size=buffer_size)

    def take_while(self, p, *, out=None, buffer=None, buffer_size=None, close=True):
        """
        Returns a channel containing values `v` from the channel until `p(v)` becomes false.

        :param p: a function receiving one element and returning whether this value should be kept.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
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

        return self.async_apply(worker, out, buffer=buffer, buffer_size=buffer_size)

    def drop_while(self, p, *, out=None, buffer=None, buffer_size=None, close=True):
        """
        Returns a channel containing values `v` from the channel after `p(v)` becomes false for the first time.

        :param p: a function receiving one element and returning whether this value should be dropped.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
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

        return self.async_apply(worker, out, buffer=buffer, buffer_size=buffer_size)

    def group(self, n, out=None, buffer=None, buffer_size=None, close=True):
        """
        Returns a channel containing the elements of the source channel grouped into batches of size `n` (the last
        batch may be less than `n`).

        :param n: the size of the batch
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
        :param close: whether `out` should be closed when there are no more values to be produced.
        :return: the output channel.
        """

        async def worker(inp, o):
            batched = []
            async for v in inp:
                batched.append(v)
                if len(batched) == n:
                    await o.put(batched)
                    batched = []
            if batched:
                await o.put(batched)
            if close:
                o.close()

        return self.async_apply(worker, out, buffer=buffer, buffer_size=buffer_size)

    def group_by(self, f, out=None, buffer=None, buffer_size=None, close=True):
        """
        Returns a channel containing `(group_key, [elements...])` where `group_key` is the result of `f` applied to
        elements of the source channel and `elements ...` are consecutive elements with the same `group_key`.

        :param f: the key function
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
        :param close: whether `out` should be closed when there are no more values to be produced.
        :return: the output channel.
        """

        async def worker(inp, o):
            last_key = object()
            buffered = []
            async for v in inp:
                cur_key = f(v)
                if cur_key == last_key:
                    buffered.append(v)
                else:
                    if buffered:
                        await o.put((last_key, buffered))
                    buffered = [v]
                    last_key = cur_key
            if buffered:
                await o.put((last_key, buffered))
            if close:
                o.close()

        return self.async_apply(worker, out, buffer=buffer, buffer_size=buffer_size)

    def distinct(self, *, out=None, buffer=None, buffer_size=None, close=True):
        """
        Returns a channel containing distinct values from the channel (consecutive duplicates are dropped).

        :param out: the output channel. If `None`, one with no buffering will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
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

        return self.async_apply(worker, out, buffer=buffer, buffer_size=buffer_size)

    def reduce(self, f, init=None, *, out=None, buffer=None, buffer_size=None, close=True):
        """
        Returns a channel containing the single value that is the reduce (i.e. left-fold) of the values in the channel.

        :param f: a function taking two arguments `accumulator` and `next_value` and returning
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

        return self.async_apply(worker, out, buffer=buffer, buffer_size=buffer_size)

    def scan(self, f, init=None, *, out=None, buffer=None, buffer_size=None, close=True):
        """
        Similar to `reduce`, but all intermediate accumulators are put onto the out channel in order as well.

        :param f: a function taking two arguments `accumulator` and `next_value` and returning
                  `new_accumulator`.
        :param init: if given, will be used as the initial accumulator. If not given, the first element in the channel
                     will be used instead.
        :param out: the output channel. If `None`, one with no buffering will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
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

        return self.async_apply(worker, out, buffer=buffer, buffer_size=buffer_size)

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

    def distribute(self, *outs, close=True):
        """
        Distribute the items in this channel to the output channels. Values will not be "lost"
        due to being put to closed channels.

        :param outs: the output channels
        :param close: whether to close the output channels when the input closes
        :return: self
        """
        outs = list(outs)

        async def worker():
            async for v in self:
                if not outs:
                    break
                while True:
                    ok, c = await select(*[(o, v) for o in outs])
                    if ok:
                        break
                    else:
                        outs.remove(c)
            if close:
                for o in outs:
                    o.close()

        self.loop.create_task(worker())
        return self


def tick_tock(seconds, start_at=None, loop=None):
    """
    Returns a channel that gives out values every `seconds`.

    The channel contains tuples, in which the first elements are numbers from 1, counting how many ticks have been
    passed, and the second elements are the times at which the elements are generated.

    :param start_at: if `None`, the first tick occurs `seconds` later. If given, the first tick occurs at the given time
                     (in float).
    :param seconds: time interval of the ticks
    :param loop: you can optionally specify the loop on which the returned channel is intended to be used.
    :return: the tick channel
    """
    loop = loop or asyncio.get_event_loop()
    c = Chan(loop=loop)

    start_time = (start_at or loop.time()) + seconds

    ct = 0

    def tick():
        nonlocal ct
        ct += 1
        if c.put_nowait((ct, loop.time()), immediate_only=False) is not False:
            loop.call_at(start_time + seconds * ct, tick)

    loop.call_at(start_time, tick)

    return c


ChanStat = collections.namedtuple('ChanStat', 'state buffered queued immediate')


class ChanIterator:

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
    c = Chan(loop=loop or asyncio.get_event_loop())

    c.loop.call_later(seconds, c.close)

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
           cb=None,
           loop=None):
    """
    Asynchronously completes at most one operation in chan_ops

    :param chan_ops: operations, each is either a channel in which a get operation is attempted, or a tuple
           (chan, val) in which a put operation is attempted.
    :param priority: if True, the operations will be tried serially, else the order is random
    :param default: if not None, do not queue the operations if they cannot be completed immediately, instead return
           a future containing SelectResult(val=default, chan=None).
    :param cb:
    :param loop: asyncio loop to run on
    :return: a function containing SelectResult(val=result, chan=succeeded_chan)
    """
    chan_ops = list(chan_ops)
    if not cb:
        loop = loop or asyncio.get_event_loop()
        ft = loop.create_future()
    flag = SelectFlag()
    if not priority:
        random.shuffle(chan_ops)
    ret = None

    if not cb:
        def set_result_wrap(c):
            def set_result(v):
                ft.set_result((v, c))

            return set_result
    else:
        def set_result_wrap(c):
            def set_result(v):
                cb(v, c)

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
    if cb:
        if ret:
            cb(ret[0], ret[1])
        elif default is not None and flag.active:
            flag.commit(None)
            cb(default, None)
        return

    if ret:
        ft.set_result(ret)
    elif default is not None and flag.active:
        flag.commit(None)
        ft.set_result((default, None))

    return ft


def merge(*inputs, out=None, buffer=None, buffer_size=None, close=True):
    """
    Merge the elements of the input channels into a single channel containing the individual values from the inputs.

    :param inputs: the input channels
    :param out: the output chan. If `None`, a new unbuffered channel will be used.
    :param buffer: buffer of the internal channel, only applies if out is `None`
    :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
    :param close: whether to close `out` when all inputs are closed.
    :return: the ouput channel
    """
    out = out or Chan(buffer, buffer_size)

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


def zip_chans(*inputs, out=None, buffer=None, buffer_size=None, close=True):
    """
    Merge the elements of the input channels into a single channel containing lists of individual values from the
    inputs. The input values are consumed in lockstep.

    :param inputs: the input channels
    :param out: the output chan. If `None`, a new unbuffered channel will be used.
    :param buffer: buffer of the internal channel, only applies if out is `None`
    :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
    :param close: whether to close `out` when all inputs are closed.
    :return: the ouput channel
    """
    assert len(inputs)
    out = out or Chan(buffer, buffer_size)

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


def combine_latest(*inputs, out=None, buffer=None, buffer_size=None, close=True):
    """
    Merge the elements of the input channels into a single channel containing lists of individual values from the
    inputs. The input values are consumed individually and each time a new value is consumed from any inputs, a
    list containing the latest values from all channels will be returned. In the list, channels that has not yet
    returned any values will have their corresponding values set to `None`.

    :param inputs: the input channels
    :param out: the output chan. If `None`, a new unbuffered channel will be used.
    :param buffer: buffer of the internal channel, only applies if out is `None`
    :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
    :param close: whether to close `out` when all inputs are closed.
    :return: the ouput channel
    """
    assert len(inputs)
    out = out or Chan(buffer, buffer_size)

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


class Dup:
    """
    A duplicator: takes values from the input, and gives out the same value to all outputs.

    Note that duplication is performed in lockstep: if any of the outputs blocks on put, the whole operation will block.
    Thus the outputs should use some buffering as appropriate for the situation.

    When there are no output channels, values from the input channels are dropped.

    :param inp: the input channel
    """

    def __init__(self, inp):
        self._in = inp
        self._outs = {}
        self._close_chan = Chan()

        async def worker():
            while True:
                val, c = await select(self._close_chan, self._in, priority=True)
                if c is self._close_chan:
                    break
                if val is None:
                    for c, will_close in self._outs.items():
                        if will_close:
                            c.close()
                    break
                for c in list(self._outs.keys()):
                    if not await c.put(val):
                        self.untap(c)

        inp.loop.create_task(worker())

    @property
    def inp(self):
        """

        :return: the input channel
        """
        return self._in

    def tap(self, out=None, buffer=None, buffer_size=None, close=True):
        """
        add channels to the duplicator to receive duplicated values from the input.

        :param out: the channel to add. If `None`, an unbuffered channel will be created.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
        :param close: whether to close the added channels when the input is closed
        :return: the output channel
        """
        if out is None:
            out = Chan(buffer, buffer_size)
        self._outs[out] = close
        return out

    def untap(self, out):
        """
        remove output channels from the duplicator so that they will no longer receive values from the input.

        :param out: the channel to remove
        :return: the removed channel
        """
        self._outs.pop(out, None)
        return out

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


class Pub:
    """
    A publisher: similar to a duplicator but allowing for topic-based duplication.

    As in the case of duplicators, the duplication process for any particular topic is processed in lockstep: i.e.
    if any particular subscriber blocks on put, the whole operation is blocked. Hence buffers should be used in
    appropriate situations, either globally by setting the `buffer` and `buffer_size` parameters, or individually
    for each subscription channel.

    :param inp: the channel to be used as the source of the publication.
    :param topic_fn: a function accepting one argument and returning one result. This will be applied to each value
            as they come in from `inp`, and the results will be used as topics for subscription. `None` topic is
            not allowed. If `topic_fn` is `None`, will assume the values from `inp` are tuples and the first element
            in each tuple is the topic.
    :param buffer: together with `buffer_size`, will be used to determine the buffering of each topic. The acceptable
                   values are the same as for the constructor of :meth:`aiochan.channel.Chan`.
    :param buffer_size: see above
    """

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
                    self.unsub_all(topic)
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

    def sub(self, topic, out=None, buffer=None, buffer_size=None, close=True):
        """
        Subscribe `outs` to `topic`.

        :param topic: the topic to subscribe
        :param out: the subscribing channel. If `None`, an unbuffered channel will be used.
        :param buffer: buffer of the internal channel, only applies if out is `None`
        :param buffer_size: buffer_size of the internal channel, only applies if out is `None`
        :param close: whether to close these channels when the input is closed
        :return: the subscribing channel
        """
        if out is None:
            out = Chan(buffer, buffer_size)
        m = self._get_mult(topic)
        m.tap(out, close=close)
        return out

    def unsub(self, topic, out):
        """
        Stop the subscription of `outs` to `topic`.

        :param topic: the topic to unsubscribe from
        :param out: the channel to unsubscribe
        :return: the unsubscribing channel
        """
        try:
            m = self._mults[topic]
        except KeyError:
            pass
        else:
            m.untap(out)
            # noinspection PyProtectedMember
            if not m._outs:
                self.unsub_all(topic)
        return out

    def unsub_all(self, topic):
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
            self.unsub_all(k)
        return self


def go(coro, loop=None):
    """
    Spawn a coroutine in the specified loop. The loop will stop when the coroutine exits.

    :param coro: the coroutine to spawn.
    :param loop: the event loop to run the coroutine, or the current loop if `None`.
    :return: An awaitable containing the result of the coroutine.
    """
    return asyncio.ensure_future(coro, loop=loop)


def nop():
    """
    Useful for yielding control to the scheduler.
    :return:
    """
    return asyncio.sleep(0)


def run_in_thread(coro, loop=None):
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


def run(coro, loop=None):
    """
    Run coroutine in loop on the current thread. Will block until the coroutine is complete.

    :param coro: the coroutine to run
    :param loop: the event loop to run the coroutine, or a newly created loop if `None`.
    :return: `None`.
    """
    import concurrent.futures
    import time

    ft = concurrent.futures.Future()

    loop = loop or asyncio.new_event_loop()

    def runner():
        result = loop.run_until_complete(coro)
        ft.set_result(result)
        for task in asyncio.Task.all_tasks(loop=loop):
            task.cancel()
        time.sleep(0.1)

    thread = threading.Thread(target=runner)
    thread.start()
    return ft.result()

import asyncio
import collections
import functools
import itertools
import numbers
import operator
import queue
import random
import typing as t
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from . import buffers
from ._util import FnHandler, SelectFlag, SelectHandler

DEBUG_FLAG = False

_buf_types = {'f': buffers.FixedLengthBuffer,
              'd': buffers.DroppingBuffer,
              's': buffers.SlidingBuffer,
              'p': buffers.PromiseBuffer}

__all__ = ('Chan',
           'select',
           'merge',
           'from_iter',
           'from_range',
           'zip_chans',
           'combine_latest',
           'tick_tock',
           'Mux',
           'Dup',
           'Pub',
           'go')

MAX_OP_QUEUE_SIZE: int = 1024
"""
The maximum pending puts or pending takes for a channel.

Usually you should leave this option as it is. If you find yourself receiving exceptions due to put/get queue size
exceeding limits, you should consider using appropriate :mod:`aiochan.buffers` when creating the channels.
"""

MAX_DIRTY_SIZE: int = 256
"""
The size of cancelled operations in put/get queues before a cleanup is triggered (an operation can only become cancelled
due to the :meth:`aiochan.channel.select` or operations using it, or in other words, there is no direct user control of
cancellation).
"""


class Chan:
    """
    A channel.

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
    :param name:
    """
    __slots__ = ('loop', '_buf', '_gets', '_puts', '_closed', '_dirty_puts', '_dirty_gets', '_name')

    _count = 0

    def __init__(self,
                 buffer=None,
                 buffer_size=None,
                 *,
                 loop: t.Optional[asyncio.AbstractEventLoop] = None,
                 name: t.Optional[str] = None):
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
                f'No more than {MAX_OP_QUEUE_SIZE} pending puts are ' + \
                f'allowed on a single channel. Consider using a windowed buffer.'
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
                f'No more than {MAX_OP_QUEUE_SIZE} pending gets ' + \
                f'are allowed on a single channel'
            handler.queue(self, False)
            self._gets.append(handler)
            return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __aiter__(self):
        return _chan_aitor(self)

    def __repr__(self):
        if DEBUG_FLAG:
            return f'Chan({self._name} puts={list(self._puts)}, ' \
                   f'gets={list(self._gets)}, ' \
                   f'buffer={self._buf}, ' \
                   f'dirty={self._dirty_gets}g{self._dirty_puts}p, ' \
                   f'closed={self.closed})'
        return f'Chan<{self._name} {id(self)}>'

    def put(self, val: t.Any) -> t.Awaitable[bool]:
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

    def put_nowait(self, val: t.Any, cb: t.Optional[t.Callable] = None, *, immediate_only: bool = True) \
            -> t.Optional[bool]:
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

    def add(self, *vals: t.Any) -> 'Chan':
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

    def get(self) -> t.Awaitable[t.Optional[t.Any]]:
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

    def get_nowait(self, cb: t.Optional[t.Callable] = None, *, immediate_only: bool = True):
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

    def close(self) -> 'Chan':
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
    def closed(self) -> bool:
        """
        :return: *True* if channel is already closed, *False* otherwise.
        """
        return self._closed

    async def _pipe_worker(self, out):
        async for v in self:
            if not await out.put(v):
                break
        out.close()

    def pipe(self, out: 'Chan' = None, f: t.Callable[['Chan', 'Chan'], t.Coroutine] = _pipe_worker):
        """

        :param out:
        :param f:
        :return: self
        """
        if out is None:
            out = Chan()
        self.loop.create_task(f(self, out))
        return out

    def async_pipe(self, n, f, out, *, close=True) -> 'Chan':
        """

        :param n:
        :param f:
        :param out:
        :param close:
        :return:
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

    def parallel_pipe(self, n: int, f: t.Callable[[t.Any], t.Any], out: t.Optional['Chan'] = None,
                      mode: 'str' = 'thread', close=True, **kwargs) -> 'Chan':
        """
        note: if mode == thread, then f should be a top-level function (no closure)
        :param n:
        :param f:
        :param out:
        :param mode:
        :param kwargs:
        :return:
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

                def wrapper(res):
                    def put_result(rft):
                        r = rft.result()
                        self.loop.call_soon_threadsafe(functools.partial(res.set_result, r))

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

    def async_pipe_unordered(self, n, f, out, *, close=True):
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

    def parallel_pipe_unordered(self, n: int, f: t.Callable[[t.Any], t.Any], out: t.Optional['Chan'] = None,
                                mode: str = 'thread', close=True, **kwargs):
        """

        :param n:
        :param f:
        :param out:
        :param mode:
        :param close:
        :param kwargs:
        :return:
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

    def timeout(self, seconds: float, *values: t.Any, close: bool = True) -> 'Chan':
        """
        close chan after seconds
        :param values:
        :param close:
        :param seconds:
        :type self: Chan
        """

        def cb():
            self.add(*values)
            if close:
                self.close()

        # noinspection PyProtectedMember
        self.loop.call_later(seconds, cb)
        return self

    def dup(self) -> 'Dup':
        """

        :return:
        """
        return Dup(self)

    def pub(self,
            topic_fn: t.Callable[[t.Any], t.Any] = operator.itemgetter(0),
            buffer: t.Union[str, int, buffers.AbstractBuffer, None] = None,
            buffer_size: t.Optional[int] = None) -> 'Pub':
        """

        :param topic_fn:
        :param buffer:
        :param buffer_size:
        :return:
        """
        return Pub(self, topic_fn=topic_fn, buffer=buffer, buffer_size=buffer_size)

    async def collect(self, n: t.Optional[int] = None) -> t.List[t.Any]:
        """

        :param n:
        :return:
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

    def to_queue(self, q: queue.Queue = None) -> queue.Queue:
        """

        :param q:
        :return:
        """
        if q is None:
            q = queue.Queue()

        async def worker():
            async for v in self:
                q.put(v)
            q.put(None)

        self.loop.create_task(worker())

        return q

    def to_iterable(self, buffer_size: t.Optional[int] = None) -> t.Iterable[t.Any]:
        """

        :param buffer_size:
        :return:
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

    def __iter__(self):
        return self.to_iterable()

    def map(self, f, *, out=None, close=True):
        """

        :param close:
        :param out:
        :param f:
        :return:
        """

        async def worker(inp, o):
            async for v in inp:
                if not await o.put(f(v)):
                    break
            if close:
                o.close()

        return self.pipe(out, worker)

    def filter(self, f, *, out=None, close=True):
        """

        :param close:
        :param out:
        :param f:
        :return:
        """

        async def worker(inp, o):
            async for v in inp:
                if f(v):
                    if not await o.put(v):
                        break
            if close:
                o.close()

        return self.pipe(out, worker)

    def take(self, n, *, out=None, close=True):
        """

        :param n:
        :param close:
        :param out:
        :return:
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

        return self.pipe(out, worker)

    def take_while(self, f, *, out=None, close=True):
        """

        :param f:
        :param close:
        :param out:
        :return:
        """

        async def worker(inp, o):
            async for v in inp:
                if not f(v):
                    break
                if not await o.put(v):
                    break
            if close:
                o.close()

        return self.pipe(out, worker)

    def drop(self, n, *, out=None, close=True):
        """

        :param n:
        :param close:
        :param out:
        :return:
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

        return self.pipe(out, worker)

    def drop_while(self, f, *, out=None, close=True):
        """

        :param f:
        :param out:
        :param close:
        :return:
        """

        async def worker(inp, o):
            async for v in inp:
                if not f(v):
                    await o.put(v)
                    break

            async for v in inp:
                if not await o.put(v):
                    break
            if close:
                o.close()

        return self.pipe(out, worker)

    def distinct(self, *, out=None, close=True):
        """

        :param close:
        :param out:
        :return:
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

        return self.pipe(out, worker)

    def reduce(self, f, init=None, *, out=None, close=True):
        """

        :param close:
        :param init:
        :param f:
        :param out:
        :return:
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

        return self.pipe(out, worker)

    def scan(self, f, init=None, *, out=None, close=True):
        """

        :param close:
        :param init:
        :param f:
        :param out:
        :return:
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

        return self.pipe(out, worker)


#     def delay(self, t):
#         pass
#
#     def debounce(self, t):
#         pass
#
#     def sample(self, interval):
#         pass
#
#     def window(self, interval, fn):
#         pass
#
#     def time_interval(self):
#         pass
#
#
def tick_tock(seconds, immediately=True, loop=None):
    """

    :param immediately:
    :param seconds:
    :param loop:
    :return:
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


async def _chan_aitor(chan):
    while True:
        ret = await chan.get()
        if ret is None:
            break
        else:
            yield ret


def from_iter(it: t.Iterable, *, loop: t.Optional[asyncio.AbstractEventLoop] = None) -> Chan:
    """
    Convert an iterable into a channel.

    The channel will be closed on creation, but gets will succeed until the iterable is exhausted.

    It is ok for the iterable to be unbounded.

    :param it: the iterable to convert.
    :param loop:
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

    :param start:
    :param end:
    :param step:
    :param loop:
    :return:
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


def select(*chan_ops: t.Union[Chan, t.Tuple[Chan, t.Any]],
           priority: bool = False,
           default: t.Optional[t.Any] = None,
           loop: t.Optional[asyncio.AbstractEventLoop] = None) -> t.Awaitable[t.Tuple[t.Optional[t.Any], Chan]]:
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


def merge(*chans: Chan,
          loop: t.Optional[asyncio.AbstractEventLoop] = None,
          buffer: t.Union[str, int, buffers.AbstractBuffer, None] = None,
          buffer_size: t.Optional[int] = None) -> Chan:
    """

    :param chans:
    :param loop:
    :param buffer:
    :param buffer_size:
    :return:
    """
    loop = loop or asyncio.get_event_loop()
    out = Chan(buffer=buffer, buffer_size=buffer_size, loop=loop)

    async def worker(chs):
        while chs:
            v, c = await select(*chs)
            if v is None:
                chs.remove(c)
            else:
                if not await out.put(v):
                    break

    loop.create_task(worker(set(chans)))
    return out


def zip_chans(*chans, loop=None, buffer=None, buffer_size=None):
    """

    :param chans:
    :param loop:
    :param buffer:
    :param buffer_size:
    :return:
    """
    assert len(chans)
    out = Chan(buffer, buffer_size, loop=loop)

    async def worker():
        while True:
            batch = [await c.get() for c in chans]
            if all(v is None for v in batch):
                out.close()
                break
            await out.put(batch)

    out.loop.create_task(worker())

    return out


def combine_latest(*chans, loop=None, buffer=None, buffer_size=None):
    """

    :param chans:
    :param loop:
    :param buffer:
    :param buffer_size:
    :return:
    """
    assert len(chans)
    out = Chan(buffer, buffer_size, loop=loop)

    async def worker():
        idxs = {c: i for i, c in enumerate(chans)}
        actives = set(chans)
        result = [None for _ in chans]
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

    out.loop.create_task(worker())

    return out


class Mux:
    """
    a multiplexer
    """
    __slots__ = ('_out', '_chans', '_solo_mode', '_change_chan')

    def __init__(self, out=None, loop=None):
        self._change_chan = Chan()
        self._out = out or Chan()
        self._solo_mode = 'mute'
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

        loop = loop or asyncio.get_event_loop()
        loop.create_task(worker())

    def _changed(self):
        self._change_chan.put_nowait(True, immediate_only=False)

    @property
    def out(self):
        return self._out

    def mix(self, *chans, attrs=()):
        attrs = {v for v in attrs if v in ('solo', 'mute', 'pause')}
        for ch in chans:
            self._chans[ch] = attrs
        self._changed()
        return self

    def unmix(self, *chans):
        for ch in chans:
            self._chans.pop(ch, None)
        self._changed()
        return self

    def unmix_all(self):
        self._chans.clear()
        self._changed()
        return self

    def solo_mode(self, mode):
        assert mode in 'mute', 'solo'
        self._solo_mode = mode
        self._changed()
        return self

    def close(self):
        self._change_chan.close()
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Dup:
    """
    a duplicator
    """

    __slots__ = ('_in', '_outs', '_close_chan')

    def __init__(self, chan):
        self._in = chan
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

        chan.loop.create_task(worker())

    @property
    def inp(self):
        return self._in

    def tap(self, *chs, close_when_done=True):
        for ch in chs:
            self._outs[ch] = close_when_done
        return self

    def untap(self, *chs):
        for ch in chs:
            self._outs.pop(ch, None)
        return self

    def untap_all(self):
        self._outs.clear()
        return self

    def close(self):
        self._close_chan.close()
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Pub:
    """
    a publisher
    """

    __slots__ = ('_mults', '_buffer', '_buffer_size')

    def __init__(self, chan, *, topic_fn=operator.itemgetter(0), buffer=None, buffer_size=None):
        self._buffer = buffer
        self._buffer_size = buffer_size
        self._mults = {}

        async def worker():
            while True:
                val = await chan.get()
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

        chan.loop.create_task(worker())

    def _get_mult(self, topic):
        if topic in self._mults:
            return self._mults[topic]
        else:
            ch = Chan(buffer=self._buffer, buffer_size=self._buffer_size)
            mult = Dup(ch)
            self._mults[topic] = mult
            return mult

    def add_sub(self, topic, *chans, close_when_done=True):
        m = self._get_mult(topic)
        m.tap(*chans, close_when_done=close_when_done)
        return self

    def remove_sub(self, topic, *chans):
        try:
            m = self._mults[topic]
        except KeyError:
            pass
        else:
            m.untap(*chans)
            # noinspection PyProtectedMember
            if not m._outs:
                self.remove_all_sub(topic)
        return self

    def remove_all_sub(self, topic):
        m = self._mults.pop(topic, None)
        m.close()
        return self

    def close(self):
        self._mults.clear()
        for k in list(self._mults.keys()):
            self.remove_all_sub(k)
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def go(f, *args, loop=None, threadsafe=False, **kwargs):
    loop = loop or asyncio.get_event_loop()
    ch = Chan(loop=loop)
    if asyncio.iscoroutinefunction(f):
        async def worker():
            res = await f(*args, **kwargs)
            if res is not None:
                ch.put_nowait(res)
            ch.close()

        if threadsafe:
            asyncio.run_coroutine_threadsafe(worker(), loop)
        else:
            loop.create_task(worker())
    elif callable(f):
        def worker():
            res = f(*args, **kwargs)
            if res is not None:
                ch.put_nowait(res)
            ch.close()

        if threadsafe:
            loop.call_soon_threadsafe(worker)
        else:
            loop.call_soon(worker)
    return ch

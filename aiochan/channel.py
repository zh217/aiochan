import asyncio
import collections
import numbers
import random
import functools
from . import buffers
from .util import Box, PutBox, Handler, FnHandler, SelectFlag, SelectHandler

_buf_types = {'f': buffers.FixedLengthBuffer,
              'fixed': buffers.FixedLengthBuffer,
              'd': buffers.DroppingBuffer,
              'dropping': buffers.DroppingBuffer,
              's': buffers.SlidingBuffer,
              'sliding': buffers.SlidingBuffer,
              'p': buffers.PromiseBuffer,
              'promise': buffers.PromiseBuffer,
              None: buffers.EmptyBuffer}

MAX_OP_QUEUE_SIZE = 1024


class Chan:
    def __init__(self, buffer=None, buffer_size=None, *, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        try:
            self._buf = _buf_types[buffer](buffer_size)
        except KeyError:
            if isinstance(buffer, numbers.Integral):
                self._buf = buffers.FixedLengthBuffer(buffer_size)
            else:
                self._buf = buffer

        self._gets = collections.deque()
        self._puts = collections.deque()
        self._closed = asyncio.Event(loop=self._loop)

    @property
    def _can_get(self):
        return bool(self._gets)

    @property
    def _can_put(self):
        return bool(self._puts)

    @property
    def closed(self):
        return self._closed.is_set()

    def _dispatch(self, f, value=None):
        if f is None:
            return
        elif asyncio.isfuture(f):
            f.set_result(value)
        elif asyncio.iscoroutine(f):
            self._loop.create_task(f(value))
        else:
            self._loop.call_soon(functools.partial(f, value))

    def _clean_gets(self):
        self._gets = collections.deque(g for g in self._gets if g.active)

    def _clean_puts(self):
        self._puts = collections.deque(p for p in self._puts if p.handler.active)

    def _put(self, val, handler: Handler):
        if val is None:
            raise TypeError('Cannot put None on a channel')

        if self.closed or not handler.active:
            return Box(not self.closed)

        # case 1: buffer available, and current buffer and then drain buffer
        if self._buf.can_add:
            handler.commit()
            self._buf.add(val)
            while self._can_get and self._buf.can_take:
                getter = self._gets.popleft()
                if getter.active:
                    self._dispatch(getter.commit(), self._buf.take())
            return Box(True)

        getter = None
        while True:
            try:
                g = self._gets.popleft()
                if g and g.active:
                    getter = g
                    break
            except IndexError:
                break

        # case 2: no buffer and pending getter, dispatch immediately
        if getter is not None:
            handler.commit()
            self._dispatch(getter.commit(), val)
            return Box(True)

        # case 3: no buffer, no pending getter, queue put op if put is blockable
        if handler.blockable:
            if len(self._puts) == MAX_OP_QUEUE_SIZE:
                self._clean_puts()
                assert len(self._puts) < MAX_OP_QUEUE_SIZE, \
                    f'No more than {MAX_OP_QUEUE_SIZE} pending puts are ' + \
                    f'allowed on a single channel. Consider using a windowed buffer.'
            self._puts.append(PutBox(handler, val))
            return None

    def put_nowait(self, val, cb=None, *, immediate_only=True):
        """
        put val into the channel but do not wait.
        :param val: value to put.
        :param cb: callback to execute if the put operation is queued, passing True if the put finally succeeds, False
        if the channel is closed before put succeeds. Cannot be supplied when immediate_only is True.
        :param immediate_only: if True, do not attempt to queue the put if it cannot succeed immediately.
        :return: True if the put succeeds immediately, False if the channel is already closed, None otherwise.
        """
        if immediate_only:
            assert cb is None, 'cb must be None if immediate_only is True'
            ret = self._put(val, FnHandler(None, blockable=False))
            if ret:
                return ret.val
            else:
                return None

        ret = self._put(val, FnHandler(cb, blockable=True))
        if ret is None:
            return None

        if cb is not None:
            self._dispatch(cb, ret.val)
        return ret.val

    def put(self, val):
        """
        asynchronously put value into the channel
        :param val: the value to put
        :return: future holding the result of the put: True if the put succeeds, False if the channel is closed before
        succeeding.
        """
        ft = self._loop.create_future()
        ret = self._put(val, FnHandler(ft, blockable=True))
        if ret is not None:
            ft.set_result(ret.val)
        return ft

    def _get(self, handler: Handler):
        if not handler.active:
            return None
        elif self.closed:
            return Box(None)

        # case 1: buffer has content, return buffered value and drain puts queue
        if self._buf.can_take:
            handler.commit()
            val = self._buf.take()
            while self._buf.can_add:
                try:
                    putter = self._puts.popleft()
                    if putter.handler.active:
                        self._buf.add(putter.val)
                        self._dispatch(handler.commit(), True)
                except IndexError:
                    break
            return Box(val)

        putter = None
        while True:
            try:
                p = self._puts.popleft()
                if p.handler.active:
                    putter = p
                    break
            except IndexError:
                break

        # case 2: we have a putter immediately available
        if putter is not None:
            handler.commit()
            self._dispatch(putter.handler.commit(), True)
            return Box(putter.val)

        # case 3: cannot deal with taker immediately: queue if blockable
        if handler.blockable:
            if len(self._gets) == MAX_OP_QUEUE_SIZE:
                self._clean_gets()
                assert len(self._gets) < MAX_OP_QUEUE_SIZE, \
                    f'No more than {MAX_OP_QUEUE_SIZE} pending gets ' + \
                    f'are allowed on a single channel'
            self._puts.append(handler)
            return None

    def get_nowait(self, cb=None, *, immediate_only=True):
        """
        try to get a value from the channel but do not wait.
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
                return ret.val
            else:
                return None

        ret = self._get(FnHandler(cb, blockable=True))

        if ret is not None:
            if cb is not None:
                self._dispatch(cb, ret.val)
            return ret.val

        return None

    def get(self):
        """
        Asynchronously getting value from the channel.
        :return: a future holding the value, or None if the channel is closed before succeeding.
        """
        ft = self._loop.create_future()
        ret = self._get(FnHandler(ft, blockable=True))
        if ret is not None:
            ft.set_result(ret.val)
        return ft

    def close(self):
        if self._closed.is_set():
            return
        while True:
            try:
                getter = self._gets.popleft()
                if getter.active:
                    val = self._buf.take() if self._buf.can_take else None
                    self._dispatch(getter.commit(), val)
            except IndexError:
                break
        self._closed.set()

    def __aiter__(self):
        return _chan_aitor(self)

    async def join(self):
        await self._closed.wait()

    def __or__(self, other):
        if not isinstance(other, Chan):
            return NotImplemented
        return select(self, other, priority=True, loop=self._loop)

    def __and__(self, other):
        if not isinstance(other, Chan):
            return NotImplemented

    def pipeline(self, f, other=None, close_source=True, close_dest=True, parallelism=None, n_workers=None):
        other = other or Chan(loop=self._loop)
        # TODO
        return other

    def map(self, f):
        pass

    def flatmap(self, f):
        pass

    def groupby(self, p):
        pass

    def scan(self, p):
        pass

    def window(self, n):
        pass

    def debounce(self, seconds):
        pass

    def delay(self, seconds):
        pass

    def broadcaster(self):
        pass

    def multicaster(self):
        pass

    def duplicate(self, n):
        pass

    def enumerate(self):
        pass

    def distinct(self):
        pass

    def element_at(self):
        pass

    def first(self):
        pass

    def last(self):
        pass

    def sample(self, seconds):
        pass

    def switch(self, control):
        pass

    def filter(self, f):
        pass

    def filter_false(self, f):
        pass

    def reduce(self, f, init=None):
        pass

    def take(self, n):
        pass

    def drop(self, n):
        pass

    def take_while(self, p):
        pass

    def drop_while(self, p):
        pass

    def reduce_sum(self, p):
        pass

    def reduce_mean(self, p):
        pass

    def reduce_max(self):
        pass

    def reduce_min(self):
        pass


async def _chan_aitor(chan):
    while True:
        ret = await chan.get()
        if ret is None:
            break
        else:
            yield ret


SelectResult = collections.namedtuple('SelectResult', 'val chan')


def select(*chan_ops, priority=False, default=None, loop=None):
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
    loop = loop or asyncio.get_event_loop()
    ft = loop.create_future()
    flag = SelectFlag()
    if not priority:
        random.shuffle(chan_ops)
    ret = None
    for chan_op in chan_ops:
        if isinstance(chan_op, Chan):
            # getting
            chan = chan_op
            r = chan._get(SelectHandler(lambda v: ft.set_result(SelectResult(v, chan)), flag))
            if r is not None:
                ret = SelectResult(r.val, chan)
                break
        else:
            # putting
            chan, val = chan_op
            r = chan._put(val, SelectHandler(lambda v: ft.set_result(SelectResult(v, chan)), flag))
            if r is not None:
                ret = SelectResult(r.val, chan)
                break
    if ret:
        ft.set_result(ret)
    elif default is not None and flag.active:
        flag.commit()
        ft.set_result(SelectResult(default, None))

    return ft


def go(f, *args, _loop=None, **kwargs):
    loop = _loop or asyncio.get_event_loop()
    ch = Chan(loop=loop)
    if asyncio.iscoroutine(f):
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


def timeout_chan(seconds, loop=None):
    loop = loop or asyncio.get_event_loop()
    ch = Chan(loop=loop)
    loop.call_later(seconds, lambda: ch.close())
    return ch


def zip(*chans):
    pass


def combine(*chans):
    pass


def merge(*chans):
    pass


def concat_elements(*chans):
    pass

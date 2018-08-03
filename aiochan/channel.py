import asyncio
import collections
import functools
import numbers
import random

from . import buffers
from .util import FnHandler, SelectFlag, SelectHandler

DEBUG_FLAG = False


def put_nowait(chan, val, cb=None, *, immediate_only=True):
    """
    put val into the channel but do not wait.
    :type chan: Chan
    :param chan:
    :param val: value to put.
    :param cb: callback to execute if the put operation is queued, passing True if the put finally succeeds, False
    if the channel is closed before put succeeds. Cannot be supplied when immediate_only is True.
    :param immediate_only: if True, do not attempt to queue the put if it cannot succeed immediately.
    :return: True if the put succeeds immediately, False if the channel is already closed, None otherwise.
    """
    if immediate_only:
        assert cb is None, 'cb must be None if immediate_only is True'
        ret = chan._put(val, FnHandler(None, blockable=False))
        if ret:
            return ret[0]
        else:
            return None

    ret = chan._put(val, FnHandler(cb, blockable=True))
    if ret is None:
        return None

    if cb is not None:
        chan._dispatch(cb, ret[0])
    return ret[0]


def put(chan, val):
    """
    asynchronously put value into the channel
    :type chan: Chan
    :param chan:
    :param val: the value to put
    :return: future holding the result of the put: True if the put succeeds, False if the channel is closed before
    succeeding.
    """
    ft = chan._loop.create_future()
    ret = chan._put(val, FnHandler(ft, blockable=True))
    if ret is not None:
        ft = chan._loop.create_future()
        ft.set_result(ret[0])
    return ft


def add(chan, *vals):
    for v in vals:
        chan.put_nowait(v, immediate_only=False)
    return chan


def get_nowait(chan, cb=None, *, immediate_only=True):
    """
    try to get a value from the channel but do not wait.
    :type chan: Chan
    :param chan:
    :param cb: a callback to execute, passing in the eventual value of the get operation, which is None
    if the channel becomes closed before a value is available. Cannot be supplied when immediate_only is True.
    Note that if cb is supplied, it will be executed even when the value IS immediately available and returned
    by the function.
    :param immediate_only: do not queue the get operation if it cannot be completed immediately.
    :return: the value if available immediately, None otherwise
    """
    if immediate_only:
        assert cb is None, 'cb must be None if immediate_only is True'
        ret = chan._get(FnHandler(None, blockable=False))
        if ret:
            return ret[0]
        else:
            return None

    ret = chan._get(FnHandler(cb, blockable=True))

    if ret is not None:
        if cb is not None:
            chan._dispatch(cb, ret[0])
        return ret[0]

    return None


def get(chan):
    """
    Asynchronously getting value from the channel.
    :type chan: Chan
    :return: a future holding the value, or None if the channel is closed before succeeding.
    """
    ft = chan._loop.create_future()
    ret = chan._get(FnHandler(ft, blockable=True))
    if ret is not None:
        ft = chan._loop.create_future()
        ft.set_result(ret[0])
    return ft


def pipeline(self, f, other=None, close_source=True, close_dest=True, parallelism=None, n_workers=None):
    other = other or Chan(loop=self._loop)
    # TODO
    return other


def timeout(chan, seconds, *values, close=True):
    """
    close chan after seconds
    :param values:
    :param close:
    :param seconds:
    :type chan: Chan
    """

    def cb():
        chan.add(*values)
        if close:
            chan.close()

    # noinspection PyProtectedMember
    chan._loop.call_later(seconds, cb)
    return chan


def respond_to(chan, ctl_chan, f):
    async def runner():
        async for signal in ctl_chan:
            f(chan, signal, ctl_chan)
        f(chan, None, ctl_chan)

    chan._loop.create_task(runner())
    return chan


_buf_types = {'f': buffers.FixedLengthBuffer,
              'd': buffers.DroppingBuffer,
              's': buffers.SlidingBuffer,
              'p': buffers.PromiseBuffer}

MAX_OP_QUEUE_SIZE = 1024
MAX_DIRTY_SIZE = 256


class Chan:
    __slots__ = ('_loop', '_buf', '_gets', '_puts', '_closed', '_dirty_puts', '_dirty_gets')

    def __init__(self, buffer=None, buffer_size=None, *, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        try:
            self._buf = _buf_types[buffer](buffer_size)
        except KeyError:
            if isinstance(buffer, numbers.Integral):
                self._buf = buffers.FixedLengthBuffer(buffer)
            else:
                self._buf = buffer

        self._gets = collections.deque()
        self._puts = collections.deque()
        self._closed = asyncio.Event(loop=self._loop)
        self._dirty_puts = 0
        self._dirty_gets = 0

    def _notify_dirty(self, is_put):
        if is_put:
            self._dirty_puts += 1
        else:
            self._dirty_gets += 1

    @property
    def closed(self):
        return self._closed.is_set()

    def _dispatch(self, f, value=None):
        if f is None:
            return
        elif asyncio.isfuture(f):
            f.set_result(value)
        elif asyncio.iscoroutinefunction(f):
            self._loop.create_task(f(value))
        else:
            self._loop.call_soon(functools.partial(f, value))

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

    def close(self):
        if self._closed.is_set():
            return
        while True:
            try:
                getter = self._gets.popleft()
                if getter.active:
                    val = self._buf.take() if self._buf and self._buf.can_take else None
                    self._dispatch(getter.commit(), val)
            except IndexError:
                self._dirty_gets = 0
                break
        self._closed.set()
        return self

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

    def __repr__(self):
        if DEBUG_FLAG:
            return f'Chan(puts={list(self._puts)}, gets={list(self._gets)}, buffer={self._buf}, closed={self.closed})'
        return f'Chan<{id(self)}>'

    put_nowait = put_nowait
    put = put
    get_nowait = get_nowait
    get = get
    timeout = timeout
    add = add
    close_on = functools.partial(respond_to, f=lambda ch, sig, ctrl: ch.close())
    respond_to = respond_to


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
                ret = SelectResult(r[0], chan)
                break
        else:
            # putting
            chan, val = chan_op
            # noinspection PyProtectedMember
            r = chan._put(val, SelectHandler(lambda v: ft.set_result(SelectResult(v, chan)), flag))
            if r is not None:
                ret = SelectResult(r[0], chan)
                break
    if ret:
        ft.set_result(ret)
    elif default is not None and flag.active:
        flag.commit(None)
        ft.set_result(SelectResult(default, None))

    return ft

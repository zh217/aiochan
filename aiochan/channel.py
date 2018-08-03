import asyncio
import collections
import functools
import numbers
import operator
import random
import sys
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from . import buffers
from .util import FnHandler, SelectFlag, SelectHandler

DEBUG_FLAG = False

_buf_types = {'f': buffers.FixedLengthBuffer,
              'd': buffers.DroppingBuffer,
              's': buffers.SlidingBuffer,
              'p': buffers.PromiseBuffer}

MAX_OP_QUEUE_SIZE = 1024
MAX_DIRTY_SIZE = 256


class Chan:
    """
    a channel
    """
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
            # print('notified put', self._dirty_puts)
        else:
            self._dirty_gets += 1
            # print('notified get', self._dirty_gets)

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
            return f'Chan(puts={list(self._puts)}, ' \
                   f'gets={list(self._gets)}, ' \
                   f'buffer={self._buf}, ' \
                   f'dirty={self._dirty_gets}g{self._dirty_puts}p, ', \
                   f'closed={self.closed})'
        return f'Chan<{id(self)}>'

    def put_nowait(self, val, cb=None, *, immediate_only=True):
        """
        put val into the channel but do not wait.
        :type self: Chan
        :param self:
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
                return ret[0]
            else:
                return None

        ret = self._put(val, FnHandler(cb, blockable=True))
        if ret is None:
            return None

        if cb is not None:
            self._dispatch(cb, ret[0])
        return ret[0]

    def put(self, val):
        """
        asynchronously put value into the channel
        :type self: Chan
        :param self:
        :param val: the value to put
        :return: future holding the result of the put: True if the put succeeds, False if the channel is closed before
        succeeding.
        """
        ft = self._loop.create_future()
        ret = self._put(val, FnHandler(ft, blockable=True))
        if ret is not None:
            ft = self._loop.create_future()
            ft.set_result(ret[0])
        return ft

    def add(self, *vals):
        for v in vals:
            self.put_nowait(v, immediate_only=False)
        return self

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

    def get(self):
        """
        Asynchronously getting value from the channel.
        :type self: Chan
        :return: a future holding the value, or None if the channel is closed before succeeding.
        """
        ft = self._loop.create_future()
        ret = self._get(FnHandler(ft, blockable=True))
        if ret is not None:
            ft = self._loop.create_future()
            ft.set_result(ret[0])
        return ft

    async def _pipe_worker(self, out):
        async for v in self:
            if not await out.put(v):
                break
        out.close()

    def pipe(self, out=None, f=_pipe_worker):
        if out is None:
            out = Chan()
        self._loop.create_task(f(self, out))
        return out

    def parallel_pipe(self, n, f, out=None, mode='thread'):
        assert mode in 'thread', 'process'
        if out is None:
            out = Chan()

        if mode == 'thread':
            executor = ThreadPoolExecutor(max_workers=n)
        else:
            executor = ProcessPoolExecutor(max_workers=n)

        results = Chan(n)

        async def job_in():
            async for v in self:
                res = self._loop.create_future()
                ft = executor.submit(f, v)
                ft.add_done_callback(lambda rft: res.set_result(rft.result()))
                await results.put(res)
            executor.shutdown(wait=False)

        async def job_out():
            async for ft in results:
                if not await out.put(await ft):
                    break

        self._loop.create_task(job_out())
        self._loop.create_task(job_in())

        return out

    def parallel_pipe_unordered(self, n, f, out=None, mode='thread'):
        assert mode in 'thread', 'process'
        if out is None:
            out = Chan()

        if mode == 'thread':
            executor = ThreadPoolExecutor(max_workers=n)
        else:
            executor = ProcessPoolExecutor(max_workers=n)

        async def job_in():
            async for v in self:
                ft = executor.submit(f, v)
                ft.add_done_callback(lambda rft: out.put_nowait(rft.result(), immediate_only=False))
            executor.shutdown(wait=False)

        self._loop.create_task(job_in())

        return out

    def timeout(self, seconds, *values, close=True):
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
        self._loop.call_later(seconds, cb)
        return self

    def dup(self):
        return Dup(self)

    def pub(self, topic_fn=operator.itemgetter(0), buffer=None, buffer_size=None):
        return Pub(self, topic_fn=topic_fn, buffer=buffer, buffer_size=buffer_size)


async def _chan_aitor(chan):
    while True:
        ret = await chan.get()
        if ret is None:
            break
        else:
            yield ret


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
            r = chan._get(SelectHandler(lambda v: ft.set_result((v, chan)), flag))
            if r is not None:
                ret = (r[0], chan)
                break
        else:
            # putting
            chan, val = chan_op
            # noinspection PyProtectedMember
            r = chan._put(val, SelectHandler(lambda v: ft.set_result((v, chan)), flag))
            if r is not None:
                ret = (r[0], chan)
                break
    if ret:
        ft.set_result(ret)
    elif default is not None and flag.active:
        flag.commit(None)
        ft.set_result((default, None))

    return ft


def merge(*chans, loop=None, buffer=None, buffer_size=None):
    loop = loop or asyncio.get_event_loop()
    out = Chan(buffer=buffer, buffer_size=buffer_size, loop=loop)

    async def worker(chs):
        while chs:
            v, c = await select(*chs)
            if v is None:
                chs.remove(c)
            else:
                await out.put(v)

    loop.create_task(worker(set(chans)))
    return out


class Mux:
    """
    a multiplexer
    """
    __slots__ = ('_loop', '_out', '_chans', '_solo_mode', '_change_chan', '_solos', '_mutes', '_reads')

    def __init__(self, out=None, loop=None):
        self._change_chan = Chan()
        self._out = out or Chan()
        self._solo_mode = 'mute'
        self._chans = {}
        self._loop = loop or asyncio.get_event_loop()
        self._solos = set()
        self._mutes = set()
        self._reads = set()

        async def worker():
            while True:
                v, c = await select(*self._reads)
                if c is self._change_chan:
                    self._calc_state()
                    continue

                if v is None:
                    self._chans.pop(c, None)
                    self._calc_state()
                    continue

                if c in self._solos or (not self._solos and c not in self._mutes):
                    if not await self._out.put(v):
                        break

        self._loop.create_task(worker())

    def _calc_state(self):
        self._solos = {c for c, v in self._chans.items() if 'solo' in v}
        self._mutes = {c for c, v in self._chans.items() if 'mute' in v}
        self._reads = {self._change_chan}
        if self._solo_mode == 'pause' and self._solos:
            self._reads += self._solos
        else:
            self._reads += (c for c, v in self._chans.items() if 'pause' not in v)

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


class Dup:
    """
    a duplicator
    """

    __slots__ = ('_in', '_outs')

    def __init__(self, chan):
        self._in = chan
        self._outs = {}

        async def worker():
            dchan = Chan(1)
            dctr = 0

            def done(_):
                nonlocal dctr
                dctr -= 1
                if dctr == 0:
                    dchan.put_nowait(True, immediate_only=False)

            while True:
                val = await self._in.get()
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

        # noinspection PyProtectedMember
        chan._loop.create_task(worker())

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
                    for m in self._mults.values():
                        m.inp.close()
                    break

                # noinspection PyBroadException
                try:
                    topic = topic_fn(val)
                except Exception as ex:
                    print(ex, file=sys.stderr)
                    continue

                try:
                    m = self._mults[topic]
                except IndexError:
                    continue

                if not await m.inp.put(val):
                    self.unsub_all(topic)

        # noinspection PyProtectedMember
        chan._loop.create_task(worker())

    def _get_mult(self, topic):
        if topic in self._mults:
            return self._mults[topic]
        else:
            ch = Chan(buffer=self._buffer, buffer_size=self._buffer_size)
            mult = Dup(ch)
            self._mults[topic] = mult
            return mult

    def sub(self, topic, *chans, close_when_done=True):
        m = self._get_mult(topic)
        m.tap(*chans, close_when_done=close_when_done)
        return self

    def unsub(self, topic, *chans):
        try:
            m = self._mults[topic]
        except KeyError:
            pass
        else:
            m.untap(*chans)
            # noinspection PyProtectedMember
            if not m._outs:
                self.unsub_all(topic)
        return self

    def unsub_all(self, topic=None):
        if topic is None:
            self._mults.clear()
        else:
            self._mults.pop(topic, None)
        return self

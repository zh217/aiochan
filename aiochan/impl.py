import random
import collections
import threading
import queue
import asyncio
from asyncio import events
import numbers

XFORM_PLAIN = 'plain'
XFORM_ASYNC = 'async'
XFORM_THREAD = 'thread'
XFORM_PROCESS = 'process'

MAX_QUEUE_SIZE = 1024

Box = collections.namedtuple('Box', 'val')
PutBox = collections.namedtuple('PutBox', 'handler val')


def _dispatch(f, *args, **kwargs):
    async def af():
        return f(*args, **kwargs)

    # TODO: in Python 3.7, use loop.submit_task()
    asyncio.ensure_future(af())


class Chan:
    def __init__(self, buffer=None):
        self._takes = collections.deque()
        self._puts = collections.deque()
        if buffer and isinstance(buffer, numbers.Integral):
            if buffer > 0:
                self._buf = Buffer(buffer)
            else:
                self._buf = None
        else:
            self._buf = buffer
        self._closed = False

    def _put(self, val, handler):
        if val is None:
            raise TypeError("Can't put None on a channel")

        if self._closed or not handler.active:
            return Box(not self._closed)

        # case 1: we have buffer to use
        if self._buf and not self._buf.full:
            handler.commit()
            self._buf.add(val)
            take_cbs = []
            while len(self._takes) and not self._buf.empty:
                taker = self._takes.popleft()
                if taker.active:
                    ret = taker.commit()
                    val = self._buf.remove()
                    take_cbs.append((ret, val))
            for ret, val in take_cbs:
                _dispatch(ret, val)
            return True

        # case 2: buffer full or no buffer
        while True:
            try:
                taker = self._takes.popleft()
                if taker and taker.active:
                    break
            except IndexError:
                taker = None
                break

        # pending taker: give directly
        if taker:
            take_cb = taker.commit()
            handler.commit()
            _dispatch(take_cb, val)
            return Box(True)

        # no pending taker: if blockable, queue put op
        if handler.blockable:
            if len(self._puts) >= MAX_QUEUE_SIZE:
                raise RuntimeError(
                    'No more than {} pending puts are allowed on a single channel. Consider using a windowed buffer.'
                    % MAX_QUEUE_SIZE)
            self._puts.append(PutBox(handler, val))

    def _get(self, handler):
        # handler no longer valid: discard
        if not handler.active:
            return None

        # buffer has content
        if self._buf and not self._buf.empty:
            handler.commit()
            val = self._buf.remove()
            cbs = []
            if len(self._puts):
                while True:
                    putter = self._puts.popleft()
                    if putter.handler.active:
                        cb = putter.handler.commit()
                        cbs.append(cb)
                        self._buf.add(putter.val)
                    if self._buf.full or not len(self._puts):
                        break
            for cb in cbs:
                _dispatch(cb, True)
            return Box(val)

        # buffer empty or non-existent
        putter = None
        while True:
            try:
                p = self._puts.popleft()
                if p.handler.active:
                    putter = p
                    break
            except IndexError:
                break

        # found a pending putter
        if putter:
            put_cb = putter.handler.commit()
            handler.commit()
            _dispatch(put_cb, True)
            return Box(putter.val)

        # no pending putter, and closed
        if self._closed:
            if handler.active and handler.commit():
                if self._buf and not self._buf.empty:
                    val = self._buf.remove()
                else:
                    val = None
                return Box(val)
            else:
                return None

        # no pending putter, not closed: queue taker
        if handler.blockable:
            if len(self._takes) >= MAX_QUEUE_SIZE:
                raise RuntimeError(
                    'No more than {} pending takes are allowed on a single channel'
                    % MAX_QUEUE_SIZE)
            self._takes.append(handler)
            return None

    @property
    def closed(self):
        return self._closed

    def close(self):
        if self._closed:
            return

        self._closed = True
        while True:
            try:
                taker = self._takes.popleft()
                if taker.active:
                    take_cb = taker.commit()
                    if self._buf and not self._buf.empty:
                        val = self._buf.remove()
                    else:
                        val = None
                    _dispatch(take_cb, val)
            except IndexError:
                break

    # non-core methods

    def put_nowait(self, val, cb=None):
        ''' Asynchronously puts a `val` into channel, calling `cb` (if supplied)
        when complete, passing `False` iff channel is already closed. `None`
        values are not allowed. Returns `True` unless channel is already closed.
        '''
        if cb is None:
            handler = _fhnop
        else:
            handler = _Handler(cb)

        retbox = self._put(val, handler)

        if retbox is None:
            return True

        retval = retbox.val

        if cb is not None:
            if on_caller:
                cb(retval)
            else:
                _dispatch(cb, retval)
        return retval

    def put(self, val):
        ft = asyncio.Future()

        def cb(is_open):
            ft.set_result(is_open)

        ret = self._put(val, _Handler(cb))

        if ret is not None:
            ft = asyncio.Future()
            ft.set_result(ret.val)
        return ft

    def get_nowait(self, cb):
        ''' Asynchronously takes a val from channel, passing to `cb`. Will pass `None`
        if closed. Returns `None`.
        '''
        retbox = self._get(_Handler(cb))
        if retbox is not None:
            _dispatch(cb, retbox.val)

    def get(self):
        ft = asyncio.Future()

        def cb(v):
            ft.set_result(v)

        ret = self._get(_Handler(cb))
        if ret is not None:
            ft = asyncio.Future()
            ft.set_result(ret.val)

        return ft

    def offer(self, val):
        ret = self._put(val, _Handler(_nop, False))
        if ret:
            return ret.val

    def poll(self):
        ret = self._get(_Handler(_nop, False))
        if ret:
            return ret.val

    def __aiter__(self):
        return _chan_iterator(self)

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()


class PromiseChan(Chan):
    def __init__(self):
        super().__init__(self, _PromiseBuffer())


async def _chan_iterator(chan):
    while True:
        ret = await chan.get()
        if ret is not None:
            yield ret
        else:
            break


class _Handler:
    active = True

    @property
    def blockable():
        return self._blockable

    def __init__(self, f, blockable=True):
        self._f = f
        self._blockable = blockable

    def commit(self):
        return self._f


class _AltFlag(_Handler):

    blockable = True

    def __init__(self):
        self._flag = True

    @property
    def active(self):
        return self._flag

    def commit(self):
        self._flag = False
        return True


class _AltHandler(_Handler):

    blockable = True

    def __init__(self, flag, cb):
        self._flag = flag
        self._cb = cb

    @property
    def active(self):
        return self._flag.active

    def commit(self):
        self._flag.commit()
        return self._cb


def _nop(_):
    pass


_fhnop = _Handler(_nop)


class Pub:
    def __init__(self, ch, topic_fn, buffer_fn=None):
        pass

    def _mux_ch(self):
        pass

    def _sub(self):
        pass

    def _unsub(self):
        pass

    def _unsub_all(self, topic=None):
        pass

    def sub(self, topic, ch, close=True):
        pass

    def unsub(self, topic=None, ch=None):
        pass


class Mix:
    def __init__(self, out):
        pass

    def _admix(self, ch):
        pass

    def _unmix(self, ch):
        pass

    def _unmix_all(self, ch):
        pass

    def _toggle(self, state_dict):
        pass

    def _solo_mode(self, mode):
        pass

    def _mux_ch(self):
        pass

    def admix(self, ch):
        pass

    def unmix(self, ch=None):
        pass

    def set_solo_mode(self, mode):
        pass

    def toggle(self, state_map):
        pass


class Mult:
    def __init__(self, ch):
        pass

    def _mux_ch(self):
        pass

    def _tap(self, ch, close):
        pass

    def _untap(self, ch):
        pass

    def _untal_all(self, ch):
        pass

    def tap(self, ch, close=True):
        pass

    def untap(self, ch=None):
        pass


class Buffer:
    unblocking = False

    @property
    def full(self):
        return len(self._queue) >= self._maxsize

    @property
    def empty(self):
        return bool(len(self._queue))

    def __init__(self, maxsize):
        self._maxsize = maxsize
        self._queue = collections.deque()

    def remove(self):
        return self._queue.popleft()

    def add(self, item):
        assert item is not None
        return self._queue.append(item)

    def __len__(self):
        return len(self._queue)


class DroppingBuffer(Buffer):
    unblocking = True

    full = False

    def add(self, item):
        assert item is not None
        if len(self._queue) < self._maxsize:
            return self._queue.append(item)
        return self


class SlidingBuffer(Buffer):
    unblocking = True

    full = False

    def add(self, item):
        assert item is not None
        if len(self._queue) == self._maxsize:
            self.remove()
        self._queue.append(item)
        return self


class _PromiseBuffer(Buffer):
    unblocking = True

    full = False

    @property
    def empty(self):
        return self._val is None

    def __init__(self):
        self._val = None

    def remove(self):
        v = self._val
        self._val = None
        return v

    def add(self, item):
        assert item is not None
        self._val = item
        return self

    def __len__(self):
        if self._val is None:
            return 0
        else:
            return 1


def _alts(f_ret, ports, priority=False, default=None):
    flag = _AltFlag()
    n = len(ports)
    if not priority:
        idxs = list(range(n))
        random.shuffle(idxs)

    ret = None

    for i in range(n):
        if priority:
            idx = i
        else:
            idx = idxs[i]
        port = ports[idx]
        # for writing
        if not isinstance(port, Chan):
            port, val = port
            vbox = port._put(val, _AltHandler(flag,
                                              lambda v: f_ret((v, port))))
        # for reading
        else:
            vbox = port._get(_AltHandler(flag, lambda v: f_ret((v, port))))
        if vbox:
            ret = Box((vbox.val, port))
            break
    if ret:
        return ret
    elif default and flag.active:
        got = flag.commit()
        if got:
            return Box((default, None))


## note: alt! is too macro-heavy to be used in Python.


def select(*chan_ops, priority=False, default=None):
    '''Completes at most one of several channel operations.
  `chan_ops` is a sequence of channel operations,
  which can be either a channel to take from or a tuple of
  `(channel_to_put_to val_to_put)`, in any combination.
  Unless the `priority` option is true, if more than one operation is
  ready a non-deterministic choice will be made. If no operation is
  ready and a `default` value is supplied, `(default_val, None)` will
  be returned, otherwise `select` will wait until the first operation to
  become ready completes. Returns `(val, chan)` of the completed
  operation, where val is the value taken for getting operations, and a
  boolean (true unless already closed, as per put!) for putting operations.

    The provided `chan_ops` should not be relied for side-effects, since
    there is no guarantee whatsoever if a particular operation will be attempted
    at all, or in what order they are attempted.
    '''
    ft = asyncio.Future()

    def deliver(r):
        ft.set_result(r)

    ret = _alts(deliver, ports, priority=priority, default=default)
    if ret:
        ft = asyncio.Future()
        ft.set_result(ret.val)
    return ft


# not needed: use async iterator API
def into(coll, ch):
    pass


def map(f, chs, **kwargs):
    pass


def merge(chs, buffer=None):
    pass


def onto_chan(ch, coll, close=True):
    pass


def pipe(source,
         dest,
         pipe_fn=None,
         close_dest=True,
         close_source=True,
         mode=XFORM_PLAIN,
         parallelism=None):
    pass


def reduce(f, init, ch):
    pass


def split(p, ch, true_buffer=None, false_buffer=None):
    pass


def take(n, ch):
    pass


def timeout(msecs):
    chan = Chan()

    async def timer():
        asyncio.sleep(msecs / 1000)
        chan.close()

    asyncio.ensure_future(timer())

    return chan


def to_chan(coll):
    pass


def go(coro):
    ch = PromiseChan()

    async def worker():
        res = await coro()
        if res is None:
            ch.close()
        else:
            await ch.put(res)
            ch.close()

    _dispatch(worker)

    return ch


def thread_gen(coro):
    queue = None

    async def runner():
        chan = await coro()
        if not isinstance(chan, Chan):
            if chan is not None:
                queue.put(chan)
        else:
            async for data in chan:
                queue.put(data)
        queue.join()

    ## use threading to run queue
    for item in queue:
        if item is None:
            raise StopIteration
        else:
            yield item

    pass


_stopper = object()


def run_coroutine_in_background(coro, max_queue_size=1024, loop_factory=None):
    q = queue.Queue(maxsize=max_queue_size)

    def worker():
        if loop_factory:
            loop = loop_factory()
        else:
            loop = asyncio.new_event_loop()

        asyncio.set_event_loop(loop)
        loop.run_until_complete(coro(q))
        q.put(_stopper)

    t = threading.Thread(target=worker)
    t.start()

    def iter():
        while True:
            r = q.get()
            q.task_done()
            if r is _stopper:
                break
            yield r

    return iter()


def run_in_background(coro, max_queue_size=1024, loop_factory=None):
    async def worker(q):
        ch = await coro()
        async for item in ch:
            q.put(item)

    run_coroutine_in_background(worker, max_queue_size, loop_factory)

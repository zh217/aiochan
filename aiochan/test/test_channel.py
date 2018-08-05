import pytest
import asyncio

import aiochan.channel
import aiochan.util
from aiochan.buffers import *
from aiochan import *

aiochan.channel.DEBUG_FLAG = True


async def nop(seconds=0.0):
    await asyncio.sleep(seconds)
    return


BLOCKED = '[BLOCKED]'


def f_result(ft):
    return ft.result() if ft.done() else BLOCKED


def test_has_asyncio():
    import pytest_asyncio
    assert pytest_asyncio.__version__


def test_channel_creation():
    assert Chan()._buf is None
    assert isinstance(Chan(1)._buf, FixedLengthBuffer)
    assert isinstance(Chan('f', 1)._buf, FixedLengthBuffer)
    assert isinstance(Chan('d', 1)._buf, DroppingBuffer)
    assert isinstance(Chan('s', 1)._buf, SlidingBuffer)


@pytest.mark.asyncio
async def test_channel_closing():
    c = Chan(2)
    await c.put(1)
    assert c.put_nowait(2)
    assert 1 == await c.get()
    c.close()
    assert c.closed
    assert 2 == await c.get()

    c = Chan()
    assert c.put_nowait(1, immediate_only=False) is None
    assert c.put_nowait(2, immediate_only=False) is None
    assert 1 == await c.get()
    c.close()
    assert c.closed
    c.close()
    assert c.put_nowait(1, immediate_only=False) is False
    assert await c.get() == 2


@pytest.mark.asyncio
async def test_writes_block_on_full_buffer():
    c = Chan(1)
    await c.put(42)
    r = c.put(43)
    # await nop()
    assert f_result(r) == BLOCKED


@pytest.mark.asyncio
async def test_unfulfilled_readers_block():
    c = Chan(1)
    r1 = c.get()
    r2 = c.get()
    await c.put(42)
    r1v = f_result(r1)
    r2v = f_result(r2)
    # await nop()
    assert (r1v == BLOCKED or r2v == BLOCKED) and (r1v == 42 or r2v == 42)


@pytest.mark.asyncio
async def test_get_and_put_nowait():
    executed = asyncio.get_event_loop().create_future()
    test_chan = Chan()
    test_chan.put_nowait('test_val',
                         lambda _: executed.set_result(True),
                         immediate_only=False)
    await nop()
    assert not executed.done()
    assert 'test_val' == (await test_chan.get())
    await nop()
    assert executed.result()


@pytest.mark.asyncio
async def test_put_and_get_nowait():
    read_promise = asyncio.get_event_loop().create_future()
    test_chan = Chan()
    test_chan.get_nowait(lambda v: read_promise.set_result(v), immediate_only=False)
    await nop()
    assert not read_promise.done()
    await test_chan.put('test_val')
    await nop()
    assert read_promise.result() == 'test_val'


@pytest.mark.asyncio
async def test_limit_async_get_nowait_and_put_nowait():
    c = Chan()
    for i in range(aiochan.channel.MAX_OP_QUEUE_SIZE):
        c.put_nowait(i, immediate_only=False)
    with pytest.raises(AssertionError):
        c.put_nowait(42, immediate_only=False)

    c = Chan()
    for i in range(aiochan.channel.MAX_OP_QUEUE_SIZE):
        c.get_nowait(lambda _: _, immediate_only=False)
    with pytest.raises(AssertionError):
        c.get_nowait(lambda _: _, immediate_only=False)

    c = Chan()
    flag = aiochan.util.SelectFlag()
    for i in range(aiochan.channel.MAX_OP_QUEUE_SIZE):
        if i % 2 == 0:
            c._put(i, aiochan.util.FnHandler(None, True))
        else:
            c._put(i, aiochan.util.SelectHandler(None, flag))
    assert c._dirty_puts == 0
    flag.commit(None)
    assert c._dirty_puts == 512
    assert c.put_nowait('last', immediate_only=False) is None
    assert c._dirty_puts == 0
    c.close()
    results = []
    while True:
        r = await c.get()
        if r is None:
            break
        else:
            results.append(r)

    assert results == list(range(0, 1024, 2)) + ['last']

    c = Chan()
    flag = aiochan.util.SelectFlag()
    results = []

    def loop(i):
        if i % 2 == 0:
            c._get(aiochan.util.FnHandler(lambda v: results.append((i, v)), True))
        else:
            c._get(aiochan.util.SelectHandler(lambda v: results.append((i, v)), flag))

    for i in range(aiochan.channel.MAX_OP_QUEUE_SIZE):
        loop(i)
    assert c._dirty_gets == 0
    flag.commit(None)
    assert c._dirty_gets == 512
    assert c.get_nowait(lambda v: results.append(('end', v)), immediate_only=False) is None
    assert c._dirty_gets == 0
    for i in range(aiochan.channel.MAX_OP_QUEUE_SIZE):
        c.add(i)
    await nop()
    assert results == list(zip(list(range(0, 1024, 2)) + ['end'], range(513)))


@pytest.mark.asyncio
async def test_puts_fulfill_when_buffer_available():
    c = Chan(1)
    p = asyncio.get_event_loop().create_future()
    await c.put('full')
    c.put_nowait('enqueues',
                 lambda _: p.set_result('proceeded'),
                 immediate_only=False)
    await c.get()
    await nop()
    assert p.result() == 'proceeded'


@pytest.mark.asyncio
async def test_offer_poll():
    c = Chan(2)
    assert c.put_nowait(1)
    assert c.put_nowait(2)
    assert c.put_nowait(3) is None
    assert (await c.get()) == 1
    assert c.get_nowait() == 2
    assert c.get_nowait() is None

    c = Chan()
    assert c.put_nowait(1) is None
    assert c.get_nowait() is None


@pytest.mark.asyncio
async def test_promise_chan():
    c = Chan('p')
    t1 = c.get()
    t2 = c.get()
    assert not t1.done()
    await c.put('val')
    assert await t1 == 'val'
    assert await t2 == 'val'

    await c.put('LOST')
    assert await c.get() == 'val'
    for _ in range(3):
        assert await c.get() == 'val'
    c.close()
    await nop()
    for _ in range(3):
        assert await c.get() == 'val'


@pytest.mark.asyncio
async def test_immediate_callback():
    p = asyncio.get_event_loop().create_future()
    c = Chan().close()
    c.get_nowait(lambda v: p.set_result(v), immediate_only=False)
    await nop()
    assert p.result() is None

    p = asyncio.get_event_loop().create_future()
    c = Chan(1)
    c.put_nowait(1, lambda v: p.set_result(v), immediate_only=False)
    await nop()
    assert p.result() is True


@pytest.mark.asyncio
async def test_coroutine_dispatch():
    v = None

    async def cb(r):
        nonlocal v
        v = r

    c = Chan(1)
    c.put_nowait('val', cb, immediate_only=False)

    await nop(0.02)
    assert v is True


def test_putting_none():
    c = Chan()
    with pytest.raises(TypeError):
        c.put_nowait(None)


@pytest.mark.asyncio
async def test_timeout():
    import time

    tout = 0.02
    start = time.time()
    c = Chan().timeout(tout)
    await c.join()
    elapsed = time.time() - start
    assert elapsed >= tout
    # assert elapsed < tout * 1.05


@pytest.mark.asyncio
async def test_async_iterator():
    c = Chan().add(*range(10)).close()

    result = []
    async for v in c:
        result.append(v)

    assert result == list(range(10))


@pytest.mark.asyncio
async def test_pipe_and_list():
    c = Chan().add(*range(5)).close()
    o = c.pipe()
    assert list(range(5)) == await o.collect()

    c = Chan().add(*range(5)).close()

    async def proc(i, o):
        async for v in i:
            await o.put(v * 2)
            await o.put(v * 2 + 1)
        o.close()

    o = Chan()
    c.pipe(o, proc)
    assert list(range(0, 10)) == await o.collect()


@pytest.mark.asyncio
async def test_merge():
    in1 = Chan().add(*range(10))
    in2 = Chan().add(*range(10, 20))
    o = merge(in1, in2)
    assert set(range(20)) == set(await o.collect(20))
    o.close()
    await in1.put('x')


@pytest.mark.asyncio
async def test_mux():
    out = Chan(name='out')
    in1 = Chan(name='in1')
    in2 = Chan(name='in2')
    in1.add(1)
    in2.add(2)
    in1.add(3)
    in2.add(4)
    in1.add(5)
    in1.close()
    in2.close()
    mx = Mux(out)
    mx.mix(in1)
    mx.mix(in2)
    r = await out.collect(5)
    assert {1, 2, 3, 4, 5, 5} == set(r)
    mx.close()


@pytest.mark.asyncio
async def test_dup():
    src = Chan()
    a = Chan(4)
    b = Chan(4)
    m = src.dup()
    m.tap(a, b)
    src.add(0, 1, 2, 3)
    assert [0, 1, 2, 3] == await a.collect(4)
    assert [0, 1, 2, 3] == await b.collect(4)
    m.close()


@pytest.mark.asyncio
async def test_pub_sub():
    import numbers
    a_ints = Chan(6)
    a_strs = Chan(6)
    b_ints = Chan(6)
    b_strs = Chan(6)
    src = Chan()

    def topic(v):
        if isinstance(v, str):
            return 'string'
        elif isinstance(v, numbers.Integral):
            return 'int'
        else:
            return 'unknown'

    p = src.pub(topic)
    p.add_sub('string', a_strs, b_strs)
    p.add_sub('int', a_ints)
    p.add_sub('int', b_ints)
    src.add(1, 'a', 2, 'b', 3, 'c')
    assert [1, 2, 3] == await a_ints.collect(3)
    assert [1, 2, 3] == await b_ints.collect(3)
    assert ['a', 'b', 'c'] == await a_strs.collect(3)
    assert ['a', 'b', 'c'] == await b_strs.collect(3)
    p.remove_sub('int', a_ints)
    p.remove_sub('int', b_ints)
    src.add(4, 'd', 5, 'e', 6, 'f')
    assert ['d', 'e', 'f'] == await a_strs.collect(3)
    src.close()
    await nop()


@pytest.mark.asyncio
async def test_go():
    r = go(lambda: 1)
    assert 1 == await r.get()
    assert r.closed

    async def af(a):
        await nop()
        return a

    r = go(af, 2)
    assert 2 == await r.get()
    assert r.closed

    r = go(lambda: 1, threadsafe=True)
    assert 1 == await r.get()
    assert r.closed

    async def af(a):
        await nop()
        return a

    r = go(af, 2, threadsafe=True)
    assert 2 == await r.get()
    assert r.closed


@pytest.mark.asyncio
async def test_parallel_pipe():
    c = Chan().add(*range(10)).close()
    d = Chan()

    def work(n):
        return n * 2

    c.parallel_pipe(10, work, d)

    assert list(range(0, 20, 2)) == await d.collect(10)

    c = Chan().add(*range(100)).close()
    d = Chan()

    def work(n):
        return n * 2

    c.parallel_pipe(2, work, d)
    assert list(range(0, 200, 2)) == await d.collect(100)


@pytest.mark.asyncio
async def test_parallel_pipe_unordered():
    import time
    import random
    c = Chan().add(*range(10)).close()
    d = Chan()

    def work(n):
        time.sleep(random.uniform(0, 0.05))
        return n * 2

    c.parallel_pipe_unordered(10, work, d)

    assert set(range(0, 20, 2)) == set(await d.collect(10))

    c = Chan().add(*range(100)).close()
    d = Chan()

    def work(n):
        time.sleep(random.uniform(0, 0.0001))
        return n * 2

    c.parallel_pipe_unordered(2, work, d)
    assert set(range(0, 200, 2)) == set(await d.collect(100))


def process_work(n):
    return n * 2


@pytest.mark.asyncio
async def test_parallel_pipe_process():
    c = Chan().add(*range(10)).close()
    d = Chan()

    c.parallel_pipe(10, process_work, d, mode='process')

    assert list(range(0, 20, 2)) == await d.collect(10)

    c = Chan().add(*range(100)).close()
    d = Chan()

    def work(n):
        return n * 2

    c.parallel_pipe(2, work, d)
    assert list(range(0, 200, 2)) == await d.collect(100)


def process_slow_work(n):
    import time
    import random
    time.sleep(random.uniform(0, 0.05))
    return n * 2


@pytest.mark.asyncio
async def test_parallel_pipe_process_unordered():
    c = Chan().add(*range(10)).close()
    d = Chan()

    c.parallel_pipe_unordered(10, process_slow_work, d, mode='process')

    assert set(range(0, 20, 2)) == set(await d.collect(10))


@pytest.mark.asyncio
async def test_select_works_at_all():
    c = Chan().add(42).close()

    assert (42, c) == await select(c)


@pytest.mark.asyncio
async def test_select_default():
    c = Chan(1)

    assert (42, None) == await select(c, default=42)


@pytest.mark.asyncio
async def test_select_puts():
    f_hits = 0
    e_hits = 0
    for _ in range(100):
        f = Chan().add(1).close()
        e = Chan(1)

        r, rc = await select(f, (e, 2))
        if rc is f:
            f_hits += 1
        elif rc is e:
            e_hits += 1
        assert (rc is f and r == 1) or (rc is e and r is True)
    # there is a 2/(2^100) chance that the following assertion become false
    # even though the program is correct
    assert (f_hits > 0) and (e_hits > 0)


def test_sync_op():
    import random
    from threading import Thread

    loop = asyncio.new_event_loop()

    c = Chan(loop=loop)
    g = c.to_generator(10)

    async def work():
        for i in range(10):
            await asyncio.sleep(random.uniform(0, 0.001))
            c.add(i)
        c.close()

    def start(loop):
        asyncio.set_event_loop(loop)
        # loop.create_task(work())
        loop.run_until_complete(work())

    t = Thread(target=start, args=(loop,))
    t.start()

    assert list(range(10)) == list(g)

import asyncio
import random
import threading
import time

import pytest

import aiochan._util
import aiochan.channel
from aiochan import *
from aiochan.buffers import *

BLOCKED = '[BLOCKED]'


def f_result(ft: asyncio.Future):
    return ft.result() if ft.done() else BLOCKED


def test_has_asyncio():
    import pytest_asyncio
    assert pytest_asyncio.__version__


def test_channel_creation():
    assert Chan(loop='no_loop').loop is None
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
async def test_limit_async_get_nowait_and_put_nowait1():
    c = Chan()
    for i in range(aiochan.channel.MAX_OP_QUEUE_SIZE):
        c.put_nowait(i, immediate_only=False)
    with pytest.raises(AssertionError):
        c.put_nowait(42, immediate_only=False)


@pytest.mark.asyncio
async def test_limit_async_get_nowait_and_put_nowait2():
    c = Chan()
    for i in range(aiochan.channel.MAX_OP_QUEUE_SIZE):
        c.get_nowait(lambda _: _, immediate_only=False)
    with pytest.raises(AssertionError):
        c.get_nowait(lambda _: _, immediate_only=False)


@pytest.mark.asyncio
async def test_limit_async_get_nowait_and_put_nowait3():
    c = Chan()
    flag = aiochan._util.SelectFlag()
    for i in range(aiochan.channel.MAX_OP_QUEUE_SIZE):
        if i % 2 == 0:
            c._put(i, aiochan._util.FnHandler(None, True))
        else:
            c._put(i, aiochan._util.SelectHandler(None, flag))
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


@pytest.mark.asyncio
async def test_limit_async_get_nowait_and_put_nowait4():
    c = Chan()
    flag = aiochan._util.SelectFlag()
    results = []

    def loop(i):
        if i % 2 == 0:
            c._get(aiochan._util.FnHandler(lambda v: results.append((i, v)), True))
        else:
            c._get(aiochan._util.SelectHandler(lambda v: results.append((i, v)), flag))

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
async def test_iter():
    c = from_iter([1, 2, 3])
    r = []
    async for i in c:
        r.append(i)
    assert r == [1, 2, 3]
    assert c.closed

    c = from_range(10, 20, 2)
    assert list(range(10, 20, 2)) == await c.collect()

    c = from_range()
    assert list(range(10)) == await c.collect(10)

    c = from_range(4, -1, -1)
    assert [4, 3, 2, 1, 0] == await c.collect()


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

    await asyncio.sleep(0.02)
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
    c = timeout(tout)
    await c.get()
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
    o = c.async_apply()
    assert list(range(5)) == await o.collect()

    c = Chan().add(*range(5)).close()

    async def proc(i, o):
        async for v in i:
            await o.put(v * 2)
            await o.put(v * 2 + 1)
        o.close()

    o = Chan()
    c.async_apply(proc, o)
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
async def test_distribute():
    inputs = [Chan(name='inp%s' % i) for i in range(3)]
    from_range(20).distribute(*inputs)
    output = merge(*inputs)
    assert set(range(20)) == set(await output.collect())


@pytest.mark.asyncio
async def test_dup():
    src = Chan()
    a = Chan(4)
    b = Chan(4)
    m = src.dup()
    m.tap(a)
    m.tap(b)
    src.add(0, 1, 2, 3)
    assert [0, 1, 2, 3] == await a.collect(4)
    assert [0, 1, 2, 3] == await b.collect(4)
    m.untap_all()
    m.close()


@pytest.mark.asyncio
async def test_dup_2():
    dup = from_range(5).dup()
    inputs = [Chan(5) for i in range(3)]
    for i in inputs:
        dup.tap(i)
    for i in inputs:
        assert list(range(5)) == await i.collect()


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
        # else:
        #     return 'unknown'

    p = src.pub(topic)
    p.sub('string', a_strs)
    p.sub('string', b_strs)
    p.sub('int', a_ints)
    p.sub('int', b_ints)
    src.add(1, 'a', 2, 'b', 3, 'c')
    assert [1, 2, 3] == await a_ints.collect(3)
    assert [1, 2, 3] == await b_ints.collect(3)
    assert ['a', 'b', 'c'] == await a_strs.collect(3)
    assert ['a', 'b', 'c'] == await b_strs.collect(3)
    p.unsub('int', a_ints)
    p.unsub('int', b_ints)
    src.add(4, 'd', 5, 'e', 6, 'f')
    assert ['d', 'e', 'f'] == await a_strs.collect(3)
    src.close()
    await nop()


@pytest.mark.asyncio
async def test_go():
    async def af(a):
        await nop()
        return a

    r = go(af(2))
    assert 2 == await r


@pytest.mark.asyncio
async def test_join():
    c = Chan()
    c.put_nowait(1, immediate_only=False)
    c.close()
    assert not c._close_event.is_set()

    c = Chan(1)
    await c.put('a')
    c.close()
    assert not c._close_event.is_set()

    r = await c.get()
    assert r == 'a'
    r = await c.get()
    # await c.join()
    assert c._close_event.is_set()


@pytest.mark.asyncio
async def test_async_pipe():
    c = Chan().add(*range(10)).close()
    d = Chan()

    async def work(n):
        return n * 2

    c.async_pipe(10, work, d)

    assert list(range(0, 20, 2)) == await d.collect()


@pytest.mark.asyncio
async def test_async_pipe_big():
    c = Chan().add(*range(100)).close()
    d = Chan()

    async def work(n):
        return n * 2

    c.async_pipe(2, work, d)
    assert list(range(0, 200, 2)) == await d.collect()


@pytest.mark.asyncio
async def test_async_pipe_unordered():
    c = Chan().add(*range(100)).close()
    d = Chan()

    async def work(n):
        await asyncio.sleep(random.uniform(0, 0.05))
        return n * 2

    c.async_pipe_unordered(10, work, d)

    r = await d.collect()

    assert set(r) == set(range(0, 200, 2))
    assert r != list(range(0, 200, 2))


@pytest.mark.asyncio
async def test_parallel_pipe():
    c = Chan().add(*range(10)).close()
    d = Chan()

    def work(n):
        return n * 2

    c.parallel_pipe(10, work, d)

    assert list(range(0, 20, 2)) == await d.collect()


@pytest.mark.asyncio
async def test_parallel_pipe_big():
    c = Chan().add(*range(100)).close()
    d = Chan()

    def work(n):
        return n * 2

    c.parallel_pipe(2, work, d)
    assert list(range(0, 200, 2)) == await d.collect()


@pytest.mark.asyncio
async def test_parallel_pipe_unordered():
    c = Chan().add(*range(10)).close()
    d = Chan()

    def work(n):
        time.sleep(random.uniform(0, 0.05))
        return n * 2

    c.parallel_pipe_unordered(10, work, d)

    assert set(range(0, 20, 2)) == set(await d.collect())


@pytest.mark.asyncio
async def test_parallel_pipe_unordered_big():
    c = Chan().add(*range(100)).close()
    d = Chan()

    def work(n):
        time.sleep(random.uniform(0, 0.0001))
        return n * 2

    c.parallel_pipe_unordered(2, work, d)
    assert set(range(0, 200, 2)) == set(await d.collect())


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

        r, rc = await select(f, (e, 2), (e, 3), (e, 4))
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
    g = c.to_iterable(10)

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


_local_data = threading.local()


@pytest.mark.asyncio
async def test_fake_initializer():
    c = Chan().add(*range(100)).close()
    d = Chan()

    def work(n):
        try:
            _local_data.count += 1
        except:
            _local_data.count = 1
            _local_data.process = lambda x: x * 2
        return _local_data.count, _local_data.process(n)

    c.parallel_pipe(10, work, d)
    r = await d.collect(100)
    rv = [v[1] for v in r]
    rc = sum(v[0] for v in r)
    assert list(range(0, 200, 2)) == rv
    assert rc > 100
    assert rc < 5050


@pytest.mark.asyncio
async def test_map():
    c = from_range(10).map(lambda v: v * 2)
    assert list(range(0, 20, 2)) == await c.collect()


@pytest.mark.asyncio
async def test_reduce():
    c = from_range(101).reduce(lambda acc, v: acc + v)
    assert 5050 == await c.get()

    c = from_range(100).reduce(lambda acc, v: acc + v, 100)
    assert 5050 == await c.get()

    c = Chan().close().reduce(lambda acc, v: acc + v)
    assert (await c.get()) is None


@pytest.mark.asyncio
async def test_scan():
    c = from_range(11).scan(lambda acc, v: acc + v)
    assert [0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55] == await c.collect()

    c = from_range(11).scan(lambda acc, v: acc + v, 0)
    assert [0, 0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55] == await c.collect()

    c = Chan().close().scan(lambda acc, v: acc + v)
    assert [] == await c.collect()


@pytest.mark.asyncio
async def test_filter():
    c = from_range(100).filter(lambda v: v % 2 == 0)
    assert list(range(0, 100, 2)) == await c.collect()


@pytest.mark.asyncio
async def test_distinct():
    c = Chan().add(1, 1, 1, 1, 2, 2, 3, 3, 3, 4, 5, 5, 5).close().distinct()
    assert [1, 2, 3, 4, 5] == await c.collect()


@pytest.mark.asyncio
async def test_take():
    c = from_range()
    assert list(range(10)) == await c.take(10).collect()


@pytest.mark.asyncio
async def test_drop():
    c = from_range(10)
    assert list(range(5, 10)) == await c.drop(5).collect()


@pytest.mark.asyncio
async def test_take_while():
    c = from_range()
    assert list(range(10)) == await c.take_while(lambda v: v < 10).collect()


@pytest.mark.asyncio
async def test_drop_while():
    c = from_range(10)
    assert list(range(5, 10)) == await c.drop_while(lambda v: v < 5).collect()


@pytest.mark.asyncio
async def test_tick_tock():
    c = tick_tock(0.001)
    assert list(range(1, 11)) == await c.collect(10)
    c.close()
    await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_zip():
    a = from_iter([1, 2, 3, 4])
    b = from_iter(['a', 'b', 'c'])
    c = from_iter(['x', 'y', 'z', 'w'])
    out = zip_chans(a, b, c)
    assert [[1, 'a', 'x'], [2, 'b', 'y'], [3, 'c', 'z'], [4, None, 'w']] == await out.collect()


@pytest.mark.asyncio
async def test_combine_latest():
    a = Chan(name='a')
    b = Chan(name='b')
    c = Chan(name='c')
    out = combine_latest(a, b, c, out=Chan(1))
    await a.put(1)
    assert [1, None, None] == await out.get()
    a.close()
    await b.put(2)
    assert [1, 2, None] == await out.get()
    await c.put(3)
    assert [1, 2, 3] == await out.get()
    b.close()
    c.close()
    await nop()
    assert out.closed


def test_go_thread1():
    async def run():
        await asyncio.sleep(0)
        return

    run_in_thread(run())


def test_run():
    async def afunc():
        return 1

    assert run(afunc()) == 1


def test_without_asyncio():
    c = Chan(1, loop='no_loop')
    d = Chan(1, loop='no_loop')
    put_chan = None

    def put_cb(v, c):
        nonlocal put_chan
        put_chan = c

    select((c, 1), (d, 2), cb=put_cb)

    get_val = None

    def get_cb(v, c):
        nonlocal get_val
        get_val = v

    select(c, d, cb=get_cb)

    assert (put_chan is c and get_val == 1) or (put_chan is d and get_val == 2)

# note about parallel_pipe and mode='process':
# the following tests depends on running ProcessPoolExecutor in tandem with asyncio loop.
# these are brittle, and whether they pass or not depends on how the tests are run.
# since the logic is the same with mode='thread', we do not automatically run them.
# not to mention that they are painfully slow on Windows.

# _p_local_data = threading.local()
#
#
# def _p_work(n):
#     try:
#         _p_local_data.count += 1
#     except:
#         _p_local_data.count = 1
#         _p_local_data.process = lambda x: x * 2
#     return _p_local_data.count, _p_local_data.process(n)
#
#
# @pytest.mark.asyncio
# async def test_fake_initializer_process():
#     c = Chan().add(*range(100)).close()
#     d = Chan()
#
#     c.parallel_pipe(10, _p_work, d, mode='process')
#     r = await d.collect(100)
#     rv = [v[1] for v in r]
#     rc = sum(v[0] for v in r)
#     assert list(range(0, 200, 2)) == rv
#     assert rc > 100
#     assert rc < 5055
#
#
# def process_work(n):
#     return n * 2
#
#
# def process_slow_work(n):
#     import time
#     import random
#     time.sleep(random.uniform(0, 0.05))
#     return n * 2
#
#
# @pytest.mark.asyncio
# async def test_parallel_pipe_process_unordered():
#     c = Chan().add(*range(10)).close()
#     d = Chan()
#
#     c.parallel_pipe_unordered(10, process_slow_work, d, mode='process')
#
#     assert set(range(0, 20, 2)) == set(await d.collect())
#
#
# @pytest.mark.asyncio
# async def test_parallel_pipe_process():
#     c = Chan().add(*range(10)).close()
#     d = Chan()
#
#     c.parallel_pipe(10, process_work, d, mode='process')
#
#     assert list(range(0, 20, 2)) == await d.collect()
#
#
# @pytest.mark.asyncio
# async def test_parallel_pipe_process_big():
#     c = Chan().add(*range(100)).close()
#     d = Chan()
#
#     c.parallel_pipe(2, process_work, d, mode='process')
#     assert list(range(0, 200, 2)) == await d.collect()

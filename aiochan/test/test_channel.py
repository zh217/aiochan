import pytest

import aiochan.util
from aiochan.buffers import *
from aiochan.channel import *


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
    for i in range(MAX_OP_QUEUE_SIZE):
        c.put_nowait(i, immediate_only=False)
    with pytest.raises(AssertionError):
        c.put_nowait(42, immediate_only=False)

    c = Chan()
    for i in range(MAX_OP_QUEUE_SIZE):
        c.get_nowait(lambda _: _, immediate_only=False)
    with pytest.raises(AssertionError):
        c.get_nowait(lambda _: _, immediate_only=False)

    c = Chan()
    flag = aiochan.util.SelectFlag()
    for i in range(MAX_OP_QUEUE_SIZE):
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

    for i in range(MAX_OP_QUEUE_SIZE):
        loop(i)
    assert c._dirty_gets == 0
    flag.commit(None)
    assert c._dirty_gets == 512
    assert c.get_nowait(lambda v: results.append(('end', v)), immediate_only=False) is None
    assert c._dirty_gets == 0
    for i in range(MAX_OP_QUEUE_SIZE):
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

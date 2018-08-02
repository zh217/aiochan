import asyncio
import pytest

from ..channel import *
from ..buffers import *


def test_channel_creation():
    assert Chan()._buf is None
    assert isinstance(Chan(1)._buf, FixedLengthBuffer)
    assert isinstance(Chan('f', 1)._buf, FixedLengthBuffer)
    assert isinstance(Chan('d', 1)._buf, DroppingBuffer)
    assert isinstance(Chan('s', 1)._buf, SlidingBuffer)


@pytest.mark.asyncio
async def test_basic_channel():
    c = Chan(2)
    await c.put(1)
    assert c.put_nowait(2) is True
    r = await c.get()
    assert r == 1
    c.close()
    assert c.closed
    r = await c.get()
    assert r is None

    c = Chan()
    assert c.put_nowait(1, immediate_only=False) is None
    r = await c.get()
    assert r == 1
    c.close()
    assert c.closed
    assert c.put_nowait(1, immediate_only=False) is False

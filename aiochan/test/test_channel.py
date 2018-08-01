from ..channel import *
from ..buffers import *


def test_channel_creation():
    assert isinstance(Chan()._buf, EmptyBuffer)
    assert isinstance(Chan(1)._buf, FixedLengthBuffer)
    assert isinstance(Chan('f', 1)._buf, FixedLengthBuffer)
    assert isinstance(Chan('d', 1)._buf, DroppingBuffer)
    assert isinstance(Chan('s', 1)._buf, SlidingBuffer)
    assert isinstance(Chan('p', 1)._buf, PromiseBuffer)

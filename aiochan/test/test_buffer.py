from aiochan.buffers import *


def test_fixed_buffer():
    buffer = FixedLengthBuffer(3)

    assert buffer.can_add
    assert not buffer.can_take

    buffer.add(1)
    buffer.add(2)

    assert buffer.can_add
    assert buffer.can_take

    buffer.add(3)

    assert not buffer.can_add
    assert buffer.can_take

    assert buffer.take() == 1

    assert buffer.can_add
    assert buffer.can_take

    assert buffer.take() == 2
    assert buffer.take() == 3

    assert buffer.can_add
    assert not buffer.can_take

    assert buffer.__repr__()


def test_dropping_buffer():
    buffer = DroppingBuffer(2)

    assert buffer.can_add
    assert not buffer.can_take

    buffer.add(1)
    buffer.add(2)

    assert buffer.can_add
    assert buffer.can_take

    assert buffer.take() == 1

    buffer.add(3)
    buffer.add(4)

    assert buffer.take() == 2
    assert buffer.take() == 3

    assert buffer.can_add
    assert not buffer.can_take

    assert buffer.__repr__()


def test_sliding_buffer():
    buffer = SlidingBuffer(2)

    assert buffer.can_add
    assert not buffer.can_take

    buffer.add(1)
    buffer.add(2)

    assert buffer.can_add
    assert buffer.can_take

    assert buffer.take() == 1

    buffer.add(3)
    buffer.add(4)

    assert buffer.take() == 3
    assert buffer.take() == 4

    assert buffer.can_add
    assert not buffer.can_take

    assert buffer.__repr__()


def test_promise_buffer():
    buffer = PromiseBuffer(None)

    assert buffer.can_add
    assert not buffer.can_take

    buffer.add(1)

    assert buffer.can_add
    assert buffer.can_take

    assert buffer.take() == 1

    buffer.add(2)

    assert buffer.can_add
    assert buffer.can_take

    assert buffer.take() == 1

    assert buffer.__repr__()


def test_it_buffer():
    buffer = IterBuffer(())

    assert not buffer.can_add
    assert not buffer.can_take

    buffer = IterBuffer(range(2))

    assert not buffer.can_add
    assert buffer.can_take

    assert buffer.take() == 0

    assert not buffer.can_add
    assert buffer.can_take

    assert buffer.take() == 1

    assert not buffer.can_add
    assert not buffer.can_take

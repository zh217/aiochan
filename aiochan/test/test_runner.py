from aiochan.runner import *
from aiochan import *


async def run_fn(in_c, out_c):
    assert isinstance(in_c, Chan)
    assert isinstance(out_c, Chan)
    r = await in_c.get()
    await out_c.put(r)


def test_runner():
    with ThreadRunner(run_fn) as rn:
        rn.send(1)
        r = rn.recv()
        assert r == 1

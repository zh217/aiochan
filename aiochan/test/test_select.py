import pytest

from aiochan import *


@pytest.mark.asyncio
async def test_works_at_all():
    c = Chan().add(42).close()

    assert (42, c) == await select(c)


@pytest.mark.asyncio
async def test_default():
    c = Chan(1)

    assert (42, None) == await select(c, default=42)
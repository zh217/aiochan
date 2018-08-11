# aiochan 

[![Build Status](https://travis-ci.com/zh217/aiochan.svg?branch=master)](https://travis-ci.com/zh217/aiochan)
[![Documentation Status](https://readthedocs.org/projects/aiochan/badge/?version=latest)](https://aiochan.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/zh217/aiochan/branch/master/graph/badge.svg)](https://codecov.io/gh/zh217/aiochan)
[![PyPI version](https://img.shields.io/pypi/v/aiochan.svg)](https://pypi.python.org/pypi/sanic/)
[![PyPI version](https://img.shields.io/pypi/pyversions/aiochan.svg)](https://pypi.python.org/pypi/sanic/)
[![PyPI status](https://img.shields.io/pypi/status/aiochan.svg)](https://pypi.python.org/pypi/aiochan/)
[![GitHub license](https://img.shields.io/github/license/zh217/aiochan.svg)](https://github.com/zh217/aiochan/blob/master/LICENSE)

![logo](logo.svg "aiochan logo")


Under construction. Stay tuned.

## What's up with the logo?

It means this:

```python
async def blue_producer(c):
    while True:
        product = ... # do some hard work
        await c.put(product)

async def yellow_consumer(c):
    while True:
        result = await c.get()
        # use result to do amazing things
        
async def main():
    c = Chan()

    for _ in range(3):
        go(blue_producer(c))

    for _ in range(3):
        go(yellow_consumer(c))

    ...

```

in other words, it is a 3-fan-in on top of a 3-fan-out. If you have read the tutorial you should know what I mean.
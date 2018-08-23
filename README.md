# aiochan 

[![Build Status](https://travis-ci.com/zh217/aiochan.svg?branch=master)](https://travis-ci.com/zh217/aiochan)
[![Documentation Status](https://readthedocs.org/projects/aiochan/badge/?version=latest)](https://aiochan.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/zh217/aiochan/branch/master/graph/badge.svg)](https://codecov.io/gh/zh217/aiochan)
[![PyPI version](https://img.shields.io/pypi/v/aiochan.svg)](https://pypi.python.org/pypi/sanic/)
[![PyPI version](https://img.shields.io/pypi/pyversions/aiochan.svg)](https://pypi.python.org/pypi/sanic/)
[![PyPI status](https://img.shields.io/pypi/status/aiochan.svg)](https://pypi.python.org/pypi/aiochan/)
[![GitHub license](https://img.shields.io/github/license/zh217/aiochan.svg)](https://github.com/zh217/aiochan/blob/master/LICENSE)

![logo](logo.gif "aiochan logo")


Under heavy construction. Stay tuned.

Aiochan is a library written to bring the wonderful idiom of CSP-style concurrency to python. The implementation is based on the battle-tested Clojure library core.async, while the API is carefully crafted to feel as pythonic as possible.

## What am I getting?

* Pythonic, object-oriented API that includes everything you'd expect from a CSP library
* Fully tested
* Fully documented
* Guaranteed to work with Python 3.5.2 or above and PyPy 3.5 or above
* Depends only on python's core libraries, zero external dependencies
* Proven, efficient implementation based on Clojure's battle-tested core.async
* Familiar semantics for users of golang's channels and Clojure's core.async channels
* Flexible implementation that does not depend on the inner workings of asyncio at all
* Permissively licensed
* A beginner-friendly tutorial to get newcomers onboard as quickly as possible

## Installation

```bash
pip3 install aiochan
```

## How to use

Read the beginner-friendly tutorial that starts from the basics. Or if you are already experienced with golang or Clojure's core.async, start with the quick introduction and then dive into the API documentation.

## Examples

We have the complete set of examples from Rob Pike's concurrency patterns translated into aiochan. In addition, here is a solution to the classical dining philosophers problem.

## I still don't know how to use it

We are just starting out, but we will try to answer aiochan-related questions on stackoverflow as quickly as possible.

## I found a bug

File an issue, or if you think you can solve it, file a pull request.

## What's up with the logo?

It is our 'hello world' example:

```python
import aiochan as ac

async def blue_python(c):
    while True:
        # do some hard work
        product = "a product made by the blue python"
        await c.put(product)

async def yellow_python(c):
    while True:
        result = await c.get()
        # use result to do amazing things
        print("A yellow python has received", result)

async def main():
    c = ac.Chan()

    for _ in range(3):
        ac.go(blue_python(c))

    for _ in range(3):
        ac.go(yellow_python(c))
```

in other words, it is a 3-fan-in on top of a 3-fan-out. If you run it, you will have an endless stream of `A yellow python has received a product made by the blue python`.

If you have no idea what this is, read the tutorial.
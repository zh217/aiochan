# aiochan 

[![Build Status](https://travis-ci.com/zh217/aiochan.svg?branch=master)](https://travis-ci.com/zh217/aiochan)
[![Documentation Status](https://readthedocs.org/projects/aiochan/badge/?version=latest)](https://aiochan.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/zh217/aiochan/branch/master/graph/badge.svg)](https://codecov.io/gh/zh217/aiochan)
[![PyPI version](https://img.shields.io/pypi/v/aiochan.svg)](https://pypi.python.org/pypi/aiochan/)
[![PyPI version](https://img.shields.io/pypi/pyversions/aiochan.svg)](https://pypi.python.org/pypi/aiochan/)
[![PyPI status](https://img.shields.io/pypi/status/aiochan.svg)](https://pypi.python.org/pypi/aiochan/)
[![GitHub license](https://img.shields.io/github/license/zh217/aiochan.svg)](https://github.com/zh217/aiochan/blob/master/LICENSE)

![logo](logo.gif "aiochan logo")

Aiochan is a library written to bring the wonderful idiom of 
[CSP-style](https://en.wikipedia.org/wiki/Communicating_sequential_processes) concurrency to python. The implementation 
is based on the battle-tested Clojure library [core.async](https://github.com/clojure/core.async), while the API is 
carefully crafted to feel as pythonic as possible.

## Why?

* Doing concurrency in Python was painful
* asyncio sometimes feels too low-level
* I am constantly missing capabilities from [golang](https://golang.org) and 
  [core.async](https://github.com/clojure/core.async)
* It is much easier to port [core.async](https://github.com/clojure/core.async) to Python than to port all those
  [wonderful](http://www.numpy.org/) [python](https://pytorch.org/) [packages](https://scrapy.org/) to some other 
  language.

## What am I getting?

* Pythonic [API](https://aiochan.readthedocs.io/en/latest/api.html) that includes everything you'd need for CSP-style
  concurrency programming
* Works seamlessly with existing asyncio-based libraries
* Fully [tested](aiochan/test)
* Fully [documented](https://aiochan.readthedocs.io/en/latest/index.html)
* Guaranteed to work with Python 3.5.2 or above and PyPy 3.5 or above
* Depends only on python's core libraries, zero external dependencies
* Proven, efficient implementation based on Clojure's battle-tested [core.async](https://github.com/clojure/core.async)
* Familiar semantics for users of [golang](https://golang.org)'s channels and Clojure's core.async channels
* Flexible implementation that does not depend on the inner workings of asyncio at all
* Permissively [licensed](LICENSE)
* A [beginner-friendly tutorial](https://aiochan.readthedocs.io/en/latest/tutorial.html) to get newcomers onboard as 
quickly as possible

## How to install?

```bash
pip3 install aiochan
```

## How to use?

Read the [beginner-friendly tutorial](https://aiochan.readthedocs.io/en/latest/tutorial.html) that starts from the 
basics. Or if you are already experienced with [golang](https://golang.org) or Clojure's 
[core.async](https://github.com/clojure/core.async), start with the 
[quick introduction](https://aiochan.readthedocs.io/en/latest/quick.html) and then dive into the 
[API documentation](https://aiochan.readthedocs.io/en/latest/api.html).

## I want to try it first

The [quick introduction](https://aiochan.readthedocs.io/en/latest/quick.html) and the 
[beginner-friendly tutorial](https://aiochan.readthedocs.io/en/latest/tutorial.html) can both be run in jupyter 
notebooks, online in binders if you want (just look for ![the binder link](https://mybinder.org/static/images/badge.svg) 
at the top of each tutorial).

## Examples

In addition to the [introduction](https://aiochan.readthedocs.io/en/latest/quick.html) and the 
[tutorial](https://aiochan.readthedocs.io/en/latest/tutorial.html), we have the 
[complete set of examples](examples/concurrency_patterns) from Rob Pike's 
[Go concurrency patterns](https://www.youtube.com/watch?v=f6kdp27TYZs) translated into aiochan. Also, here is a 
[solution](examples/dining_philosophers.py) to the classical 
[dining philosophers problem](https://en.wikipedia.org/wiki/Dining_philosophers_problem).

## I still don't know how to use it

We are just starting out, but we will try to answer aiochan-related questions on 
[stackoverflow](https://stackoverflow.com/questions/ask?tags=python+python-aiochan) as quickly as possible.

## I found a bug

File an [issue](https://github.com/zh217/aiochan/issues/new), or if you think you can solve it, a pull request is even 
better.

## Do you use it in production? For what use cases?

`aiochan` is definitely not a toy and we do use it in production, mainly in the two following scenarios:

* Complex data-flow in routing.  We integrate aiochan with an asyncio-based web server.
  This should be easy to understand.
* Data-preparation piplelines.  We prepare and pre-process data to feed into our machine learning 
  algorithms as fast as possible so that our algorithms spend no time waiting for data
  to come in, but no faster than necessary so that we don't have a memory explosion due to
  data coming in faster than they can be consumed.  For this we make heavy use of
  [parallel_pipe](https://aiochan.readthedocs.io/en/latest/api.html#aiochan.channel.Chan.parallel_pipe)
  and [parallel_pipe_unordered](https://aiochan.readthedocs.io/en/latest/api.html#aiochan.channel.Chan.parallel_pipe_unordered).
  Currently we are not aware of any other library that can completely satisfy this need of ours.

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

in other words, it is a 3-fan-in on top of a 3-fan-out. If you run it, you will have an endless stream of 
`A yellow python has received a product made by the blue python`.

If you have no idea what this is, read the [tutorial](https://aiochan.readthedocs.io/en/latest/tutorial.html).
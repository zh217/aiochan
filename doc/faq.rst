FAQ
===

Is aiochan original?
    Almost *nothing* in aiochan is original. It is not intended to be original.

    The theory is of course laid out long ago, for example see the CSP paper and the Dijkstra paper.

    The API is mostly inspired by Golang and Clojure's core.async, with a few pythonic touches here and there.

    The implementation of the core logic of channels is mostly a faithful port of Clojure's core.async. But then you
    can have confidence in the correctness of results that aiochan gives, since core.async *is* battle-tested.
    Writing concurrency libraries *from scratch* is difficult, and writing *correct* ones *from scratch* takes many
    iterations and an unreasonable number of hours.

    Aiochan *is* intended to be *unoriginal*, *useful*, *productive*, and *fun to use*.

If you like CSP, why don't you use *Golang*/*core.async*/*elixir*/*erlang* ... directly?
    Well, we do use these things.

    But often we find ourselves in the situation where python is the most natural language. However, without a
    high level concurrency control library, our code suffers from the following symptoms:

    * brittle and impossible to understand due to ad-hoc non-deterministic callback hell;
    * slow and idle running time due to single threaded code without any concurrent or parallel execution;
    * slow and frustrating developing time due to drowning in locks, semaphores, etc.

    Well, aiochan definitely cured these for us. And perhaps you will agree with me that porting core.async to python's
    asyncio is much easier than porting numpy/scipy/scikit-learn/pytorch/tensorflow ... to some other language, or
    finding usable alternative thereof. Coercing libraries to behave well when unsuspectingly manipulated by a foreign
    language is agonizing.

How do I handle exceptions?
    Unfortunately, in aiochan we assume that whatever goes into channels and comes out of channels won't cause
    exceptions to be raised during the processing of these values. If your processing functions (for example, those
    passed into `async_apply` or `parallel_pipe` may raise exceptions, you should catch and handle them in your
    functions (and maybe return some "exception-values").

    Let me explain why aiochan won't handle exceptions for you. First, when consuming values, we encourage you to
    use the `async for` syntax::

        async for v in chan:
            # do some processing

    Now if an exception is thrown when getting values out of `chan`, it is not clear how you can recover the async
    iteration, even if you catch the exception.

    Another example: say you have some `async_apply` workflow::

        out_chan = in_chan.async_apply(async_f)

    Now for some values, `async_f` raises an exception. Now where should this exception go? `in_chan.put`?
    `outchan.get`? What happens if there is buffering for `in_chan` or for `out_chan`? What if the buffering is
    of the sliding or the dropping type? Things get very hairy with exceptions and async processing, so we encourage
    you to deal with them in the synchronous parts of your code.
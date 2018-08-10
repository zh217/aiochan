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
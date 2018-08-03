def respond_to(chan, ctl_chan, f):
    async def runner():
        async for signal in ctl_chan:
            f(chan, signal, ctl_chan)
        f(chan, None, ctl_chan)

    # noinspection PyProtectedMember
    chan._loop.create_task(runner())
    return chan

# close_on = functools.partial(respond_to, f=lambda ch, sig, ctrl: ch.close())
# respond_to = respond_to

class FnHandler:
    __slots__ = ('_f', '_blockable')
    active = True

    @property
    def blockable(self):
        return self._blockable

    def __init__(self, f, blockable=True):
        self._f = f
        self._blockable = blockable

    def queue(self, chan, is_put):
        pass

    def commit(self):
        return self._f


class SelectFlag:
    __slots__ = ('active', '_handlers')

    def __init__(self):
        self._handlers = []
        self.active = True

    def set(self, handler):
        self._handlers.append(handler)

    def commit(self, handler):
        for h in self._handlers:
            if h is not handler:
                h.notify_inactive()
        del self._handlers
        self.active = False


class SelectHandler:
    __slots__ = ('active', '_f', '_flag', '_chan', '_type')
    blockable = True

    def __init__(self, f, flag):
        flag.set(self)
        self.active = True
        self._f = f
        self._flag = flag
        self._chan = None
        self._type = None

    def notify_inactive(self):
        # noinspection PyProtectedMember
        self._chan._notify_dirty(self._type)
        self.active = False

    def queue(self, chan, is_put):
        self._chan = chan
        self._type = is_put

    def commit(self):
        self._flag.commit(self)
        return self._f

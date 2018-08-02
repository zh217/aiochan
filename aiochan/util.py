class FnHandler:
    __slots__ = ('_f', '_blockable')
    active = True

    @property
    def blockable(self):
        return self._blockable

    def __init__(self, f, blockable=True):
        self._f = f
        self._blockable = blockable

    def commit(self):
        return self._f


class SelectFlag:
    __slots__ = ('active',)

    def __init__(self):
        self.active = True

    def commit(self):
        self.active = False


class SelectHandler:
    __slots__ = ('_f', '_flag')
    blockable = True

    def __init__(self, f, flag):
        self._f = f
        self._flag = flag

    @property
    def active(self):
        return self._flag.active

    def commit(self):
        self._flag.commit()
        return self._f

import collections
import abc

Box = collections.namedtuple('Box', 'get')
PutBox = collections.namedtuple('PutBox', 'handler val')


class Handler(abc.ABC):
    @property
    @abc.abstractmethod
    def active(self):
        pass

    @property
    @abc.abstractmethod
    def blockable(self):
        pass

    @abc.abstractmethod
    def commit(self):
        pass


class FnHandler(Handler):
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
    def __init__(self):
        self.active = True

    def commit(self):
        self.active = False


class SelectHandler(Handler):
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

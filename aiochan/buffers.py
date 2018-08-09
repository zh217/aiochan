import abc
import collections
import typing as t


class AbstractBuffer(abc.ABC):
    @abc.abstractmethod
    def __init__(self, maxsize):
        """

        :param maxsize:
        """

    @abc.abstractmethod
    def add(self, el):
        """

        :param el:
        :return:
        """

    @abc.abstractmethod
    def take(self):
        """

        :return:
        """

    @property
    @abc.abstractmethod
    def can_add(self):
        """

        :return:
        """

    @property
    @abc.abstractmethod
    def can_take(self):
        """

        :return:
        """


class FixedLengthBuffer:
    """
    TODO
    """
    __slots__ = ('_maxsize', '_queue')

    def __init__(self, maxsize):
        self._maxsize = maxsize
        self._queue = collections.deque()

    def add(self, el):
        """
        adding shit
        :param el:
        :return:
        """
        self._queue.append(el)

    def take(self):
        return self._queue.popleft()

    @property
    def can_add(self):
        return len(self._queue) < self._maxsize

    @property
    def can_take(self):
        return bool(len(self._queue))


class DroppingBuffer:
    """
    TODO
    """
    __slots__ = ('_maxsize', '_queue')

    def __init__(self, maxsize):
        self._maxsize = maxsize
        self._queue = collections.deque()

    def add(self, el):
        if len(self._queue) < self._maxsize:
            self._queue.append(el)

    def take(self):
        return self._queue.popleft()

    @property
    def can_add(self):
        return True

    @property
    def can_take(self):
        return bool(len(self._queue))


class SlidingBuffer:
    """
    TODO
    """
    __slots__ = ('_maxsize', '_queue')

    def __init__(self, maxsize):
        self._maxsize = maxsize
        self._queue = collections.deque(maxlen=maxsize)

    def add(self, el):
        self._queue.append(el)

    def take(self):
        return self._queue.popleft()

    @property
    def can_add(self):
        return True

    @property
    def can_take(self):
        return bool(len(self._queue))


class PromiseBuffer:
    """
    TODO
    """
    __slots__ = ('_val',)

    def __init__(self, _=None):
        self._val = None

    def add(self, el):
        if not self._val:
            self._val = el

    def take(self):
        return self._val

    can_add = True

    @property
    def can_take(self):
        return self._val is not None


class IterBuffer:
    """
    A buffer that is constructed from a iterable (unbounded ones are ok). The buffer never accepts new inputs and will
    give out items from the iterable one by one.
    """

    __slots__ = ('_iter', '_nxt')

    def __init__(self, it):
        self._iter = iter(it)
        try:
            self._nxt = next(self._iter)
        except StopIteration:
            self._nxt = None

    can_add = False

    @property
    def can_take(self):
        return self._nxt is not None

    def take(self):
        ret = self._nxt
        try:
            self._nxt = next(self._iter)
        except StopIteration:
            self._nxt = None
        return ret

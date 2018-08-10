import abc
import collections


class AbstractBuffer(abc.ABC):
    """
    Abstract buffer class intended for subclassing, to be used by channels.
    """

    @abc.abstractmethod
    def add(self, el):
        """
        Add an element to the buffer.

        Will only be called after `can_add` returns `True`.

        :param el: the element to add
        :return: `None`
        """

    @abc.abstractmethod
    def take(self):
        """
        Take an element from the buffer.

        Will only be called after `can_take` returns `True`.
        :return: an element from the buffer
        """

    @property
    @abc.abstractmethod
    def can_add(self):
        """
        Will be called each time before calling `add`.

        :return: bool, whether an element can be added.
        """

    @property
    @abc.abstractmethod
    def can_take(self):
        """
        Will be called each time before calling `take`.

        :return: bool, whether an element can be taken.
        """


class FixedLengthBuffer:
    """
    A fixed length buffer that will block on get when empty and block on put when full.

    :param maxsize: size of the buffer
    """
    __slots__ = ('_maxsize', '_queue')

    def __init__(self, maxsize):
        self._maxsize = maxsize
        self._queue = collections.deque()

    def add(self, el):
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
    A dropping buffer that will block on get when empty and never blocks on put.

    When the buffer is full, puts will succeed but the new values are dropped.

    :param maxsize: size of the buffer
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
    A sliding buffer that will block on get when empty and never blocks on put.

    When the buffer is full, puts will succeed and the oldest values are dropped.

    :param maxsize: size of the buffer
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
    A promise buffer that blocks on get when empty and never blocks on put.

    After a single value is put into the buffer, *all* subsequent gets will succeed with this value, and *all*
    subsequent puts will succeed but new values are ignored.
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
    A buffer that is constructed from a iterable (unbounded iterable is ok).

    The buffer never accepts new inputs and will give out items from the iterable one by one, and when the iterable
    is exhausted will block on further gets.

    :param it: the iterable to construct the buffer from.
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

import abc
import collections


class AbstractBuffer(abc.ABC):
    @abc.abstractmethod
    def add(self, el):
        """
        adding an element
        :param el:
        :return:
        """

    @abc.abstractmethod
    def take(self):
        """
        remove an element. if there is none, return `None`.
        :return:
        """

    @property
    @abc.abstractmethod
    def can_add(self):
        """
        whether the buffer is full
        :return:
        """

    @property
    @abc.abstractmethod
    def can_take(self):
        """
        whether the buffer contains at least one element
        :return:
        """


class FixedLengthBuffer(AbstractBuffer):
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


class DroppingBuffer(AbstractBuffer):
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


class SlidingBuffer(AbstractBuffer):
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


class PromiseBuffer(AbstractBuffer):
    def __init__(self, maxsize=None):
        self._val = None

    def add(self, el):
        self._val = el

    def take(self):
        return self._val

    @property
    def can_add(self):
        return self._val is None

    @property
    def can_take(self):
        return self._val is not None


class EmptyBuffer(AbstractBuffer):
    can_add = False
    can_take = False

    def __init__(self, maxsize=None):
        pass

    def add(self, el):
        raise RuntimeError

    def take(self):
        raise RuntimeError

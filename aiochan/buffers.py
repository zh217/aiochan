import collections


class FixedLengthBuffer:
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
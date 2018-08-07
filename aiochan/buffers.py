import collections


class FixedLengthBuffer:
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

    def __repr__(self):
        return f'FixedLengthBuffer({self._maxsize}, {list(self._queue)})'


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

    def __repr__(self):
        return f'DroppingBuffer({self._maxsize}, {list(self._queue)})'


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

    def __repr__(self):
        return f'SlidingBuffer({self._maxsize}, {list(self._queue)})'


class PromiseBuffer:
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

    def __repr__(self):
        return f'PromiseBuffer({self._val})'

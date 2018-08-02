import asyncio
from .channel import Chan


def map(self, f):
    pass


def flatmap(self, f):
    pass


def groupby(self, p):
    pass


def scan(self, p):
    pass


def window(self, n):
    pass


def debounce(self, seconds):
    pass


def delay(self, seconds):
    pass


def broadcaster(self):
    pass


def multicaster(self):
    pass


def duplicate(self, n):
    pass


def enumerate(self):
    pass


def distinct(self):
    pass


def element_at(self):
    pass


def first(self):
    pass


def last(self):
    pass


def sample(self, seconds):
    pass


def switch(self, control):
    pass


def filter(self, f):
    pass


def filter_false(self, f):
    pass


def reduce(self, f, init=None):
    pass


def take(self, n):
    pass


def drop(self, n):
    pass


def take_while(self, p):
    pass


def drop_while(self, p):
    pass


def reduce_sum(self, p):
    pass


def reduce_mean(self, p):
    pass


def reduce_max(self):
    pass


def reduce_min(self):
    pass


def zip(*chans):
    pass


def combine(*chans):
    pass


def merge(*chans):
    pass


def concat_elements(*chans):
    pass

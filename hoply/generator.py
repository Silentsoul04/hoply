import logging
from functools import reduce


log = logging.getLogger(__name__)


def application(iterator, step):
    return step(iterator)


def compose(generator, *steps):
    return reduce(application, steps, generator)


def skip(count):
    """Skip first ``count`` items"""

    def step(iterator):
        counter = 0
        for item in iterator:
            counter += 1
            if counter > count:
                yield item

    return step


def limit(count):
    """Keep only first ``count`` items"""

    def step(iterator):
        counter = 0
        for item in iterator:
            counter += 1
            yield item
            if counter == count:
                break

    return step


def paginator(count):
    """paginate..."""

    def step(iterator):
        counter = 0
        page = list()
        for item in iterator:
            page.append(item)
            counter += 1
            if counter == count:
                yield page
                counter = 0
                page = list()
        yield page

    return step


def add1(x, r):
    return r + 1


def count(iterator):
    """Count the number of items in the iterator"""
    return reduce(add1, iterator, 0)


def pick(name):
    def wrapped(bindings):
        return bindings.get(name)

    return wrapped


def map(func):
    def step(iterator):
        return (func(x) for x in iterator)

    return step


def unique(iterator):
    def uniquify(iterable, key=None):
        seen = set()
        for item in iterable:
            if item in seen:
                continue
            seen.add(item)
            yield item

    iterator = uniquify(iterator)
    return iterator


def filter(predicate):
    def step(iterator):
        return (x for x in iterator if predicate(x))

    return step


def mean(iterator):
    count = 0.0
    total = 0.0
    for item in iterator:
        total += item
        count += 1
    return total / count


def group_count(iterator):
    # return Counter(map(lambda x: x.value, iterator))
    raise NotImplemented


def describe(iterator):
    raise NotImplemented

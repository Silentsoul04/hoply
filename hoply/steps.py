import logging
from functools import reduce

from hoply.base import Triple


log = logging.getLogger(__name__)


def compose(*steps):
    """Pipeline builder and executor"""

    def composed(hoply, iterator=None):
        for step in steps:
            log.debug('running step=%r', step)
            iterator = step(hoply, iterator)
        return iterator

    return composed


def skip(count):
    """Skip first ``count`` items"""
    def step(hoply, iterator):
        counter = 0
        for item in iterator:
            counter += 1
            if counter > count:
                yield item
    return step


def limit(count):
    """Keep only first ``count`` items"""
    def step(hoply, iterator):
        counter = 0
        for item in iterator:
            counter += 1
            yield item
            if counter == count:
                break
    return step


def paginator(count):
    """paginate..."""
    def step(hoply, iterator):
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


def _add1(x, y):
    # TODO: check if this is better than a lambda
    return x + 1


def count(hoply, iterator):
    """Count the number of items in the iterator"""
    return reduce(_add1, iterator, 0)


def pick(name):
    raise NotImplemented


def map(func):
    def step(hoply, iterator):
        return (func(hoply, x) for x in iterator)
    return step


def unique(hoply, iterator):

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
    def step(hoply, iterator):
        return (x for x in iterator if predicate(x))
    return step


def mean(hoply, iterator):
    count = 0.
    total = 0.
    for item in iterator:
        total += item
        count += 1
    return total / count


def group_count(hoply, iterator):
    # return Counter(map(lambda x: x.value, iterator))
    raise NotImplemented


def describe(hoply, iterator):
    raise NotImplemented


def where(subject, predicate, object):
    # TODO: validate pattern
    pattern = Triple(subject, predicate, object)

    def step(hoply, iterator):
        return hoply._where(pattern, iterator)

    return step

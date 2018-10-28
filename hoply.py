# hoply - python triple store database for exploring bigger than
# memory relational graph data
#
# Copyright (C) 2015-2018  Amirouche Boubekki <amirouche@hypermove.net>
#
import logging
import json
from contextlib import contextmanager
from functools import reduce
from immutables import Map
from itertools import tee
from uuid import uuid4

from wiredtiger_ffi import Wiredtiger


log = logging.getLogger(__name__)


def pk(*args):
    print(args)
    return args[-1]


def uid():
    return uuid4().hex


def ngrams(iterable, n=2):
    if n < 1:
        raise ValueError
    t = tee(iterable, n)
    for i, x in enumerate(t):
        for j in range(i):
            next(x, None)
    return zip(*t)


def trigrams(string):
    # cf. http://stackoverflow.com/a/17532044/140837
    N = 3
    for word in string.split():
        token = '$' + word + '$'
        for i in range(len(token)-N+1):
            yield token[i:i+N]


def levenshtein(s1, s2):
    # cf. https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Python
    if len(s1) < len(s2):
        return levenshtein(s2, s1)

    # len(s1) >= len(s2)
    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # j+1 instead of j since previous_row and current_row are
            # one character longer
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1       # than s2
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


class HoplyException(Exception):
    pass


WT_NOT_FOUND = -31803


class Hoply(object):

    def __init__(self, path, logging=False, dumps=json.dumps, loads=json.loads):
        self.dumps = dumps
        self.loads = loads

        # init wiredtiger
        config = 'create,log=(enabled=true)' if logging else 'create'
        self._wiredtiger = Wiredtiger(path, config)
        session = self._wiredtiger.open_session()

        # tables with indices
        config = 'key_format=SSS,value_format=u,columns=(subject,predicate,object,nop)'
        session.create('table:spo', config)
        self._spo = session.open_cursor('table:spo')
        session.create('index:spo:pos', 'columns=(predicate,object,subject)')

        # TODO: global fuzzy search over subject, predicate and object

        self._session = session

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    @contextmanager
    def _spo_cursor(self):
        cursor = self._session.open_cursor('table:spo')
        try:
            yield cursor
        finally:
            cursor.close()

    @contextmanager
    def _pos_cursor(self):
        cursor = self._session.open_cursor('index:spo:pos()')
        try:
            yield cursor
        finally:
            cursor.close()

    def close(self):
        self._wiredtiger.close()

    def add(self, subject, predicate, object):
        subject = self.dumps(subject)
        predicate = self.dumps(predicate)
        object = self.dumps(object)
        self._spo.set_key(subject, predicate, object)
        self._spo.set_value(b'')
        self._spo.insert()

    def delete(self, subject, predicate, object):
        subject = self.dumps(subject)
        predicate = self.dumps(predicate)
        object = self.dumps(object)
        self._spo.set_key(subject, predicate, object)
        code = self._spo.search()
        if code == WT_NOT_FOUND:
            raise HoplyException('Triple not found')
        self._spo.remove()


open = Hoply


def compose(*steps):
    """Pipeline builder and executor"""

    def composed(hoply, iterator=None):
        for step in steps:
            log.debug('running step=%r', step)
            iterator = step(hoply, iterator)
        return iterator

    return composed


class var:

    __slots__ = ('name',)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<var %r>' % self.name


class Triple:

    __slots__ = ('subject', 'predicate', 'object')

    def __init__(self, subject, predicate, object):
        self.subject = subject
        self.predicate = predicate
        self.object = object

    def is_variables(self):
        return (
            isinstance(self.subject, var),
            isinstance(self.predicate, var),
            isinstance(self.object, var),
        )


def _pattern_bind(pattern, binding):
    subject = pattern.subject
    predicate = pattern.predicate
    object = pattern.object
    if isinstance(subject, var) and binding.get(subject.name) is not None:
        subject = binding[subject.name]
    if isinstance(predicate, var) and binding.get(predicate.name) is not None:
        predicate = binding[predicate.name]
    if isinstance(object, var) and binding.get(object.name) is not None:
        object = binding[object.name]
    return Triple(subject, predicate, object)


def where(subject, predicate, object):
    # TODO: validate pattern
    pattern = Triple(subject, predicate, object)

    def step(hoply, iterator):
        # cache
        dumps = hoply.dumps
        loads = hoply.loads

        if iterator is None:
            log.debug("where: 'iterator' is 'None'")
            # If the iterator is None then it's a seed step.
            vars = pattern.is_variables()
            # Generate bindings for the pattern.
            if vars == (True, False, False):
                log.debug("where: only 'subject' is a variable")

                # cache
                predicatex = dumps(pattern.predicate)
                objectx = dumps(pattern.object)

                # start range prefix search
                with hoply._pos_cursor() as cursor:
                    cursor.set_key(predicatex, objectx, '')
                    code = cursor.search_near()
                    if code == WT_NOT_FOUND:
                        return
                    elif code < 0:
                        if cursor.next() == WT_NOT_FOUND:
                            return

                    while True:
                        p, o, s = cursor.get_key()
                        matched = Triple(s, p, o)
                        if matched.predicate == predicatex and matched.object == objectx:
                            # yield match
                            subject = loads(matched.subject)
                            binding = Map().set(pattern.subject.name, subject)
                            yield binding

                            if cursor.next() == WT_NOT_FOUND:
                                return  # end of table
                        else:
                            return  # end of range prefix search

            elif vars == (False, True, True):
                log.debug('where: subject is NOT a variable')

                # cache
                subjectx = dumps(pattern.subject)

                # start range prefix search
                with hoply._spo_cursor() as cursor:
                    cursor.set_key(subjectx, '', '')
                    code = cursor.search_near()
                    if code == WT_NOT_FOUND:
                        return
                    elif code < 0:
                        if cursor.next() == WT_NOT_FOUND:
                            return

                    while True:
                        matched = Triple(*cursor.get_key())
                        if matched.subject == subjectx:
                            # yield match
                            binding = Map()
                            predicate = loads(matched.predicate)
                            binding = binding.set(pattern.predicate.name, predicate)
                            object = loads(matched.object)
                            binding = binding.set(pattern.object.name, object)
                            yield binding

                            if cursor.next() == WT_NOT_FOUND:
                                return  # end of table
                        else:
                            return  # end of range prefix search

            elif vars == (False, False, True):
                log.debug('where: object is a variable')

                # cache
                subjectx = dumps(pattern.subject)
                predicatex = dumps(pattern.predicate)

                # start range prefix search
                with hoply._spo_cursor() as cursor:
                    cursor.set_key(subjectx, predicatex, '')
                    code = cursor.search_near()
                    if code == WT_NOT_FOUND:
                        return
                    elif code < 0:
                        if cursor.next() == WT_NOT_FOUND:
                            return

                    while True:
                        matched = Triple(*cursor.get_key())
                        if matched.subject == subjectx and matched.predicate == predicatex:
                            # yield match
                            object = loads(matched.object)
                            binding = Map().set(pattern.object.name, object)
                            yield binding

                            if cursor.next() == WT_NOT_FOUND:
                                return  # end of table
                        else:
                            return  # end of range prefix search

            else:
                msg = 'Pattern not supported, '
                msg += 'create a bug report '
                msg += 'if you think that pattern should be supported: %r'
                msg = msg.format(vars)
                raise HoplyException(msg)

        else:
            log.debug("where: 'iterator' is not 'None'")
            for binding in iterator:
                bound = _pattern_bind(pattern, binding)
                log.debug("where: bound is %r", bound)
                vars = bound.is_variables()
                if vars == (False, False, False):
                    log.debug('fully bound pattern')
                    # fully bound pattern, check that it really exists
                    with hoply._spo_cursor() as cursor:
                        cursor.set_key(
                            hoply.dumps(bound.subject),
                            hoply.dumps(bound.predicate),
                            hoply.dumps(bound.object),
                        )
                        code = cursor.search()
                        if code == WT_NOT_FOUND:
                            continue
                        else:
                            yield binding

                elif vars == (False, False, True):
                    log.debug("where: only 'object' is a variable")

                    # cache
                    subjectx = dumps(bound.subject)
                    predicatex = dumps(bound.predicate)

                    # start range prefix search
                    with hoply._spo_cursor() as cursor:
                        cursor.set_key(subjectx, predicatex, '')
                        code = cursor.search_near()
                        if code == WT_NOT_FOUND:
                            return
                        elif code < 0:
                            if cursor.next() == WT_NOT_FOUND:
                                return

                        while True:
                            matched = Triple(*cursor.get_key())
                            if subjectx == matched.subject and predicatex == matched.predicate:
                                # yield match
                                object = loads(matched.object)
                                new = binding.set(bound.object.name, object)
                                yield new

                                if cursor.next() == WT_NOT_FOUND:
                                    # end of table, iter to next binding
                                    break
                            else:
                                # end of range prefix search, iter to next binding
                                break

                elif vars == (True, False, False):
                    log.debug("where: subject is variable")

                    # cache
                    predicatex = dumps(bound.predicate)
                    objectx = dumps(bound.object)

                    # start range prefix search
                    with hoply._pos_cursor() as cursor:
                        cursor.set_key(predicatex, objectx, '')
                        code = cursor.search_near()
                        if code == WT_NOT_FOUND:
                            return
                        elif code < 0:
                            if cursor.next() == WT_NOT_FOUND:
                                return

                        while True:
                            p, o, s = cursor.get_key()
                            matched = Triple(s, p, o)
                            if predicatex == matched.predicate and objectx == matched.object:
                                # yield match
                                subject = loads(matched.subject)
                                new = binding.set(pattern.subject.name, subject)
                                yield new

                                if cursor.next() == WT_NOT_FOUND:
                                    # end of table, iter to the next binding
                                    break
                            else:
                                # end of range prefix search, iter to the next binding
                                break

                else:
                    msg = 'Pattern not supported, '
                    msg += 'create a bug report '
                    msg += 'if you think that pattern should be supported: {}'
                    msg = msg.format(vars)
                    raise HoplyException(msg)

    return step


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
        for item in iterator:
            out = func(hoply, item)
            yield out
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
    raise NotImplemented


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

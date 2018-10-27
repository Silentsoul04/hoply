# hoply - python triple store database for exploring bigger than
# memory relational graph data
#
# Copyright (C) 2015-2018  Amirouche Boubekki <amirouche@hypermove.net>
#
import logging
import json
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
        self._pos = session.open_cursor('index:spo:pos()')

        # TODO: global fuzzy search over subject, predicate and object

        self._session = session

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

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


def _pattern_bind(pattern, binding):
    subject, predicate, object = pattern
    if isinstance(subject, var) and binding.get(subject.name) is not None:
        subject = binding[subject.name]
    if isinstance(predicate, var) and binding.get(predicate.name) is not None:
        predicate = binding[predicate.name]
    if isinstance(object, var) and binding.get(object.name) is not None:
        object = binding[object.name]
    return subject, predicate, object


def where(subject, predicate, object):
    # TODO: validate pattern
    pattern = (subject, predicate, object)

    def step(hoply, iterator):
        # cache
        dumps = hoply.dumps
        loads = hoply.loads

        if iterator is None:
            log.debug("where: 'iterator' is 'None'")
            # If the iterator is None then it's a seed step.
            vars = tuple((isinstance(item, var) for item in pattern))
            # Generate bindings for the pattern.
            if vars == (True, False, False):
                log.debug("where: only 'subject' is a variable")

                # cache
                predicatex = dumps(predicate)
                objectx = dumps(object)

                # start range prefix search
                hoply._pos.set_key(predicatex, objectx, '')
                code = hoply._pos.search_near()
                if code == WT_NOT_FOUND:
                    return
                elif code < 0:
                    if hoply._pos.next() == WT_NOT_FOUND:
                        return

                while True:
                    matched = hoply._pos.get_key()
                    if matched[0] == predicatex and matched[1] == objectx:
                        # yield match
                        binding = Map().set(subject.name, loads(matched[2]))
                        yield binding

                        if hoply._pos.next() == WT_NOT_FOUND:
                            return  # end of table
                    else:
                        return  # end of range prefix search

            elif vars == (False, True, True):
                log.debug('where: subject is NOT a variable')

                # cache
                subjectx = dumps(subject)

                # start range prefix search
                hoply._spo.set_key(subjectx, '', '')
                code = hoply._spo.search_near()
                if code == WT_NOT_FOUND:
                    return
                elif code < 0:
                    if hoply._spo.next() == WT_NOT_FOUND:
                        return

                while True:
                    matched = hoply._spo.get_key()
                    if matched[0] == subjectx:
                        # yield match
                        binding = Map()
                        binding = binding.set(predicate.name, matched[1])
                        binding = binding.set(object.name, matched[2])
                        yield binding

                        if hoply._spo.next() == WT_NOT_FOUND:
                            return  # end of table
                    else:
                        return  # end of range prefix search

            elif vars == (False, False, True):
                log.debug('where: object is a variable')

                # cache
                subjectx = dumps(subject)
                predicatex = dumps(predicate)

                # start range prefix search
                hoply._spo.set_key(subjectx, predicatex, '')
                code = hoply._spo.search_near()
                if code == WT_NOT_FOUND:
                    return
                elif code < 0:
                    if hoply._spo.next() == WT_NOT_FOUND:
                        return

                while True:
                    matched = hoply._spo.get_key()
                    if matched[0] == subjectx and matched[1] == predicatex:
                        # yield match
                        binding = Map().set(object.name, loads(matched[2]))
                        yield binding

                        if hoply._spo.next() == WT_NOT_FOUND:
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
                vars = tuple((isinstance(item, var) for item in bound))
                if vars == (False, False, False):
                    log.debug('fully bound pattern')
                    # fully bound pattern, check that it really exists
                    hoply._spo.set_key(
                        hoply.dumps(subject),
                        hoply.dumps(predicate),
                        hoply.dumps(object),
                    )
                    code = hoply._pos.search()
                    if code == WT_NOT_FOUND:
                        continue
                    else:
                        yield binding

                elif vars == (False, False, True):
                    log.debug("where: only 'object' is a variable")

                    # cache
                    subjectx = dumps(bound[0])
                    predicatex = dumps(bound[1])

                    # start range prefix search
                    hoply._spo.set_key(subjectx, predicatex, '')
                    code = pk(hoply._spo.search_near())
                    if code == WT_NOT_FOUND:
                        return
                    elif code < 0:
                        if hoply._spo.next() == WT_NOT_FOUND:
                            return

                    while True:
                        matched = hoply._spo.get_key()
                        pk('>>>>', matched)
                        if subjectx == matched[0] and predicatex == matched[1]:
                            # yield match
                            new = binding.set(object.name, loads(matched[2]))
                            print(new)
                            yield new

                            if hoply._spo.next() == WT_NOT_FOUND:
                                # end of table, iter to next binding
                                break
                        else:
                            # end of range prefix search, iter to next binding
                            break

                elif vars == (True, False, False):
                    log.debug("where: subject is variable")

                    # cache
                    predicatex = dumps(bound[1])
                    objectx = dumps(bound[2])

                    # start range prefix search
                    hoply._pos.set_key(predicatex, objectx, '')
                    code = hoply._pos.search_near()
                    if code == WT_NOT_FOUND:
                        return
                    elif code < 0:
                        if hoply._pos.next() == WT_NOT_FOUND:
                            return

                    while True:
                        matched = hoply._pos.get_key()
                        if predicatex == matched[0] and objectx == matched[1]:
                            # yield match
                            value = loads(matched[2])
                            new = binding.set(subject.name, value)
                            yield new

                            if hoply._pos.next() == WT_NOT_FOUND:
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

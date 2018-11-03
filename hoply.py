# hoply - python triple store database for exploring bigger than
# memory relational graph data
#
# Copyright (C) 2015-2018  Amirouche Boubekki <amirouche@hypermove.net>
#
import logging
import json
import struct
from bisect import bisect_left
from contextlib import contextmanager
from functools import reduce
from immutables import Map
from itertools import tee
from uuid import UUID
from uuid import uuid4

from wiredtiger_ffi import Wiredtiger


log = logging.getLogger(__name__)


# This source file was part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
# Copyright 2018 Amirouche Boubekki <amirouche.boubekki@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

_size_limits = tuple((1 << (i * 8)) - 1 for i in range(9))

int2byte = struct.Struct(">B").pack

# Define type codes:
NULL_CODE = 0x00
BYTES_CODE = 0x01
STRING_CODE = 0x02
INT_ZERO_CODE = 0x14
POS_INT_END = 0x1d
NEG_INT_START = 0x0b
FLOAT_CODE = 0x20
DOUBLE_CODE = 0x21
FALSE_CODE = 0x26
TRUE_CODE = 0x27
UUID_CODE = 0x30

# Reserved: Codes 0x03, 0x04, 0x23, and 0x24 are reserved for historical reasons.


def _find_terminator(v, pos):
    # Finds the start of the next terminator [\x00]![\xff] or the end of v
    while True:
        pos = v.find(b'\x00', pos)
        if pos < 0:
            return len(v)
        if pos + 1 == len(v) or v[pos + 1:pos + 2] != b'\xff':
            return pos
        pos += 2


# If encoding and sign bit is 1 (negative), flip all of the bits. Otherwise, just flip sign.
# If decoding and sign bit is 0 (negative), flip all of the bits. Otherwise, just flip sign.
def _float_adjust(v, encode):
    if encode and v[0] & 0x80 != 0x00:
        return b''.join((int2byte(x ^ 0xff) for x in v))
    elif not encode and v[0] & 0x80 != 0x80:
        return b''.join((int2byte(x ^ 0xff) for x in v))
    else:
        return int2byte(v[0] ^ 0x80) + v[1:]


def _decode(v, pos):
    code = v[pos]
    if code == NULL_CODE:
        return None, pos + 1
    elif code == BYTES_CODE:
        end = _find_terminator(v, pos + 1)
        return v[pos + 1:end].replace(b"\x00\xFF", b"\x00"), end + 1
    elif code == STRING_CODE:
        end = _find_terminator(v, pos + 1)
        return v[pos + 1:end].replace(b"\x00\xFF", b"\x00").decode("utf-8"), end + 1
    elif code >= INT_ZERO_CODE and code < POS_INT_END:
        n = code - 20
        end = pos + 1 + n
        return struct.unpack(">Q", b'\x00' * (8 - n) + v[pos + 1:end])[0], end
    elif code > NEG_INT_START and code < INT_ZERO_CODE:
        n = 20 - code
        end = pos + 1 + n
        return struct.unpack(">Q", b'\x00' * (8 - n) + v[pos + 1:end])[0] - _size_limits[n], end
    elif code == POS_INT_END:  # 0x1d; Positive 9-255 byte integer
        length = v[pos + 1]
        val = 0
        for i in range(length):
            val = val << 8
            val += v[pos + 2 + i]
        return val, pos + 2 + length
    elif code == NEG_INT_START:  # 0x0b; Negative 9-255 byte integer
        length = v[pos + 1] ^ 0xff
        val = 0
        for i in range(length):
            val = val << 8
            val += v[pos + 2 + i]
        return val - (1 << (length * 8)) + 1, pos + 2 + length
    elif code == DOUBLE_CODE:
        return struct.unpack(">d", _float_adjust(v[pos + 1:pos + 9], False))[0], pos + 9
    elif code == UUID_CODE:
        return UUID(bytes=v[pos + 1:pos + 17]), pos + 17
    elif code == FALSE_CODE:
        return False, pos + 1
    elif code == TRUE_CODE:
        return True, pos + 1
    else:
        raise ValueError("Unknown data type in DB: " + repr(v))


def _encode(value):
    # returns [code][data] (code != 0xFF)
    # encoded values are self-terminating
    # sorting need to work too!
    if value is None:
        return int2byte(NULL_CODE)
    elif isinstance(value, bytes):
        return int2byte(BYTES_CODE) + value.replace(b'\x00', b'\x00\xFF') + b'\x00'
    elif isinstance(value, str):
        return int2byte(STRING_CODE) + value.encode('utf-8').replace(b'\x00', b'\x00\xFF') + b'\x00'
    elif isinstance(value, int):
        if value == 0:
            return int2byte(INT_ZERO_CODE)
        elif value > 0:
            if value >= _size_limits[-1]:
                length = (value.bit_length() + 7) // 8
                data = [int2byte(POS_INT_END), int2byte(length)]
                for i in range(length - 1, -1, -1):
                    data.append(int2byte((value >> (8 * i)) & 0xff))
                return b''.join(data)

            n = bisect_left(_size_limits, value)
            return int2byte(INT_ZERO_CODE + n) + struct.pack(">Q", value)[-n:]
        else:
            if -value >= _size_limits[-1]:
                length = (value.bit_length() + 7) // 8
                value += (1 << (length * 8)) - 1
                data = [int2byte(NEG_INT_START), int2byte(length ^ 0xff)]
                for i in range(length - 1, -1, -1):
                    data.append(int2byte((value >> (8 * i)) & 0xff))
                return b''.join(data)

            n = bisect_left(_size_limits, -value)
            maxv = _size_limits[n]
            return int2byte(INT_ZERO_CODE - n) + struct.pack(">Q", maxv + value)[-n:]
    elif isinstance(value, float):
        return int2byte(DOUBLE_CODE) + _float_adjust(struct.pack(">d", value), True)
    elif isinstance(value, UUID):
        return int2byte(UUID_CODE) + value.bytes
    elif isinstance(value, bool):
        if value:
            return int2byte(TRUE_CODE)
        else:
            return int2byte(FALSE_CODE)
    else:
        raise ValueError("Unsupported data type: " + str(type(value)))


def pack(t):
    return b''.join((_encode(x) for x in t))


def unpack(key):
    pos = 0
    res = []
    copy = b''.join((x for x in key))
    while pos < len(copy):
        r, pos = _decode(copy, pos)
        res.append(r)
    return tuple(res)


# rest of hoply

def pk(*args):
    log.critical('%r', args)
    return args[-1]


def uid():
    return uuid4()


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


def _check(value, other):
    if value == '':
        # in this case the check is not revelant because,
        # the value doesn't matter in the prefix search
        return True
    return value == other


def _cursor_prefix(cursor, a, b):
    """Return a generator over the range described by the parameters"""
    prefix = (a, b)
    cursor.set_key(pack(prefix))
    code = cursor.search_near()
    if code == WT_NOT_FOUND:
        return
    elif code < 0:
        if cursor.next() == WT_NOT_FOUND:
            return

    while True:
        key = cursor.get_key()
        out = unpack(key[0])
        ok = (_check(*x) for x in zip(prefix, out))
        if all(ok):
            # yield match
            yield out
            if cursor.next() == WT_NOT_FOUND:
                return  # end of table
        else:
            return  # end of range prefix search


class Hoply(object):

    def __init__(self, path, logging=False):
        # init wiredtiger
        config = 'create,log=(enabled=true)' if logging else 'create'
        self._wiredtiger = Wiredtiger(path, config)
        session = self._wiredtiger.open_session()

        # tables with indices
        config = 'key_format=u,value_format=u,columns=(value,nop)'
        session.create('table:spo', config)
        session.create('table:pos', config)

        self._spo = session.open_cursor('table:spo')
        self._pos = session.open_cursor('table:pos')

        # TODO: global fuzzy search over subject, predicate and object

        self._session = session

    def begin(self):
        return self._session.transaction_begin()

    def commit(self):
        return self._session.transaction_commit()

    def rollback(self):
        return self._session.transaction_rollback()

    @contextmanager
    def transaction(self):
        self.begin()
        try:
            yield
        except Exception as exc:
            self.rollback()
            raise
        else:
            self.commit()

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
        cursor = self._session.open_cursor('table:pos')
        try:
            yield cursor
        finally:
            cursor.close()

    def close(self):
        self._wiredtiger.close()

    def add(self, subject, predicate, object):
        # insert in spo
        spo = pack((subject, predicate, object))
        self._spo.set_key(spo)
        self._spo.set_value(b'')
        self._spo.insert()
        # insert in pos
        pos = pack((predicate, object, subject))
        self._pos.set_key(pos)
        self._pos.set_value(b'')
        self._pos.insert()

    # def delete(self, subject, predicate, object):
    #     subject = self.dumps(subject)
    #     predicate = self.dumps(predicate)
    #     object = self.dumps(object)
    #     self._spo.set_key(subject, predicate, object)
    #     code = self._spo.search()
    #     if code == WT_NOT_FOUND:
    #         raise HoplyException('Triple not found')
    #     self._spo.remove()


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

    def bind(self, binding):
        subject = self.subject
        predicate = self.predicate
        object = self.object
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
        if iterator is None:
            log.debug("where: 'iterator' is 'None'")
            # If the iterator is None then it's a seed step.
            vars = pattern.is_variables()
            # Generate bindings for the pattern.
            if vars == (True, False, False):
                log.debug("where: only 'subject' is a variable")
                # cache
                predicatex = pattern.predicate
                objectx = pattern.object
                # start range prefix search
                with hoply._pos_cursor() as cursor:
                    for _, _, subject in _cursor_prefix(cursor, predicatex, objectx):
                        # yield binding with subject set to its
                        # variable
                        binding = Map().set(pattern.subject.name, subject)
                        yield binding

            elif vars == (False, True, True):
                log.debug('where: subject is NOT a variable')
                # cache
                subjectx = pattern.subject
                # start range prefix search
                with hoply._spo_cursor() as cursor:
                    for _, predicate, object in _cursor_prefix(cursor, subjectx, ''):
                        # yield binding where predicate and object are
                        # set to their variables
                        binding = Map()
                        binding = binding.set(pattern.predicate.name, predicate)
                        binding = binding.set(pattern.object.name, object)
                        yield binding

            elif vars == (False, False, True):
                log.debug('where: object is a variable')
                # cache
                subjectx = pattern.subject
                predicatex = pattern.predicate
                # start range prefix search
                with hoply._spo_cursor() as cursor:
                    for _, _, object in _cursor_prefix(cursor, subjectx, predicatex):
                        # yield binding where object is set to its
                        # variable
                        binding = Map().set(pattern.object.name, object)
                        yield binding
            else:
                msg = 'Pattern not supported, '
                msg += 'create a bug report '
                msg += 'if you think that pattern should be supported: %r'
                msg = msg.format(vars)
                raise HoplyException(msg)

        else:
            log.debug("where: 'iterator' is not 'None'")
            for binding in iterator:
                bound = pattern.bind(binding)
                log.debug("where: bound is %r", bound)
                vars = bound.is_variables()
                if vars == (False, False, False):
                    log.debug('fully bound pattern')
                    # fully bound pattern, check that it really exists
                    with hoply._spo_cursor() as cursor:
                        key = (
                            bound.subject,
                            bound.predicate,
                            bound.object,
                        )
                        cursor.set_key(pack(key))
                        code = cursor.search()
                        if code == WT_NOT_FOUND:
                            continue
                        else:
                            yield binding

                elif vars == (False, False, True):
                    log.debug("where: only 'object' is a variable")
                    # cache
                    subjectx = bound.subject
                    predicatex = bound.predicate
                    # start range prefix search
                    with hoply._spo_cursor() as cursor:
                        for _, _, object in _cursor_prefix(cursor, subjectx, predicatex):
                            new = binding.set(bound.object.name, object)
                            yield new

                elif vars == (True, False, False):
                    log.debug("where: subject is variable")
                    # cache
                    predicatex = bound.predicate
                    objectx = bound.object
                    # start range prefix search
                    with hoply._pos_cursor() as cursor:
                        for _, _, subject in _cursor_prefix(cursor, predicatex, objectx):
                            new = binding.set(pattern.subject.name, subject)
                            yield new
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

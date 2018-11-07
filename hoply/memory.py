import logging
from contextlib import contextmanager

from sortedcontainers import SortedSet

from hoply.base import AbstractStore
from hoply.tuple import pack
from hoply.tuple import unpack
from hoply.tuple import strinc

WT_NOT_FOUND = -31803


log = logging.getLogger(__name__)


class MemoryStore(AbstractStore):

    def __init__(self, *args, **kwargs):
        pass

    # basics

    def open(self):
        self._spo = SortedSet()
        self._pos = SortedSet()

    def close(self):
        pass

    # transaction

    def begin(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    # cursor

    @contextmanager
    def _spo_cursor(self):
        yield self._spo

    @contextmanager
    def _pos_cursor(self):
        yield self._pos

    def _prefix(self, cursor, a, b):
        """Return a generator over the range described by the parameters"""
        if b:
            start = pack((a, b))
        else:
            start = pack((a,))
        stop = strinc(start)
        for key in cursor.irange(start, stop, (True, False)):
            out = unpack(key)
            yield out

    # garbage in, garbage out

    def add(self, subject, predicate, object):
        # insert in spo
        spo = pack((subject, predicate, object))
        self._spo.add(spo)
        # insert in pos
        pos = pack((predicate, object, subject))
        self._pos.add(pos)

    def exists(self, cursor, subject, predicate, object):
        key = (
            subject,
            predicate,
            object,
        )
        key = pack(key)
        try:
            self._spo.index(key)
        except ValueError:
            return False
        else:
            return True

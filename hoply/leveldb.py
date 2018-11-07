import logging
from contextlib import contextmanager

from plyvel import DB

from hoply.base import AbstractStore
from hoply.tuple import strinc
from hoply.tuple import pack
from hoply.tuple import unpack


log = logging.getLogger(__name__)


class LevelDBStore(AbstractStore):

    def __init__(self, path, *args, **kwargs):
        self._path = path

    # basics

    def open(self):
        self._db = DB(self._path, create_if_missing=True)

        self._spo = self._db.prefixed_db(b'spo-')
        self._pos = self._db.prefixed_db(b'pos-')

    def close(self):
        self._db.close()

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

        for key, _ in cursor.iterator(start=start, stop=stop):
            out = unpack(key)
            yield out

    # garbage in, garbage out

    def add(self, subject, predicate, object):
        # insert in spo
        spo = pack((subject, predicate, object))
        self._spo.put(spo, b'')
        # insert in pos
        pos = pack((predicate, object, subject))
        self._pos.put(pos, b'')

    def exists(self, cursor, subject, predicate, object):
        key = pack((subject, predicate, object))
        out = self._pos.get(key) is not None
        return out

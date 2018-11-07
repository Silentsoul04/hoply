import logging
from contextlib import contextmanager

from wiredtiger_ffi import Wiredtiger

from hoply.base import AbstractStore
from hoply.helpers import check
from hoply.tuple import pack
from hoply.tuple import unpack


WT_NOT_FOUND = -31803


log = logging.getLogger(__name__)


class WiredtigerStore(AbstractStore):

    def __init__(self, path, logging=False):
        self._path = path
        self._logging = logging

    # basics

    def open(self):
        # init wiredtiger
        config = 'create,log=(enabled=true)' if self._logging else 'create'
        self._wiredtiger = Wiredtiger(self._path, config)
        session = self._wiredtiger.open_session()

        # tables with indices
        config = 'key_format=u,value_format=u,columns=(value,nop)'
        session.create('table:spo', config)
        session.create('table:pos', config)

        self._spo = session.open_cursor('table:spo')
        self._pos = session.open_cursor('table:pos')

        # TODO: global fuzzy search over subject, predicate and object

        self._session = session

    def close(self):
        self._wiredtiger.close()

    # transaction

    def begin(self):
        return self._session.transaction_begin()

    def commit(self):
        return self._session.transaction_commit()

    def rollback(self):
        return self._session.transaction_rollback()

    # cursor

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

    def _prefix(self, cursor, a, b):
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
            ok = (check(*x) for x in zip(prefix, out))
            if all(ok):
                # yield match
                yield out
                if cursor.next() == WT_NOT_FOUND:
                    return  # end of table
            else:
                return  # end of range prefix search

    # garbage in, garbage out

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

    def exists(self, cursor, subject, predicate, object):
        key = (
            subject,
            predicate,
            object,
        )
        cursor.set_key(pack(key))
        code = cursor.search()
        if code == WT_NOT_FOUND:
            return False
        else:
            return True

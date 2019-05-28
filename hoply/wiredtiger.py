from contextlib import contextmanager

from wiredtiger import wiredtiger_open

from hoply.hoply import HoplyBase


WT_NOT_FOUND = -31803


class WiredTigerConnexion(HoplyBase):
    """Storage layer connection

    WiredTiger specifics.

    """

    def __init__(self, path, logging=True):
        self._path = path
        self._logging = logging

        # init wiredtiger
        config = (
            "create,log=(enabled=true,file_max=512MB),cache_size=1024MB"
            if self._logging
            else "create"
        )
        self._cnx = wiredtiger_open(self._path, config)
        self._session = self._cnx.open_session()

    def init(self, table):
        self._session.create(table, "key_format=u,value_format=u")

    def close(self):
        self._cnx.close()

    # transaction

    def begin(self):
        return self._session.begin_transaction()

    def commit(self):
        self._session.commit_transaction()
        self._session.reset()

    def rollback(self):
        self._session.rollback_transaction()
        self._session.reset()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def make_cursor(self, table):
        out = self._session.open_cursor(table)
        return out

    @contextmanager
    def cursor(self, table):
        cursor = self.make_cursor(table)
        try:
            yield cursor
        finally:
            cursor.close()

    def add(self, cursor, key):
        cursor.set_key(key)
        cursor.set_value(b"0")
        cursor.insert()

    def rm(self, cursor, key):
        cursor.set_key(key)
        cursor.remove()

    def range(self, cursor, prefix):
        """Return a generator over the range described by PREFIX"""
        cursor.set_key(prefix)
        code = cursor.search_near()
        if code == WT_NOT_FOUND:
            return
        elif code < 0:
            if cursor.next() == WT_NOT_FOUND:
                return

        while True:
            key = cursor.get_key()
            if key.startswith(prefix):
                yield key
                if cursor.next() == WT_NOT_FOUND:
                    return  # end of table
            else:
                return  # end of range prefix search

    def search(self, cursor, key):
        # TODO: more efficiant implementation
        for _ in self._range(cursor, key):
            return True
        return False

    @contextmanager
    def transaction(self):
        self.begin()
        try:
            yield
        except Exception:
            self.rollback()
            raise
        else:
            self.commit()

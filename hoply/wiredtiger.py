from contextlib import contextmanager

from wiredtiger import wiredtiger_open

from hoply.hoply import HoplyBase


WT_NOT_FOUND = -31803


class WiredTigerConnexion(HoplyBase):
    """Database connection

    Hold information about the database that is used. In the future
    this class MIGHT be abstract and implement different backend.

    """

    def __init__(self, path, logging=True):
        self._path = path
        self._logging = logging

        # init wiredtiger
        config = "create,log=(enabled=true)" if self._logging else "create"
        self._wiredtiger = wiredtiger_open(self._path, config)
        self._session = self._wiredtiger.open_session()

    def close(self):
        self._wiredtiger.close()

    # transaction

    def begin(self):
        return self._session.transaction_begin()

    def commit(self):
        return self._session.transaction_commit()

    def rollback(self):
        return self._session.transaction_rollback()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

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
    def _cursor(self, table):
        cursor = self._session.open_cursor(table)
        try:
            yield cursor
        finally:
            cursor.close()

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

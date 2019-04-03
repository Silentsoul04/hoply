from contextlib import contextmanager

from sortedcontainers import SortedSet

from hoply.hoply import HoplyBase
from hoply.tuple import strinc


class MemoryConnexion(HoplyBase):
    """Storage layer connection

    LevelDB specifics.

    """

    def __init__(self, path, logging=True):
        self._path = path
        self._tables = dict()

    def init(self, table):
        self._tables[table] = SortedSet()

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def make_cursor(self, table):
        out = self._tables[table]
        return out

    @contextmanager
    def cursor(self, table):
        cursor = self.make_cursor(table)
        yield cursor

    def add(self, cursor, key):
        cursor.add(key)

    def rm(self, cursor, key):
        cursor.remove(key)

    def search(self, cursor, key):
        out = key in cursor
        return out

    def range(self, cursor, prefix):
        """Return a generator over the range described by PREFIX"""
        stop = strinc(prefix)
        yield from cursor.irange(prefix, stop, (True, False))

    # transaction

    def begin(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    @contextmanager
    def transaction(self):
        yield

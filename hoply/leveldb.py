from contextlib import contextmanager

from plyvel import DB

from hoply.hoply import HoplyBase
from hoply.tuple import strinc


class LevelDBConnexion(HoplyBase):
    """Storage layer connection

    LevelDB specifics.

    """

    def __init__(self, path, logging=True):
        self._path = path
        self._storage = DB(path, create_if_missing=True)

    def init(self, table):
        pass

    def close(self):
        self._storage.close()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def make_cursor(self, table):
        out = self._storage.prefixed_db(table.encode())
        return out

    @contextmanager
    def cursor(self, table):
        cursor = self.make_cursor(table)
        yield cursor

    def add(self, cursor, key):
        cursor.put(key, b"")

    def rm(self, cursor, key):
        cursor.delete(key)

    def search(self, cursor, key):
        out = cursor.get(key) is not None
        return out

    def range(self, cursor, prefix):
        """Return a generator over the range described by PREFIX"""
        stop = strinc(prefix)

        for key, _ in cursor.iterator(start=prefix, stop=stop):
            yield key

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

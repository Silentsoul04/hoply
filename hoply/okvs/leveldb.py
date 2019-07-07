from plyvel import DB
from hoply.hoply import HoplyBase
from hoply.hoply import drop
from hoply.hoply import take
from hoply.tuple import strinc


class Transaction(HoplyBase):
    def __init__(self, storage):
        self._storage = storage._storage

    def rollback(self, **config):
        pass

    def commit(self, **config):
        pass

    def add(self, key, value):
        self._storage.put(key, value)

    def remove(self, key):
        self._storage.delete(key)

    def get(self, key):
        return self._storage.get(key)

    def range(self, start, start_include, end, end_include, **config):
        out = self._storage.iterator(
            start=start, stop=end, include_start=start_include, include_stop=end_include
        )

        # apply offset
        try:
            offset = config["offset"]
        except KeyError:
            pass
        else:
            out = drop(out, offset)

        # apply limit
        try:
            limit = config["limit"]
        except KeyError:
            pass
        else:
            out = take(out, limit)

        # apply reverse
        try:
            reverse = config["reverse"]
        except KeyError:
            pass
        else:
            if reverse:
                out = reversed(out)

        # at last!
        return out

    def prefix(self, prefix, **config):
        """Return a generator over the range described by PREFIX"""
        stop = strinc(prefix)
        yield from self.range(prefix, True, stop, False, **config)


class LevelDB(HoplyBase):
    """Storage layer connection

    LevelDB specifics.

    """

    def __init__(self, home, **config):
        self._home = home
        self._storage = DB(home, create_if_missing=True)

    def __enter__(self):
        return self

    def close(self):
        self._storage.close()

    def __exit__(self, *args, **kwargs):
        self.close()

    def begin(self, **config):
        return Transaction(self)

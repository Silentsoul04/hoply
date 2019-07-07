from sortedcontainers import SortedDict
from hoply.hoply import HoplyBase
from hoply.hoply import take
from hoply.hoply import drop
from hoply.tuple import strinc


class Transaction(HoplyBase):
    def __init__(self, memory):
        self._memory = memory
        self._space = memory._space.copy()

    def rollback(self, **kwargs):
        pass

    def commit(self, **kwargs):
        self._memory._space = self._space

    def get(self, key):
        return self._space.get(key)

    def add(self, key, value):
        self._space[key] = value

    def remove(self, key):
        del self._space[key]

    def range(self, start, start_include, end, end_include, **config):
        """Return (key, value) pairs that are in the range described by the
        arguments

        """
        keys = self._space.irange(start, end, (start_include, end_include))
        out = ((key, self._space[key]) for key in keys)

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


class Memory(HoplyBase):
    """Memory storage.

    """

    def __init__(self, home, **config):
        self._space = SortedDict()

    def __enter__(self):
        return self

    def close(self):
        pass

    def __exit__(self, *args, **config):
        self.close()

    def begin(self, **config):
        return Transaction(self)

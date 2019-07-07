from contextlib import contextmanager
from wiredtiger import wiredtiger_open
from hoply.hoply import HoplyBase
from hoply.hoply import take
from hoply.hoply import drop
from hoply.tuple import strinc


WT_NOT_FOUND = -31803


class Transaction(HoplyBase):
    def __init__(self, storage):
        self._session = storage._cnx.open_session()
        self._session.begin_transaction()

    def rollback(self, **kwargs):
        self._session.rollback_transaction()
        self._session.close()

    def commit(self, **kwargs):
        self._session.commit_transaction()
        self._session.close()

    @contextmanager
    def _cursor(self):
        cursor = self._session.open_cursor("table:hoply")
        try:
            yield cursor
        finally:
            cursor.close()

    def get(self, key):
        with self._cursor() as cursor:
            cursor.set_key(key)
            if cursor.search() == WT_NOT_FOUND:
                return None
            else:
                return cursor.get_value()

    def add(self, key, value):
        with self._cursor() as cursor:
            cursor.set_key(key)
            cursor.set_value(value)
            cursor.insert()

    def remove(self, key):
        with self._cursor() as cursor:
            cursor.set_key(key)
            cursor.remove()

    def _range(self, start, start_include, end, end_include, **config):
        with self._cursor() as cursor:
            cursor.set_key(start)
            code = cursor.search_near()
            if code == WT_NOT_FOUND:
                return
            elif code < 0:
                if cursor.next() == WT_NOT_FOUND:
                    return
            elif code == 0:
                if start_include:
                    yield (start, cursor.get_value())

            while True:
                key = cursor.get_key()
                if key < end:
                    yield (key, cursor.get_value())
                    if cursor.next() == WT_NOT_FOUND:
                        return
                    else:
                        continue
                elif key == end:
                    if end_include:
                        yield (key, cursor.get_value())
                    return
                else:  # key is after end
                    return

    def _range_reverse(self, start, start_include, end, end_include, **config):
        with self._cursor() as cursor:
            cursor.set_key(end)
            code = cursor.search_near()
            if code == WT_NOT_FOUND:
                return
            elif code > 0:
                if cursor.prev() == WT_NOT_FOUND:
                    return
            elif code == 0:
                if end_include:
                    yield (end, cursor.get_value())

            while True:
                key = cursor.get_key()
                if key > start:
                    yield (key, cursor.get_value())
                    if cursor.prev() == WT_NOT_FOUND:
                        return
                    else:
                        continue
                elif key == start:
                    if start_include:
                        yield (key, cursor.get_value())
                    return
                else:  # key is before start
                    return

    def range(self, start, start_include, end, end_include, **config):
        """Return a generator over the range described by PREFIX"""
        try:
            reverse = config["reverse"]
        except KeyError:
            reverse = False

        if reverse:
            out = self._range_reverse(start, start_include, end, end_include, **config)
        else:
            out = self._range(start, start_include, end, end_include, **config)

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

        # at last!
        return out

    def prefix(self, prefix, **config):
        """Return a generator over the range described by PREFIX"""
        stop = strinc(prefix)
        yield from self.range(prefix, True, stop, False, **config)


class WiredTiger(HoplyBase):
    """WiredTiger storage

    """

    def __init__(self, home, **config):
        self._home = home

        # TODO: take into account CONFIG
        config = "create,log=(enabled=true,file_max=512MB),cache_size=1024MB"
        self._cnx = wiredtiger_open(home, config)
        # init table
        session = self._cnx.open_session()
        session.create("table:hoply", "key_format=u,value_format=u")
        session.close()

    def __enter__(self):
        return self

    def close(self):
        self._cnx.close()

    def __exit__(self, *args, **config):
        self.close()

    def begin(self, **config):
        return Transaction(self)

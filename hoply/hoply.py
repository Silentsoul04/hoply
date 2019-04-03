# https://github.com/amirouche/hoply/
#
# Copyright (C) 2015-2019  Amirouche Boubekki <amirouche.boubekki@gmail.com>
#
import asyncio
import functools
import inspect
import logging
import operator

from immutables import Map

from hoply.tuple import pack
from hoply.tuple import unpack
from hoply.indices import compute_indices


log = logging.getLogger(__name__)


class HoplyBase:
    pass


class HoplyException(Exception):
    pass


class NotFound(HoplyException):
    pass


class Variable:

    __slots__ = ("name",)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<var %r>" % self.name


var = Variable  # XXX: use only 'var' in where queries please!


def is_permutation_prefix(combination, index):
    index = "".join(str(x) for x in index)
    combination = "".join(str(x) for x in combination)
    out = index.startswith(combination)
    return out


class Hoply(HoplyBase):
    def __init__(self, cnx, name, items):
        self.name = name
        self._cnx = cnx
        self._items = items
        self._cursors = dict()
        self._tables = dict()
        # create a table for each index.
        for index in compute_indices(len(items)):
            table = "table:" + self.name + "-" + "".join(str(x) for x in index)
            self._cnx.init(table)
            # cache cursor
            self._cursors[index] = self._cnx.make_cursor(table)
            self._tables[index] = table

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self._cnx.close()

    def add(self, *items):
        assert len(items) == len(self._items)
        for index, cursor in self._cursors.items():
            permutation = tuple(items[i] for i in index)
            self._cnx.add(cursor, pack(permutation))

    def rm(self, *items):
        assert len(items) == len(self._items)
        for index, cursor in self._cursors.items():
            permutation = tuple(items[i] for i in index)
            self._cnx.rm(cursor, pack(permutation))

    def ask(self, *items):
        assert len(items) == len(self._items)
        # XXX: that index is always part of the list of indices see
        # hoply.indices.
        index = tuple(range(len(self._items)))
        cursor = self._cursors[index]
        out = self._cnx.search(pack(items))
        cursor.reset()
        return out

    def FROM(self, *pattern, seed=Map()):  # seed is immutable
        """Yields bindings that match pattern"""
        assert len(pattern) == len(self._items)
        variable = tuple(isinstance(x, Variable) for x in pattern)
        # find the first index suitable for the query
        combination = tuple(x for x in range(len(self._items)) if not variable[x])
        for index, table in self._cursors.items():
            if is_permutation_prefix(combination, index):
                break
        else:
            raise HoplyException("oops!")
        # index variable holds the permutation suitable for the query
        table = self._tables[index]
        with self._cnx.cursor(table) as cursor:
            prefix = tuple(x for x in pattern if not isinstance(x, Variable))
            for key in self._cnx.range(cursor, pack(prefix)):
                items = unpack(key)
                # re-order the items
                items = tuple(items[index.index(i)] for i in range(len(self._items)))
                bindings = seed
                for i, item in enumerate(pattern):
                    if isinstance(item, Variable):
                        bindings = bindings.set(item.name, items[i])
                yield bindings

    def where(self, *pattern):
        assert len(pattern) == len(self._items)

        def _where(iterator):
            for bindings in iterator:
                # bind PATTERN against BINDINGS
                bound = []
                for item in pattern:
                    # if item is variable try to bind
                    if isinstance(item, Variable):
                        try:
                            value = bindings[item.name]
                        except KeyError:
                            bound.append(item)
                        else:
                            # pick the value in bindings
                            bound.append(value)
                    else:
                        # otherwise keep item as is
                        bound.append(item)
                # hey!
                yield from self.FROM(*bound, seed=bindings)

        return _where


open = Hoply


class Transaction(HoplyBase):
    def __init__(self, hoply):
        self._hoply = hoply
        self.where = hoply.where
        self.FROM = hoply.FROM
        self.ask = hoply.ask
        self.rm = hoply.rm


def transactional(func):
    """Run query in a thread

    This will start when appropriate a transaction and run it in a
    thread.  This can be composed, coroutines decorated with
    transactional can call other coroutines decorated with
    transactional.

    Nevertheless, transaction are supposed to stay short because even
    if they release the Global Interpreter Lock when they enter C
    land, they will block the main thread when it is in the Python
    interpreter. Otherwise said, avoid asynchronous io inside
    transactional functions.

    """
    # inspired from foundationdb python bindings
    spec = inspect.getfullargspec(func)
    try:
        # XXX: unlike FDB bindings, transaction name can not be changed
        # XXX: unlike FDB bindings, 'tr' can not be passed as a keyword
        index = spec.args.index("tr")
    except ValueError:
        msg = "the decorator @transactional expect one of the argument to be name 'tr'"
        raise NameError(msg)

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        db_or_tr = args[
            index
        ]  # in general, index == 0 or in case of methods index == 1
        if isinstance(db_or_tr, Transaction):
            out = await func(*args, **kwargs)
            return out
        else:
            args = list(args)
            args[index] = tr = Transaction(db_or_tr)
            loop = asyncio.get_event_loop()
            tr._hoply._cnx.begin()
            try:
                # func is mix of IO and CPU but because wiredtiger
                # doesn't work in read-write multiprocessing context,
                # fallback to threads.
                out = await loop.run_in_executor(
                    None, functools.partial(func, *args, **kwargs)
                )
            except Exception:
                tr._hoply._cnx.rollback()
                raise
            else:
                tr._hoply._cnx.commit()
                return out

    return wrapper
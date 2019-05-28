#
# Copyright (C) 2015-2019  Amirouche Boubekki <amirouche.boubekki@gmail.com>
#
# https://github.com/amirouche/hoply/
#
import functools
import inspect
import logging
from itertools import permutations

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


var = Variable  # XXX: use only 'var' in 'where' queries please!


def stringify(list):
    return "".join(str(x) for x in list)


def is_permutation_prefix(combination, index):
    index = stringify(index)
    out = any(index.startswith(stringify(x)) for x in permutations(combination))
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
        assert len(items) == len(self._items), "invalid item count"
        for index, cursor in self._cursors.items():
            permutation = tuple(items[i] for i in index)
            self._cnx.add(cursor, pack(permutation))

    def rm(self, *items):
        assert len(items) == len(self._items), "invalid item count"
        for index, cursor in self._cursors.items():
            permutation = tuple(items[i] for i in index)
            self._cnx.rm(cursor, pack(permutation))

    def ask(self, *items):
        assert len(items) == len(self._items), "invalid item count"
        # XXX: that index is always part of the list of indices see
        # hoply.indices.
        index = tuple(range(len(self._items)))
        cursor = self._cursors[index]
        out = self._cnx.search(cursor, pack(items))
        return out

    def FROM(self, *pattern, seed=Map()):  # seed is immutable
        """Yields bindings that match pattern"""
        assert len(pattern) == len(self._items), "invalid item count"
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
            prefix = tuple(
                pattern[i] for i in index if not isinstance(pattern[i], Variable)
            )
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
        assert len(pattern) == len(self._items), "invalid item count"

        def _where(iterator):
            for bindings in iterator:
                # bind PATTERN against BINDINGS
                bound = []
                for item in pattern:
                    # if ITEM is variable try to bind
                    if isinstance(item, Variable):
                        try:
                            value = bindings[item.name]
                        except KeyError:
                            # no bindings
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
        self.add = hoply.add
        self.rm = hoply.rm


def transactional(func):
    """Run query in a transaction

    This will start when appropriate a transaction.  This can be
    composed, functions decorated with transactional can call other
    coroutines decorated with transactional.

    Nevertheless, transaction are supposed to stay short because even
    if they release the Global Interpreter Lock when they enter C
    land, they will block the main thread when it is in the Python
    interpreter.

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
    def wrapper(*args, **kwargs):

        db_or_tr = args[
            index
        ]  # in general, index == 0 or in case of methods index == 1
        if isinstance(db_or_tr, Transaction):
            out = func(*args, **kwargs)
            out = out
            return out
        else:
            args = list(args)
            args[index] = tr = Transaction(db_or_tr)
            tr._hoply._cnx.begin()
            try:
                out = func(*args, **kwargs)
            except Exception:
                tr._hoply._cnx.rollback()
                raise
            else:
                tr._hoply._cnx.commit()
                return out

    return wrapper

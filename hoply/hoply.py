#
# Copyright (C) 2015-2019  Amirouche Boubekki <amirouche.boubekki@gmail.com>
#
# https://github.com/amirouche/hoply/
#
import logging
from itertools import permutations
from contextlib import contextmanager

from immutables import Map

from hoply.tuple import pack
from hoply.tuple import unpack
from hoply.indices import compute_indices


log = logging.getLogger(__name__)


# helpers


def take(iterator, count):
    for _ in range(count):
        out = next(iterator)
        yield out


def drop(iterator, count):
    for _ in range(count):
        next(iterator)
    yield from iterator


# hoply! hoply! hoply!


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
    def __init__(self, name, prefix, items):
        self.name = name
        self._prefix = prefix
        self._items = items
        self._indices = compute_indices(len(items))

    def add(self, transaction, *items):
        """Add ITEMS to the associated database"""
        assert len(items) == len(self._items), "invalid item count"
        for subspace, index in enumerate(self._indices):
            permutation = list(items[i] for i in index)
            key = self._prefix + [subspace] + permutation
            transaction.add(pack(key), b"")

    def remove(self, transaction, *items):
        """Remove ITEMS from the associated database"""
        assert len(items) == len(self._items), "invalid item count"
        for subspace, index in enumerate(self._indices):
            permutation = list(items[i] for i in index)
            key = self._prefix + [subspace] + permutation
            transaction.remove(pack(key))

    def ask(self, transaction, *items):
        """Return True if ITEMS is found in the associated database"""
        assert len(items) == len(self._items), "invalid item count"
        subspace = 0
        key = self._prefix + [subspace] + list(items)
        out = transaction.get(pack(key))
        out = out is not None
        return out

    def FROM(self, transaction, *pattern, seed=Map()):  # seed is immutable
        """Yields bindings that match PATTERN"""
        assert len(pattern) == len(self._items), "invalid item count"
        variable = tuple(isinstance(x, Variable) for x in pattern)
        # find the first index suitable for the query
        combination = tuple(x for x in range(len(self._items)) if not variable[x])
        for subspace, index in enumerate(self._indices):
            if is_permutation_prefix(combination, index):
                break
        else:
            raise HoplyException("oops!")
        # `index` variable holds the permutation suitable for the
        # query. `subspace` is the "prefix" of that index.
        prefix = list(pattern[i] for i in index if not isinstance(pattern[i], Variable))
        prefix = self._prefix + [subspace] + prefix
        for key, _ in transaction.prefix(pack(prefix)):
            items = unpack(key)[len(self._prefix) + 1 :]
            # re-order the items
            items = tuple(items[index.index(i)] for i in range(len(self._items)))
            bindings = seed
            for i, item in enumerate(pattern):
                if isinstance(item, Variable):
                    bindings = bindings.set(item.name, items[i])
            yield bindings

    def where(self, tr, *pattern):
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
                yield from self.FROM(tr, *bound, seed=bindings)

        return _where


open = Hoply


@contextmanager
def transaction(storage):
    tr = storage.begin()
    try:
        yield tr
    except Exception as exc:  # noqa
        tr.rollback()
        raise
    finally:
        tr.commit()


def select(seed, *wheres):
    out = seed
    for where in wheres:
        out = where(out)
    return out

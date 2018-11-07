# hoply - python triple store database for exploring bigger than
# memory relational graph data
#
# Copyright (C) 2015-2018  Amirouche Boubekki <amirouche@hypermove.net>
#
import logging
from contextlib import contextmanager
from uuid import uuid4

from immutables import Map

from hoply.base import HoplyException


log = logging.getLogger(__name__)


def uid():
    return uuid4()


class Hoply:

    def __init__(self, store):
        self._store = store

        store.open()

        # transaction
        self.begin = store.begin
        self.commit = store.commit
        self.rollback = store.rollback

        # garbage in, garbage out
        self.add = store.add
        self.delete = store.delete
        self._spo_cursor = store._spo_cursor
        self._pos_cursor = store._pos_cursor
        self._prefix = store._prefix

        # don't forget to close the store
        self.close = store.close

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    @contextmanager
    def transaction(self):
        self.begin()
        try:
            yield
        except Exception as exc:
            self.rollback()
            raise
        else:
            self.commit()

    def _where(self, pattern, iterator):
        # cache
        if iterator is None:
            log.debug("where: 'iterator' is 'None'")
            # If the iterator is None then it's a seed step.
            vars = pattern.is_variables()
            # Generate bindings for the pattern.
            if vars == (True, False, False):
                log.debug("where: only 'subject' is a variable")
                # cache
                predicatex = pattern.predicate
                objectx = pattern.object
                # start range prefix search
                with self._pos_cursor() as cursor:
                    for _, _, subject in self._prefix(cursor, predicatex, objectx):
                        # yield binding with subject set to its
                        # variable
                        binding = Map().set(pattern.subject.name, subject)
                        yield binding

            elif vars == (False, True, True):
                log.debug('where: subject is NOT a variable')
                # cache
                subjectx = pattern.subject
                # start range prefix search
                with self._spo_cursor() as cursor:
                    for _, predicate, object in self._prefix(cursor, subjectx, ''):
                        # yield binding where predicate and object are
                        # set to their variables
                        binding = Map()
                        binding = binding.set(pattern.predicate.name, predicate)
                        binding = binding.set(pattern.object.name, object)
                        yield binding

            elif vars == (False, False, True):
                log.debug('where: object is a variable')
                # cache
                subjectx = pattern.subject
                predicatex = pattern.predicate
                # start range prefix search
                with self._spo_cursor() as cursor:
                    for _, _, object in self._prefix(cursor, subjectx, predicatex):
                        # yield binding where object is set to its
                        # variable
                        binding = Map().set(pattern.object.name, object)
                        yield binding
            else:
                msg = 'Pattern not supported, '
                msg += 'create a bug report '
                msg += 'if you think that pattern should be supported: %r'
                msg = msg.format(vars)
                raise HoplyException(msg)

        else:
            log.debug("where: 'iterator' is not 'None'")
            for binding in iterator:
                bound = pattern.bind(binding)
                log.debug("where: bound is %r", bound)
                vars = bound.is_variables()
                if vars == (False, False, False):
                    log.debug('fully bound pattern')
                    # fully bound pattern, check that it really exists
                    with self._spo_cursor() as cursor:
                            yield binding

                elif vars == (False, False, True):
                    log.debug("where: only 'object' is a variable")
                    # cache
                    subjectx = bound.subject
                    predicatex = bound.predicate
                    # start range prefix search
                    with self._spo_cursor() as cursor:
                        for _, _, object in self._prefix(cursor, subjectx, predicatex):
                            new = binding.set(bound.object.name, object)
                            yield new

                elif vars == (True, False, False):
                    log.debug("where: subject is variable")
                    # cache
                    predicatex = bound.predicate
                    objectx = bound.object
                    # start range prefix search
                    with self._pos_cursor() as cursor:
                        for _, _, subject in self._prefix(cursor, predicatex, objectx):
                            new = binding.set(pattern.subject.name, subject)
                            yield new
                else:
                    msg = 'Pattern not supported, '
                    msg += 'create a bug report '
                    msg += 'if you think that pattern should be supported: {}'
                    msg = msg.format(vars)
                    raise HoplyException(msg)


open = Hoply

class HoplyException(Exception):
    pass


class AbstractStore:

    # basics

    def open(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    # transaction

    def begin(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def rollback(self):
        raise NotImplementedError

    # cursor stuff

    def _spo_cursor(self):
        raise NotImplementedError

    def _pos_cursor(self):
        raise NotImplementedError

    def _prefix(self, cursor, a, b):
        raise NotImplementedError

    def _exists(self, cursor, a, b, c):
        raise NotImplementedError

    # garbage in, garbage out

    def add(self, subject, predicate, object):
        raise NotImplementedError

    def delete(self, subject, predicate, object):
        raise NotImplementedError


class var:

    __slots__ = ('name',)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<var %r>' % self.name


class Triple:

    __slots__ = ('subject', 'predicate', 'object')

    def __init__(self, subject, predicate, object):
        self.subject = subject
        self.predicate = predicate
        self.object = object

    def is_variables(self):
        return (
            isinstance(self.subject, var),
            isinstance(self.predicate, var),
            isinstance(self.object, var),
        )

    def bind(self, binding):
        subject = self.subject
        predicate = self.predicate
        object = self.object
        if isinstance(subject, var) and binding.get(subject.name) is not None:
            subject = binding[subject.name]
        if isinstance(predicate, var) and binding.get(predicate.name) is not None:
            predicate = binding[predicate.name]
        if isinstance(object, var) and binding.get(object.name) is not None:
            object = binding[object.name]
        return Triple(subject, predicate, object)

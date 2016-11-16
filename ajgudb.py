# AjuDB - graphdb for exploring your connected data
# Copyright (C) 2015-2016 Amirouche Boubekki <amirouche@hypermove.net>
from itertools import imap

from collections import namedtuple
from collections import Counter

from wiredtiger import wiredtiger_open
from json import loads
from json import dumps


def pk(*args):
    print args
    return args[-1]


VERTEX_KIND, EDGE_KIND = range(2)


class AjguDBException(Exception):
    pass


class Vertex(dict):

    __slots__ = ('uid')

    def __init__(self, **properties):
        self.uid = None
        super(Vertex, self).__init__(properties)

    def link(self, end, **properties):
        return Edge(self, end, properties)

    def __repr__(self):
        return '<Vertex uid:%s 0x%s>' % (self.uid, id(self))


class Edge(dict):

    __slots__ = ('uid', 'start', 'end')

    def __init__(self, start, end, properties):
        self.uid = None
        self.start = start
        self.end = end
        super(Edge, self).__init__(properties)

    def __repr__(self):
        return '<Edge uid:%s 0x%x>' % (self.uid, id(self))


WT_NOT_FOUND = -31803


class AjguDB(object):

    def __init__(self, path, logging=False):
        # init wiredtiger
        config = 'create,log=(enabled=true)' if logging else 'create'
        self._wiredtiger = wiredtiger_open(path, config)
        session = self._wiredtiger.open_session()

        # sequence table of uids
        session.create('table:uids', 'key_format=r,value_format=u')
        self._uids = session.open_cursor('table:uids', None, 'append')

        # tuples
        session.create('table:tuples', 'key_format=QS,value_format=S,columns=(uid,key,value)')
        self._tuples = session.open_cursor('table:tuples')

        # reversed index for (key, value) querying
        session.create('index:tuples:index', 'columns=(key,value)')
        self._reversed = session.open_cursor('index:tuples:index(uid)')
        
    def debug(self):
        self._tuples.reset()
        while self._tuples.next() != WT_NOT_FOUND:
            uid, key = self._tuples.get_key()
            value = loads(self._tuples.get_value())
            print uid, key, value

    def _next_uid(self):
        self._uids.set_value(b'')
        self._uids.insert()
        uid = self._uids.get_key()
        return uid

    def _delete(self, uid):
        # remove uid from uids table
        self._uids.set_key(uid)
        self._uids.search()
        self._uids.remove()
        self._uids.reset()

        # remove properties from tuples table
        self._tuples.set_key(uid, '')
        code = self._tuples.search_near()

        if code == WT_NOT_FOUND:
            self._tuples.reset()
            return
        elif code == -1:
            if self._tuples.next() == WT_NOT_FOUND:
                return

        while True:
            other_uid, other_key = self._tuples.get_key()
            if other_uid == uid:
                self._tuples.remove()
                if self._tuples.next() == WT_NOT_FOUND:
                    self._tuples.reset()
                    break
            else:
                self._tuples.reset()
                break

    def _update(self, uid, properties):
        self._delete(uid)
        for key, value in properties.items():
            self._tuples.set_key(uid, key)
            self._tuples.set_value(dumps(value))
            self._tuples.insert()

    def search(self, key, value):
        value = dumps(value)
        self._reversed.set_key(key, value)

        code = self._reversed.search()
        if code == WT_NOT_FOUND:
            self._reversed.reset()
            return list()

        def iterator():
            while True:
                other_key, other_value = self._reversed.get_key()
                if key == other_key and value == other_value:
                    yield self._reversed.get_value()
                    if self._reversed.next() == WT_NOT_FOUND:
                        self._reversed.reset()
                        break
                else:
                    self._reversed.reset()
                    break

        return list(iterator())

    def _key(self, uid, key):
        self._tuples.set_key(uid, key)
        if self._tuples.search() == 0:
            out = loads(self._tuples.get_value())
        else:
            out = None  # do not raise an exception or it will be painful
        self._tuples.reset()
        return out

    def delete(self, element):
        assert element.uid
        self._delete(element.uid)

    def get(self, uid):
        self._tuples.set_key(uid, '')
        code = self._tuples.search_near()

        if code == WT_NOT_FOUND:
            self._tuples.reset()
            return None
        elif code == -1:
            if self._tuples.next() == WT_NOT_FOUND:
                return None

        properties = dict()
        while True:
            other, key = self._tuples.get_key()
            if other == uid:
                value = self._tuples.get_value()
                properties[key] = loads(value)
                if self._tuples.next() == WT_NOT_FOUND:
                    self._tuples.reset()
                    break
            else:
                self._tuples.reset()
                break

        kind = properties.pop('__kind__')
        if kind == VERTEX_KIND:
            vertex = Vertex(**properties)
            vertex.uid = uid
            return vertex
        elif kind == EDGE_KIND:
            start = properties.pop('__start__')
            start = self.get(start)
            end = properties.pop('__end__')
            end = self.get(end)
            edge = Edge(start, end, properties)
            edge.uid = uid
            return edge

    def close(self):
        self._wiredtiger.close()

    def save(self, element):
        if isinstance(element, Vertex):
            if element.uid is not None:
                uid = element.uid
            else:
                uid = element.uid = self._next_uid()
            properties = dict(element)  # make a copy
            properties['__kind__'] = VERTEX_KIND
            self._update(uid, properties)
            return element
        elif isinstance(element, Edge):
            if element.uid is not None:
                uid = element.uid
            else:
                uid = element.uid = self._next_uid()
            properties = dict(element)
            properties['__kind__'] = EDGE_KIND
            if (element.start.uid is not None):
                start = element.start.uid
            else:
                start = self.save(element.start).uid
            properties['__start__'] = start
            if (element.end.uid is not None):
                end = element.end.uid
            else:
                end = self.save(element.end).uid
            properties['__end__'] = end
            self._update(uid, properties)
            return element
        else:
            msg = '%s is not supported' % type(element).__name__
            raise AjguDBException(msg)

    def get_or_create(self, element):
        if isinstance(element, Vertex):
            items = element.items()
            seed = {items[0][0]: items[0][1]}
            others = dict(items[1:])
            query = gremlin(FROM(**seed), where(**others), get)
            try:
                return False, next(iter(query(self)))
            except StopIteration:
                return True, self.save(element)
        else:
            raise NotImplementedError('FIXME')


GremlinResult = namedtuple('GremlinResult', ('value', 'parent'))


def gremlin(*steps):
    """Gremlin pipeline builder and executor"""

    def composed(ajgudb, iterator=None):
        if isinstance(iterator, Vertex) or isinstance(iterator, Edge):
            iterator = [GremlinResult(iterator.uid, None)]
        elif isinstance(iterator, GremlinResult):
            iterator = [iterator]
        # else it might be a GremlinResult iterator (or something else)
        #      but we don't care, if it must crash, it will crash!
        for step in steps:
            iterator = step(ajgudb, iterator)
        return iterator

    return composed

def VERTICES(ajgudb, _):
    """Seed step. Iterator over all vertices"""
    for uid in ajgudb.search('__kind__', VERTEX_KIND):
        yield GremlinResult(uid, None)

def EDGES(ajgudb, _):
    """Seed step. Iterator over all vertices"""
    for uid in ajgudb.search('__kind__', EDGE_KIND):
        yield GremlinResult(uid, None)

def FROM(**kwargs):
    """Seed step. Iterator over element of class ``klass`` that match
    ``kwargs`` where ``kwargs`` is a single `key=value` pair"""
    if len(kwargs.items()) > 1:
        raise Exception('Only one key/value pair is supported')

    key, value = kwargs.items()[0]

    def step(ajgudb, _):
        for uid in ajgudb.search(key, value):
            yield GremlinResult(uid, None)

    return step

def where(**kwargs):
    """Keep elements that match the ``kwargs`` specification.

    This step accepts uids as input."""

    def step(ajgudb, iterator):
        for item in iterator:
            for key, value in kwargs.items():
                other = ajgudb._key(item.value, key)
                # if the input ``value`` is different from ``other``
                # which is the value associated with (item.value, key)
                # then this item.value is not a match
                if value != other:
                    break
            else:
                # this a match!
                yield item

    return step

def skip(count):
    """Skip first ``count`` items"""
    def step(ajgudb, iterator):
        counter = 0
        for item in iterator:
            counter += 1
            if counter > count:
                yield item
    return step

def limit(count):
    """Keep only first ``count`` items"""
    def step(ajgudb, iterator):
        counter = 0
        for item in iterator:
            counter += 1
            yield item
            if counter == count:
                break
    return step

def paginator(count):
    """paginate..."""
    def step(ajgudb, iterator):
        counter = 0
        page = list()
        for item in iterator:
            page.append(item)
            counter += 1
            if counter == count:
                yield page
                counter = 0
                page = list()
        yield page
    return step

def count(ajgudb, iterator):
    """Count the number of items in the iterator"""
    return reduce(lambda x, y: x + 1, iterator, 0)

def key(name):
    """Get the value associated with ``key``.

    This accepts uids as input."""

    def step(ajgudb, iterator):
        for item in iterator:
            out = ajgudb._key(item.value, name)
            yield GremlinResult(out, item)

    return step

# edges navigation

def incomings(ajgudb, iterator):
    """Return the list of incomings edges.

    Accepts vertex uids as input"""
    for item in iterator:
        out = ajgudb.search('__end__', item.value)
        yield GremlinResult(out, item)



def outgoings(ajgudb, iterator):
    """Return the list of incomings edges.

    Accepts vertex uids as input"""
    for item in iterator:
        out = ajgudb.search('__start__', item.value)
        yield GremlinResult(out, item)


def start(ajgudb, iterator):
    """Return the start vertex of edges

    Accepts edge uids as input"""
    for item in iterator:
        out = ajgudb._key(item.value, '__start__')
        yield GremlinResult(out, item)

def end(ajgudb, iterator):
    """Return the end vertex of edges

    Accepts edge uids as input"""
    for item in iterator:
        out = ajgudb._key(item.value, '__end__')
        yield GremlinResult(out, item)

def gmap(func):
    def step(ajgudb, iterator):
        return imap(lambda x: GremlinResult(func(ajgudb, x.value), x), iterator)
    return step

def value(ajgudb, iterator):
    "Retrieve the content of the iterator"
    return map(lambda x: x.value, iterator)

def get(ajgudb, iterator):
    return imap(lambda x: ajgudb.get(x.value), iterator)

def sort(key=lambda g, x: x, reverse=False):
    def step(ajgudb, iterator):
        out = sorted(iterator, key=lambda x: key(ajgudb, x), reverse=reverse)
        return iter(out)
    return step

def unique(ajgudb, iterator):
    # from ActiveState (MIT)
    #
    #   Lazy Ordered Unique elements from an iterator
    #
    def __unique(iterable, key=None):
        seen = set()

        if key is None:
            # Optimize the common case
            for item in iterable:
                if item in seen:
                    continue
                seen.add(item)
                yield item

        else:
            for item in iterable:
                keyitem = key(item)
                if keyitem in seen:
                    continue
                seen.add(keyitem)
                yield item

    iterator = __unique(iterator, lambda x: x.value)
    return iterator

def gfilter(predicate):
    def step(ajgudb, iterator):
        for item in iterator:
            if predicate(ajgudb, item.value):
                yield item
    return step

def back(ajgudb, iterator):
    return imap(lambda x: x.parent, iterator)

def path(count):

    def path_(previous, _):
        previous.append(previous[-1].parent)
        return previous

    def step(ajgudb, iterator):
        for item in iterator:
            yield map(lambda x: x.value, reduce(path_, range(count - 1), [item]))

    return step

def mean(ajgudb, iterator):
    count = 0.
    total = 0.
    for item in iterator:
        total += item
        count += 1
    return total / count

def group_count(ajgudb, iterator):
    return Counter(map(lambda x: x.value, iterator))

def scatter(ajgudb, iterator):
    for item in iterator:
        for other in item.value:
            yield GremlinResult(other, item)

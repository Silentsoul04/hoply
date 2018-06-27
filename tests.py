#!/usr/bin/env python
import os
from shutil import rmtree
from unittest import TestCase

from ajgudb import AjguDB
from ajgudb import EDGES
from ajgudb import Edge
from ajgudb import FROM
from ajgudb import VERTICES
from ajgudb import Vertex
from ajgudb import back
from ajgudb import count
from ajgudb import end
from ajgudb import get
from ajgudb import gfilter
from ajgudb import gmap
from ajgudb import gremlin
from ajgudb import incomings
from ajgudb import key
from ajgudb import limit
from ajgudb import mean
from ajgudb import outgoings
from ajgudb import path
from ajgudb import scatter
from ajgudb import skip
from ajgudb import sort
from ajgudb import start
from ajgudb import trigrams
from ajgudb import unique
from ajgudb import value
from ajgudb import where


class TrigramTests(TestCase):

    def test_single_word(self):
        result = list(trigrams('abc'))
        expected = ['$ab', 'abc', 'bc$']
        self.assertEqual(result, expected)

    def test_two_words(self):
        result = list(trigrams('abc def'))
        expected = ['$ab', 'abc', 'bc$', '$de', 'def', 'ef$']
        self.assertEqual(result, expected)


class DatabaseTestCase(TestCase):

    def setUp(self):
        try:
            rmtree('/tmp/ajgudb')
        except OSError:
            pass
        os.makedirs('/tmp/ajgudb')
        self.graph = AjguDB('/tmp/ajgudb')

    def tearDown(self):
        self.graph.close()
        rmtree('/tmp/ajgudb')

    def test_create_emtpy_vertex(self):
        vertex = Vertex()
        self.graph.save(vertex)
        self.assertIsNotNone(vertex.uid)

    def test_delete_vertex(self):
        vertex = Vertex()
        self.graph.save(vertex)
        self.graph.delete(vertex)
        self.assertIsNotNone(vertex.uid)

    def test_get_vertex(self):
        vertex = Vertex()
        self.graph.save(vertex)
        print(vertex.uid)
        other = self.graph.get(vertex.uid)

        self.assertTrue(isinstance(other, Vertex))
        self.assertEqual(vertex.uid, other.uid)

    def test_create_vertex(self):
        vertex = Vertex(key='value')
        self.graph.save(vertex)
        self.assertIsNotNone(vertex.uid)

    def test_create_empty_edge(self):
        start = Vertex(key='value')
        end = Vertex(key='value')
        edge = start.link(end)
        self.graph.save(edge)
        self.assertIsNotNone(edge.uid)

    def test_create_edge(self):
        start = Vertex(key='value')
        end = Vertex(key='value')
        edge = start.link(end, key='value')
        self.graph.save(edge)
        self.assertIsNotNone(edge.uid)

    def test_get_edge(self):
        start = Vertex(key='value')
        end = Vertex(key='value')
        edge = start.link(end, key='value')
        self.graph.save(edge)
        other = self.graph.get(edge.uid)
        self.assertTrue(isinstance(other, Edge))
        self.assertEqual(other.uid, edge.uid)

    def test_not_found(self):
        self.assertIsNone(self.graph.get(42))

    def test_update_vertex(self):
        vertex = Vertex(key='value')
        self.graph.save(vertex)
        vertex['key'] = 'other'
        self.graph.save(vertex)
        other = self.graph.get(vertex.uid)
        self.assertEqual(other['key'], 'other')
        self.assertEqual(other.uid, vertex.uid)

    def test_gremlin_VERTICES(self):
        vertex = Vertex()
        self.graph.save(vertex)
        vertex = Vertex()
        self.graph.save(vertex)
        vertex = Vertex()
        self.graph.save(vertex)
        query = gremlin(VERTICES)
        length = len(list(query(self.graph)))
        self.assertEqual(length, 3)

    def test_gremlin_EDGES(self):
        vertex = Vertex()
        self.graph.save(vertex)
        self.graph.save(vertex.link(vertex))
        self.graph.save(vertex.link(vertex))
        self.graph.save(vertex.link(vertex))
        query = gremlin(EDGES)
        length = len(list(query(self.graph)))
        self.assertEqual(length, 3)

    def test_gremlin_FROM(self):
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        query = gremlin(FROM(key='value'))
        results = list(query(self.graph))
        length = len(results)
        self.assertEqual(length, 3)

    def test_gremlin_where(self):
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        query = gremlin(VERTICES, where(key='value'))
        results = list(query(self.graph))
        length = len(results)
        self.assertEqual(length, 3)

    def test_gremlin_skip(self):
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        query = gremlin(VERTICES, where(key='value'), skip(1))
        results = list(query(self.graph))
        length = len(results)
        self.assertEqual(length, 2)

    def test_gremlin_limit(self):
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        query = gremlin(VERTICES, where(key='value'), limit(1))
        length = len(list(query(self.graph)))
        self.assertEqual(length, 1)

    def test_gremlin_count(self):
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='value'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        self.graph.save(Vertex(key='other'))
        query = gremlin(VERTICES, count)
        number = query(self.graph)
        self.assertEqual(number, 7)

    def test_gremlin_key(self):
        self.graph.save(Vertex(key='value'))
        query = gremlin(VERTICES, key('key'))
        out = list(query(self.graph))[0].value
        self.assertEqual(out, 'value')

    def test_gremlin_incomings(self):
        vertex = Vertex()
        self.graph.save(vertex)
        self.graph.save(vertex.link(vertex))
        self.graph.save(vertex.link(vertex))
        self.graph.save(vertex.link(vertex))
        query = gremlin(incomings)
        length = len(list(query(self.graph, vertex))[0].value)
        self.assertEqual(length, 3)

    def test_gremlin_outgoings(self):
        vertex = Vertex()
        self.graph.save(vertex)
        self.graph.save(vertex.link(vertex))
        self.graph.save(vertex.link(vertex))
        self.graph.save(vertex.link(vertex))
        query = gremlin(outgoings)
        length = len(list(query(self.graph, vertex))[0].value)
        self.assertEqual(length, 3)

    def test_gremlin_start(self):
        vertex = Vertex()
        other = Vertex()
        edge = self.graph.save(vertex.link(other))
        query = gremlin(start)
        uid = list(query(self.graph, edge))[0].value
        self.assertEqual(uid, vertex.uid)

    def test_gremlin_end(self):
        vertex = Vertex()
        other = Vertex()
        edge = self.graph.save(vertex.link(other))
        query = gremlin(end)
        uid = list(query(self.graph, edge))[0].value
        self.assertEqual(uid, other.uid)

    def test_gremlin_gmap_and_value(self):
        self.graph.save(Vertex(weight=1.0))
        self.graph.save(Vertex(weight=2.0))
        query = gremlin(VERTICES, key('weight'), gmap(lambda g, x: x + 1), value)
        out = list(query(self.graph))
        self.assertEqual(out, [2.0, 3.0])

    def test_gremlin_get(self):
        self.graph.save(Vertex())
        self.graph.save(Vertex())
        query = gremlin(VERTICES, get)
        out = map(lambda x: x.uid, query(self.graph))
        self.assertEqual(out, [1, 2])

    def test_gremlin_sort(self):
        self.graph.save(Vertex(weight=3.0))
        self.graph.save(Vertex(weight=2.0))
        self.graph.save(Vertex(weight=4.0))
        self.graph.save(Vertex(weight=1.0))
        query = gremlin(VERTICES, key('weight'), sort(), value)
        out = list(query(self.graph))
        self.assertEqual(out, [1.0, 2.0, 3.0, 4.0])

    def test_gremlin_unique(self):
        self.graph.save(Vertex(weight=4.0))
        self.graph.save(Vertex(weight=1.0))
        self.graph.save(Vertex(weight=4.0))
        self.graph.save(Vertex(weight=1.0))
        query = gremlin(VERTICES, key('weight'), unique, value)
        out = list(query(self.graph))
        self.assertEqual(out, [4.0, 1.0])

    def test_gremlin_gfilter(self):
        self.graph.save(Vertex(weight=3.0))
        self.graph.save(Vertex(weight=2.0))
        self.graph.save(Vertex(weight=4.0))
        self.graph.save(Vertex(weight=1.0))
        query = gremlin(VERTICES, key('weight'), gfilter(lambda g, x: x > 2), value)
        out = list(query(self.graph))
        self.assertEqual(out, [3.0, 4.0])

    def test_gremlin_back(self):
        a = self.graph.save(Vertex(weight=3.0))
        self.graph.save(Vertex(weight=2.0))
        c = self.graph.save(Vertex(weight=4.0))
        self.graph.save(Vertex(weight=1.0))
        query = gremlin(VERTICES, key('weight'), gfilter(lambda g, x: x > 2), back, value)
        out = list(query(self.graph))
        self.assertEqual(out, [a.uid, c.uid])

    def test_gremlin_mean(self):
        self.graph.save(Vertex(weight=3.0))
        self.graph.save(Vertex(weight=2.0))
        self.graph.save(Vertex(weight=4.0))
        self.graph.save(Vertex(weight=1.0))
        query = gremlin(VERTICES, key('weight'), value, mean)
        out = query(self.graph)
        self.assertEqual(out, 2.5)

    def test_gremlin_path(self):
        vertex = self.graph.save(Vertex(weight=42))
        query = gremlin(key('weight'), gmap(lambda g, x: x + 1), path(3))
        out = list(query(self.graph, vertex))
        self.assertEqual(out, [[43, 42, 1]])

    def test_get_or_create(self):
        new, vertex = self.graph.get_or_create(Vertex(key='value'))
        self.assertTrue(new)
        self.assertIsNotNone(vertex.uid)

    def test_get_or_create_2(self):
        vertex = self.graph.save(Vertex(key='value'))
        new, other = self.graph.get_or_create(Vertex(key='value'))
        self.assertFalse(new)
        self.assertEqual(vertex.uid, other.uid)

    def test_gremlin_scatter(self):
        a = self.graph.save(Vertex())
        b = self.graph.save(Vertex())
        c = self.graph.save(Vertex())
        d = self.graph.save(Vertex())
        self.graph.save(a.link(b))
        self.graph.save(a.link(c))
        self.graph.save(a.link(d))
        query = gremlin(outgoings, scatter, end, value)
        out = list(query(self.graph, a))
        self.assertEqual(out, [2, 3, 4])

    def test_like_single_item_in_db(self):
        self.graph.fuzzy_index(Vertex(), 'abc')
        self.assertEqual(self.graph.like('abc'), [(1, 0)])

    def test_like_two_items_in_db(self):
        self.graph.fuzzy_index(Vertex(), 'abc')
        self.graph.fuzzy_index(Vertex(), 'def')
        self.assertEqual(self.graph.like('abc'), [(1, 0)])

    def test_like_abcdef_one(self):
        self.graph.fuzzy_index(Vertex(), 'abc')
        self.graph.fuzzy_index(Vertex(), 'def')
        self.assertEqual(self.graph.like('abcdef'), [(1, -3), (2, -3)])

    def test_like_abcdef_two(self):
        self.graph.fuzzy_index(Vertex(), 'abcdef')
        self.graph.fuzzy_index(Vertex(), 'def')
        self.assertEqual(self.graph.like('abcdef'), [(1, 0), (2, -3)])

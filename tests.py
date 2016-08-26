#!/usr/bin/env python
import os
from shutil import rmtree
from unittest import TestCase

from ajgudb import *


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

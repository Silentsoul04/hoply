import logging
import os
from shutil import rmtree

import daiquiri
import pytest
import hoply as h


TEST_DIRECTORY = "/tmp/hoply-tests/"


daiquiri.setup(logging.DEBUG, outputs=('stderr',))


@pytest.fixture
def db():
    if os.path.exists(TEST_DIRECTORY):
        rmtree(TEST_DIRECTORY)
    os.makedirs(TEST_DIRECTORY)
    db = h.open(TEST_DIRECTORY)

    yield db

    db.close()
    rmtree(TEST_DIRECTORY)


def test_nop(db):
    assert db


def test_simple_single_item_db_subject_lookup(db):
    expected = h.uid()
    db.add(expected, 'title', 'hyperdev.fr')
    query = h.compose(h.where(h.var('subject'), 'title', 'hyperdev.fr'))
    out = list(query(db))
    out = out[0]['subject']
    assert out == expected


def test_simple_multiple_items_db_subject_lookup(db):
    expected = h.uid()
    db.add(expected, 'title', 'hyperdev.fr')
    db.add(h.uid(), 'title', 'blog.dolead.com')
    db.add(h.uid(), 'title', 'julien.danjou.info')
    query = h.compose(h.where(h.var('subject'), 'title', 'hyperdev.fr'))
    out = list(query(db))
    out = out[0]['subject']
    assert out == expected


def test_complex(db):
    hyperdev = h.uid()
    db.add(hyperdev, 'title', 'hyperdev.fr')
    db.add(hyperdev, 'keyword', 'scheme')
    db.add(hyperdev, 'keyword', 'hacker')
    dolead = h.uid()
    db.add(dolead, 'title', 'blog.dolead.com')
    db.add(dolead, 'keyword', 'corporate')
    julien = h.uid()
    db.add(julien, 'title', 'julien.danjou.info')
    db.add(julien, 'keyword', 'python')
    db.add(julien, 'keyword', 'hacker')
    query = h.compose(
        h.where(h.var('subject'), 'keyword', 'hacker'),
        h.where(h.var('subject'), 'title', h.var('blog')),
    )
    out = query(db)
    out = sorted([x['blog'] for x in out])
    assert out == ['hyperdev.fr', 'julien.danjou.info']


def test_seed_subject_variable(db):
    hyperdev = h.uid()
    db.add(hyperdev, 'title', 'hyperdev.fr')
    db.add(hyperdev, 'keyword', 'scheme')
    db.add(hyperdev, 'keyword', 'hacker')
    dolead = h.uid()
    db.add(dolead, 'title', 'blog.dolead.com')
    db.add(dolead, 'keyword', 'corporate')
    julien = h.uid()
    db.add(julien, 'title', 'julien.danjou.info')
    db.add(julien, 'keyword', 'python')
    db.add(julien, 'keyword', 'hacker')
    query = h.compose(
        h.where(h.var('subject'), 'keyword', 'corporate'),
    )
    out = query(db)
    out = list(out)[0]['subject']
    assert out == dolead


def test_seed_subject_lookup(db):
    hyperdev = h.uid()
    db.add(hyperdev, 'title', 'hyperdev.fr')
    db.add(hyperdev, 'keyword', 'scheme')
    db.add(hyperdev, 'keyword', 'hacker')
    dolead = h.uid()
    db.add(dolead, 'title', 'blog.dolead.com')
    db.add(dolead, 'keyword', 'corporate')
    julien = h.uid()
    db.add(julien, 'title', 'julien.danjou.info')
    db.add(julien, 'keyword', 'python')
    db.add(julien, 'keyword', 'hacker')
    query = h.compose(
        h.where(dolead, h.var('key'), h.var('value')),
    )
    out = query(db)
    out = [dict(x) for x in out]
    expected = [
        {'key': '"keyword"', 'value': '"corporate"'},
        {'key': '"title"', 'value': '"blog.dolead.com"'}
    ]
    assert out == expected


def test_seed_object_variable(db):
    hyperdev = h.uid()
    db.add(hyperdev, 'title', 'hyperdev.fr')
    db.add(hyperdev, 'keyword', 'scheme')
    db.add(hyperdev, 'keyword', 'hacker')
    dolead = h.uid()
    db.add(dolead, 'title', 'blog.dolead.com')
    db.add(dolead, 'keyword', 'corporate')
    julien = h.uid()
    db.add(julien, 'title', 'julien.danjou.info')
    db.add(julien, 'keyword', 'python')
    db.add(julien, 'keyword', 'hacker')
    query = h.compose(
        h.where(hyperdev, 'title', h.var('title')),
    )
    out = query(db)
    out = list(out)[0]['title']
    assert out == 'hyperdev.fr'


def test_subject_variable(db):
    # prepare
    hyperdev = h.uid()
    db.add(hyperdev, 'title', 'hyperdev.fr')
    db.add(hyperdev, 'keyword', 'scheme')
    db.add(hyperdev, 'keyword', 'hacker')
    post1 = h.uid()
    db.add(post1, 'blog', hyperdev)
    db.add(post1, 'title', 'hoply is awesome')
    post2 = h.uid()
    db.add(post2, 'blog', hyperdev)
    db.add(post2, 'title', 'hoply triple store')

    # exec, fetch all blog title from hyperdev.fr
    query = h.compose(
        h.where(h.var('blog'), 'title', 'hyperdev.fr'),
        h.where(h.var('post'), 'blog', h.var('blog')),
        h.where(h.var('post'), 'title', h.var('title')),
    )
    out = query(db)
    out = sorted([x['title'] for x in out])
    assert out == ['hoply is awesome', 'hoply triple store']


def test_skip():
    out = list(h.skip(3)(None, range(5)))
    assert out == [3, 4]


def test_limit():
    out = list(h.limit(2)(None, range(5)))
    assert out == [0, 1]


def test_paginator():
    out = list(h.paginator(2)(None, range(5)))
    assert out == [[0, 1], [2, 3], [4]]


def test_count():
    out = h.count(None, range(5))
    assert out == 5


def test_map():
    out = list(h.map(lambda hoply, x: x + 1)(None, [1]))
    assert out == [2]


def test_unique():
    out = list(h.unique(None, [1] * 10))
    assert out == [1]


def test_mean():
    out = h.mean(None, range(5))
    assert out == 2.0

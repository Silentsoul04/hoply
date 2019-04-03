import logging
import os
import uuid
from shutil import rmtree
from uuid import uuid4

import daiquiri
import pytest
import hoply as h
from hoply.leveldb import LevelDBConnexion
from hoply.memory import MemoryConnexion
from hoply.tuple import pack
from hoply.tuple import unpack
from cffi import FFI


TEST_DIRECTORY = "/tmp/hoply-tests/"


daiquiri.setup(logging.DEBUG, outputs=("stderr",))


def test_pack_unpack():
    expected = (
        2 ** 256,
        -2 ** 256,
        True,
        False,
        None,
        3.1415,
        1337,
        uuid.uuid4(),
        b"42",
        "hello",
        0,
        -1,
    )
    out = unpack(FFI().from_buffer(pack(expected)))
    assert expected == out


STORES = [MemoryConnexion, LevelDBConnexion]


@pytest.fixture
def path():
    if os.path.exists(TEST_DIRECTORY):
        rmtree(TEST_DIRECTORY)
    os.makedirs(TEST_DIRECTORY)

    return TEST_DIRECTORY


def TripleStoreDB(cnx):
    out = h.open(cnx, "hoply-test", ("subject", "predicate", "object"))
    return out


@pytest.mark.parametrize("store_class", STORES)
def test_nop(store_class, path):
    with TripleStoreDB(store_class(path)) as db:
        assert db


@pytest.mark.parametrize("store_class", STORES)
def test_simple_single_item_db_subject_lookup(store_class, path):
    with TripleStoreDB(store_class(path)) as db:
        expected = uuid4()
        db.add(expected, "title", "hyperdev.fr")
        query = db.FROM(h.var("subject"), "title", "hyperdev.fr")
        out = list(query)
        out = out[0]["subject"]
        assert out == expected


@pytest.mark.parametrize("store_class", STORES)
def test_ask_rm_and_ask(store_class, path):
    with TripleStoreDB(store_class(path)) as db:
        expected = uuid4()
        db.add(expected, "title", "hyperdev.fr")
        assert db.ask(expected, "title", "hyperdev.fr")
        db.rm(expected, "title", "hyperdev.fr")
        assert not db.ask(expected, "title", "hyperdev.fr")


@pytest.mark.parametrize("store_class", STORES)
def test_simple_multiple_items_db_subject_lookup(store_class, path):
    with TripleStoreDB(store_class(path)) as db:
        expected = uuid4()
        db.add(expected, "title", "hyperdev.fr")
        db.add(uuid4(), "title", "blog.dolead.com")
        db.add(uuid4(), "title", "julien.danjou.info")
        query = db.FROM(h.var("subject"), "title", "hyperdev.fr")
        out = list(query)
        out = out[0]["subject"]
        assert out == expected


@pytest.mark.parametrize("store_class", STORES)
def test_complex(store_class, path):
    with TripleStoreDB(store_class(path)) as db:
        hyperdev = uuid4()
        db.add(hyperdev, "title", "hyperdev.fr")
        db.add(hyperdev, "keyword", "scheme")
        db.add(hyperdev, "keyword", "hacker")
        dolead = uuid4()
        db.add(dolead, "title", "blog.dolead.com")
        db.add(dolead, "keyword", "corporate")
        julien = uuid4()
        db.add(julien, "title", "julien.danjou.info")
        db.add(julien, "keyword", "python")
        db.add(julien, "keyword", "hacker")
        out = h.compose(
            db.FROM(h.var("identifier"), "keyword", "hacker"),
            db.where(h.var("identifier"), "title", h.var("blog")),
        )
        out = sorted([x["blog"] for x in out])
        assert out == ["hyperdev.fr", "julien.danjou.info"]


@pytest.mark.parametrize("store_class", STORES)
def test_seed_subject_variable(store_class, path):
    with TripleStoreDB(store_class(path)) as db:
        hyperdev = uuid4()
        db.add(hyperdev, "title", "hyperdev.fr")
        db.add(hyperdev, "keyword", "scheme")
        db.add(hyperdev, "keyword", "hacker")
        dolead = uuid4()
        db.add(dolead, "title", "blog.dolead.com")
        db.add(dolead, "keyword", "corporate")
        julien = uuid4()
        db.add(julien, "title", "julien.danjou.info")
        db.add(julien, "keyword", "python")
        db.add(julien, "keyword", "hacker")
        query = db.FROM(h.var("subject"), "keyword", "corporate")
        out = list(query)[0]["subject"]
        assert out == dolead


@pytest.mark.parametrize("store_class", STORES)
def test_seed_subject_lookup(store_class, path):
    with TripleStoreDB(store_class(path)) as db:
        hyperdev = uuid4()
        db.add(hyperdev, "title", "hyperdev.fr")
        db.add(hyperdev, "keyword", "scheme")
        db.add(hyperdev, "keyword", "hacker")
        dolead = uuid4()
        db.add(dolead, "title", "blog.dolead.com")
        db.add(dolead, "keyword", "corporate")
        julien = uuid4()
        db.add(julien, "title", "julien.danjou.info")
        db.add(julien, "keyword", "python")
        db.add(julien, "keyword", "hacker")
        query = db.FROM(dolead, h.var("key"), h.var("value"))
        out = [dict(x) for x in query]
        expected = [
            {"key": "keyword", "value": "corporate"},
            {"key": "title", "value": "blog.dolead.com"},
        ]
        assert out == expected


@pytest.mark.parametrize("store_class", STORES)
def test_seed_object_variable(store_class, path):
    with TripleStoreDB(store_class(path)) as db:
        hyperdev = uuid4()
        db.add(hyperdev, "title", "hyperdev.fr")
        db.add(hyperdev, "keyword", "scheme")
        db.add(hyperdev, "keyword", "hacker")
        dolead = uuid4()
        db.add(dolead, "title", "blog.dolead.com")
        db.add(dolead, "keyword", "corporate")
        julien = uuid4()
        db.add(julien, "title", "julien.danjou.info")
        db.add(julien, "keyword", "python")
        db.add(julien, "keyword", "hacker")
        query = db.FROM(hyperdev, "title", h.var("title"))
        out = list(query)[0]["title"]
        assert out == "hyperdev.fr"


@pytest.mark.parametrize("store_class", STORES)
def test_subject_variable(store_class, path):
    with TripleStoreDB(store_class(path)) as db:
        # prepare
        hyperdev = uuid4()
        db.add(hyperdev, "title", "hyperdev.fr")
        db.add(hyperdev, "keyword", "scheme")
        db.add(hyperdev, "keyword", "hacker")
        post1 = uuid4()
        db.add(post1, "blog", hyperdev)
        db.add(post1, "title", "hoply is awesome")
        post2 = uuid4()
        db.add(post2, "blog", hyperdev)
        db.add(post2, "title", "hoply triple store")

        # exec, fetch all blog title from hyperdev.fr
        out = h.compose(
            db.FROM(h.var("blog"), "title", "hyperdev.fr"),
            db.where(h.var("post"), "blog", h.var("blog")),
            db.where(h.var("post"), "title", h.var("title")),
        )
        out = sorted([x["title"] for x in out])
        assert out == ["hoply is awesome", "hoply triple store"]


def test_skip():
    out = list(h.skip(3)(range(5)))
    assert out == [3, 4]


def test_limit():
    out = list(h.limit(2)(range(5)))
    assert out == [0, 1]


def test_paginator():
    out = list(h.paginator(2)(range(5)))
    assert out == [[0, 1], [2, 3], [4]]


def test_count():
    out = h.count(range(5))
    assert out == 5


def test_map():
    out = list(h.map(lambda x: x + 1)([1]))
    assert out == [2]


def test_unique():
    out = list(h.unique([1] * 10))
    assert out == [1]


def test_mean():
    out = h.mean(range(5))
    assert out == 2.0

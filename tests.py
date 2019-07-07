import logging
import os
import uuid
from shutil import rmtree
from uuid import uuid4

import daiquiri
import pytest
import hoply as h
from hoply.okvs.wiredtiger import WiredTiger
from hoply.okvs.memory import Memory
from hoply.okvs.leveldb import LevelDB
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


STORES = [Memory, LevelDB, WiredTiger]


@pytest.fixture
def path():
    if os.path.exists(TEST_DIRECTORY):
        rmtree(TEST_DIRECTORY)
    os.makedirs(TEST_DIRECTORY)

    return TEST_DIRECTORY


triplestore = h.open("hoply-test", [42], ("subject", "predicate", "object"))
quadstore = h.open("hoply-test", [43], ("collection", "identifer", "key", "value"))


@pytest.mark.parametrize("storage_class", STORES)
def test_nop(storage_class, path):
    with storage_class(path) as storage:
        assert storage


@pytest.mark.parametrize("storage_class", STORES)
def test_simple_single_item_db_subject_lookup(storage_class, path):
    with storage_class(path) as storage:
        expected = uuid4()
        with h.transaction(storage) as tr:
            triplestore.add(tr, expected, "title", "hyperdev.fr")
        with h.transaction(storage) as tr:
            query = triplestore.FROM(tr, h.var("subject"), "title", "hyperdev.fr")
            out = list(query)
            out = out[0]["subject"]
        assert out == expected


@pytest.mark.parametrize("storage_class", STORES)
def test_ask_rm_and_ask(storage_class, path):
    with storage_class(path) as storage:
        expected = uuid4()
        with h.transaction(storage) as tr:
            triplestore.add(tr, expected, "title", "hyperdev.fr")
        with h.transaction(storage) as tr:
            assert triplestore.ask(tr, expected, "title", "hyperdev.fr")
        with h.transaction(storage) as tr:
            triplestore.remove(tr, expected, "title", "hyperdev.fr")
        with h.transaction(storage) as tr:
            assert not triplestore.ask(tr, expected, "title", "hyperdev.fr")


@pytest.mark.parametrize("storage_class", STORES)
def test_simple_multiple_items_db_subject_lookup(storage_class, path):
    with storage_class(path) as storage:
        expected = uuid4()
        with h.transaction(storage) as tr:
            triplestore.add(tr, expected, "title", "hyperdev.fr")
            triplestore.add(tr, uuid4(), "title", "blog.dolead.com")
            triplestore.add(tr, uuid4(), "title", "julien.danjou.info")
        with h.transaction(storage) as tr:
            query = triplestore.FROM(tr, h.var("subject"), "title", "hyperdev.fr")
            out = list(query)
            out = out[0]["subject"]
        assert out == expected


@pytest.mark.parametrize("storage_class", STORES)
def test_complex(storage_class, path):
    with storage_class(path) as storage:
        hyperdev = uuid4()
        with h.transaction(storage) as tr:
            triplestore.add(tr, hyperdev, "title", "hyperdev.fr")
            triplestore.add(tr, hyperdev, "keyword", "scheme")
            triplestore.add(tr, hyperdev, "keyword", "hacker")
            dolead = uuid4()
            triplestore.add(tr, dolead, "title", "blog.dolead.com")
            triplestore.add(tr, dolead, "keyword", "corporate")
            julien = uuid4()
            triplestore.add(tr, julien, "title", "julien.danjou.info")
            triplestore.add(tr, julien, "keyword", "python")
            triplestore.add(tr, julien, "keyword", "hacker")
        with h.transaction(storage) as tr:
            out = h.select(
                triplestore.FROM(tr, h.var("identifier"), "keyword", "hacker"),
                triplestore.where(tr, h.var("identifier"), "title", h.var("blog")),
            )
            out = sorted([x["blog"] for x in out])
        assert out == ["hyperdev.fr", "julien.danjou.info"]


@pytest.mark.parametrize("storage_class", STORES)
def test_seed_subject_variable(storage_class, path):
    with storage_class(path) as storage:
        with h.transaction(storage) as tr:
            hyperdev = uuid4()
            triplestore.add(tr, hyperdev, "title", "hyperdev.fr")
            triplestore.add(tr, hyperdev, "keyword", "scheme")
            triplestore.add(tr, hyperdev, "keyword", "hacker")
        with h.transaction(storage) as tr:
            dolead = uuid4()
            triplestore.add(tr, dolead, "title", "blog.dolead.com")
            triplestore.add(tr, dolead, "keyword", "corporate")
        with h.transaction(storage) as tr:
            julien = uuid4()
            triplestore.add(tr, julien, "title", "julien.danjou.info")
            triplestore.add(tr, julien, "keyword", "python")
            triplestore.add(tr, julien, "keyword", "hacker")
        with h.transaction(storage) as tr:
            query = triplestore.FROM(tr, h.var("subject"), "keyword", "corporate")
            out = list(query)[0]["subject"]
        assert out == dolead


@pytest.mark.parametrize("storage_class", STORES)
def test_seed_subject_lookup(storage_class, path):
    with storage_class(path) as storage:
        with h.transaction(storage) as tr:
            hyperdev = uuid4()
            triplestore.add(tr, hyperdev, "title", "hyperdev.fr")
            triplestore.add(tr, hyperdev, "keyword", "scheme")
            triplestore.add(tr, hyperdev, "keyword", "hacker")
        with h.transaction(storage) as tr:
            dolead = uuid4()
            triplestore.add(tr, dolead, "title", "blog.dolead.com")
            triplestore.add(tr, dolead, "keyword", "corporate")
        with h.transaction(storage) as tr:
            julien = uuid4()
            triplestore.add(tr, julien, "title", "julien.danjou.info")
            triplestore.add(tr, julien, "keyword", "python")
            triplestore.add(tr, julien, "keyword", "hacker")

        with h.transaction(storage) as tr:
            query = triplestore.FROM(tr, dolead, h.var("key"), h.var("value"))
            out = [dict(x) for x in query]

        expected = [
            {"key": "keyword", "value": "corporate"},
            {"key": "title", "value": "blog.dolead.com"},
        ]
        assert out == expected


@pytest.mark.parametrize("storage_class", STORES)
def test_seed_object_variable(storage_class, path):
    with storage_class(path) as storage:
        with h.transaction(storage) as tr:
            hyperdev = uuid4()
            triplestore.add(tr, hyperdev, "title", "hyperdev.fr")
            triplestore.add(tr, hyperdev, "keyword", "scheme")
            triplestore.add(tr, hyperdev, "keyword", "hacker")
        with h.transaction(storage) as tr:
            dolead = uuid4()
            triplestore.add(tr, dolead, "title", "blog.dolead.com")
            triplestore.add(tr, dolead, "keyword", "corporate")
        with h.transaction(storage) as tr:
            julien = uuid4()
            triplestore.add(tr, julien, "title", "julien.danjou.info")
            triplestore.add(tr, julien, "keyword", "python")
            triplestore.add(tr, julien, "keyword", "hacker")
        with h.transaction(storage) as tr:
            query = triplestore.FROM(tr, hyperdev, "title", h.var("title"))
            out = list(query)[0]["title"]
        assert out == "hyperdev.fr"


@pytest.mark.parametrize("storage_class", STORES)
def test_subject_variable(storage_class, path):
    with storage_class(path) as storage:
        # prepare
        with h.transaction(storage) as tr:
            hyperdev = uuid4()
            triplestore.add(tr, hyperdev, "title", "hyperdev.fr")
            triplestore.add(tr, hyperdev, "keyword", "scheme")
            triplestore.add(tr, hyperdev, "keyword", "hacker")
            post1 = uuid4()
            triplestore.add(tr, post1, "blog", hyperdev)
            triplestore.add(tr, post1, "title", "hoply is awesome")
            post2 = uuid4()
            triplestore.add(tr, post2, "blog", hyperdev)
            triplestore.add(tr, post2, "title", "hoply triple store")

        # exec, fetch all blog title from hyperdev.fr
        with h.transaction(storage) as tr:
            out = h.select(
                triplestore.FROM(tr, h.var("blog"), "title", "hyperdev.fr"),
                triplestore.where(tr, h.var("post"), "blog", h.var("blog")),
                triplestore.where(tr, h.var("post"), "title", h.var("title")),
            )
            out = sorted([x["title"] for x in out])
        assert out == ["hoply is awesome", "hoply triple store"]

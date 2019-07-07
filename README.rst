hoply
#####

.. image:: https://raw.githubusercontent.com/amirouche/hoply/master/hoply.jpg



.. image:: https://codecov.io/gh/amirouche/hoply/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/amirouche/hoply

.. image:: https://travis-ci.com/amirouche/hoply.svg?branch=master
   :target: https://travis-ci.com/amirouche/hoply


hoply is a generic n-tuple store that can be used to create a
triplestore or a quadstore or whatever.

Checkout the ``tests.py`` for a glimpse on how to use it.

Getting started
===============

::

   git clone https://github.com/amirouche/hoply/

And read the documentation at `docs/source/index.html <https://git.io/fjI1l>`_.

ChangeLog
=========

0.14.0
------

- Move to a new storage backend API inspired from
  [SRFI-167](https://srfi.schemers.org/srfi-167/) in particular expose
  a `prefix` argument in `Hoply` so that it is possible to hook
  multiple abstractions to the same OKVS database.

- Replace `h.compose` with `h.select`. It might be more familiar to
  Pythonista and the result is similar to what the SPARQL `SELECT`
  does and somewhat similar to what SQL `SELECT` does.

- Drop helpers

- Makefile: bring back WiredTiger

0.13.4
------

- fix bug in prefix construction during querying affecting all queries

0.13.3
------

- fix bug in ``is_permutation_prefix`` and add ``Transaction.add``

0.13.2
------

- moar doc
- add memory backend
- add leveldb backend

0.13.1
------

- README++

0.13.0
------

- Changed the public interface
- Implement generic n-tuple store
- wiredtiger backend

Author
======

`Say héllo! <amirouche.boubekki@gmail.com>`_

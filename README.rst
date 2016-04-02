========
 AjguDB
========

**This program is alpha becarful**

- graphdb
- schemaless
- single thread
- transaction-less
- GPLv2 or GPLv3

AjguDB wants to be a fast enough python graph database for exploring connected data.

Dependencies
============

- [wiredtiger 2.6.1](http://source.wiredtiger.com/releases/wiredtiger-2.6.1.tar.bz2)

ChangeLog
=========

0.7.1
-----

- small typofix in README...

0.7
---

- **storage: only wiredtiger 2.6.1 backend storage is supported**
- ajgudb: you can threat ``AjguDB`` as simple key/value store via its methods
  ``AjguDB.get(key)``, ``AjguDB.set(key, value)`` and ``AjguDB.remove(key)``
- rewrite gremlin querying
- gremlin: ``select`` is renamed ``where`` because it match the SQL terminology.
  SQL's ``FROM`` is ``vertices`` and ``edges`` steps.
- storage: rework the backend to use less write and similar read operations
  count.
- storage: Now edges and vertices are stored in different tables this might
  also lead to performance improvement during querying.
- storage: elements identifiers are now computed by the backend storage, wiredtiger.
- add fuzzy search

0.5
---

- ajgudb

  - add bsddb backend
  - add wiredtiger backend
  - leveldb: increase block size to 1GB

- gremlin:

  - add ``keys`` to retrieve several keys at the same time
  - use lazy ``itertools.imap`` instead of the gready python2's ``map``


0.4.2
-----

- ajgudb:

  - add a shortcut method ``AjguDB.one(**kwargs)`` to query for one element.

- gremlin:

  - fix ``group_count``, now it's a step and not a *final step*
  - fix ``each`` to return ``GremlinResult`` so that history is not lost
    and ``back`` can be used
  - add ``scatter``, it's only useful after ``group_count`` so far.

Coverage: 62%
=============


Author
======

`Say hi! <amirouche@hypermove.net>`_

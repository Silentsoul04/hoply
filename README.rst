========
 AjguDB
========

AjguDB wants to be a python graph database for exploring connected data.

- graphdb
- networkless
- with gremlin querying

- Apache 2


Dependencies
============

- `plyvel <http://plyvel.readthedocs.io/>`_

Documentation
=============

Check out the presentation @ `speakerdeck <https://speakerdeck.com/_amirouche_/ajgudb>`_

ChangeLog
========

0.10 (wip)
----------

- Python 3 support
- Move to leveldb via plyvel instead of wiredtiger

0.9
---

- implement fuzzy search
- expose `AjguDB.search`
- add optional logging and transaction as `AjguDB.transaction()`

0.8.1
-----

- improve documentation
- fix `scatter` step
- improve test coverage (89%)
- more experience with conceptnet

0.8
---

- move to tuple space implementation
- work with wiredtiger develop branch

Author
======

`Say héllo! <amirouche@hypermove.net>`_

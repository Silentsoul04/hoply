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
- wiredtiger and leveldb backend

0.12
----

- Improve ``README.rst``

0.11
----

- Add ``leveldb`` backend
- Add memory based backend

0.10
----

- Move to Python 3.6
- Move to wiredtiger git branch ``wt-3929-python3``
- Move to triple store

Author
======

`Say héllo! <amirouche.boubekki@gmail.com>`_

hoply
#####

**Explore bigger than RAM relational data in the comfort of Python**


Getting started
===============

How to get started
------------------

.. code-block:: python

   import hoply as h
   from hoply.memory import MemoryStore

.. note:: Here ``MemoryStore`` is used instead of LevelDB or
          WiredTiger. It is for the purpose of this tutorial. LevelDB
          is prolly the easiest to install because readily available
          in your favorite distribution. WiredTiger is prefered for
          production use.

How to init the database
------------------------

.. code-block:: python

  db = h.open(MemoryStore())

How to add a triple
-------------------

.. code-block:: python

  db.add('P4X432', 'title', 'hyperdev.fr')

How to add several triples
--------------------------

.. code-block:: python

  db.add('P4X432', 'description', 'my blog')
  db.add('P4X432', 'tagline', 'forward and beyond')

How to create a transaction
---------------------------

.. code-block:: python

  with db.transaction():
      db.add('P4X432', 'title', 'hyperdev.fr')
      db.add('P4X432', 'description', 'my blog')
      db.add('P4X432', 'tagline', 'forward and beyond')

.. note:: Sometime it's better to batch several writes in the same
          transaction to speed things up. Check out ``Hoply.begin()``,
          ``Hoply.rollback()`` and ``Hoply.commit()``.

How to query
------------

.. code-block:: python

  query = h.compose(h.where(h.var('subject'), 'title', 'hyperdev.fr'))
  query = list(query(db))


Installation
============

On Ubuntu Trusty and beyond do the following:

::

   git clone https://github.com/amirouche/hoply/
   sudo apt install build-essential python3-pip
   make dev
   make check

Then you can read with your favorite emacs editor ``tests.py`` to get
to know how to use it!

You can also install ``leveldb`` via ``apt``::

  sudo apt install libleveldb-dev

And ``wiredtiger`` 3.1.0 from sources::

  wget https://source.wiredtiger.com/releases/wiredtiger-3.1.0.tar.bz2
  tar xvf wiredtiger-3.1.0.tar.bz2
  cd wiredtiger*
  ./configure && make -j 9 && sudo make install

ChangeLog
=========

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
- Move to wiredtiger 3.1.0 via wiredtiger-ffi
- Move to triple store

Many features were dropped for the time being.

Author
======

`Say héllo! <amirouche@hypermove.net>`_

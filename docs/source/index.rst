.. hoply documentation master file, created by
   sphinx-quickstart on Wed Apr  3 11:45:09 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to hoply's documentation!
#################################

..
   .. toctree::
      :maxdepth: 2
      :caption: Contents:

Kesako?
=======

Hoply is generic n-tuple store. It is a database storing tuples with
n-items in a set. Each tuple is unique. With Hoply you can use a
triplestore or quadstore similar Blazegraph, Jena, Virtuoso et al. in
the comfort of Python programming language.

The query language is somewhat similar to SPARQL in the sense that it
implement the ``WHERE`` clause but it is embedded in Python. As such
it does not allow as much freedom as SPARQL. It is a cognitive load
vs. performance tradeoff. This is verbose but doesn't say much. Well,
Hoply query language is only a manipulation of generators or if you
prefer a manipulation of streams.

Hop-by-hop
==========

It this walkthrough, the main public interface is presented. Hoply
will be used to model entities of a blog engine.

First let's create an instance of a triplestore with ``subject``,
``predicate`` and ``object`` as tuple items:

.. code-block:: python

   import hoply as h
   from hoply.memory import MemoryConnexion

   cnx = MemoryConnexion('blog')
   db = h.Hoply(cnx, 'engine', ('subject', 'predicate', 'object'))

Now that we have a the database handle ``db`` we can populate it
with a few records describing the blog:

.. code-block:: python

   db.add('blog', 'title', 'Overkill')
   db.add('blog', 'tagline', 'Never invented hereafter')
   db.add('blog', 'author', 'amz3')

Now that the blog is setup, let's add a few blog posts:

.. code-block:: python

   from uuid import uuid4

   post1 = uuid4()
   db.add(post1, 'kind', 'post')
   db.add(post1, 'title', 'Hoply! Hoply! Hoply!')
   db.add(post1, 'body', 'The frog hops over the lazy virtuose')

   post2 = uuid4()
   db.add(post2, 'kind', 'post')
   db.add(post2, 'title', 'How to build a triple store in 500 lines or less')
   db.add(post2, 'body', 'Start using the right tool for the job. A key-value store is perfect for that matter!')

Now let's add a few comments:

.. code-block:: python

   comment1 = uuid4()
   db.add(comment1, 'kind', 'comment')
   db.add(comment1, 'post', post1)
   db.add(comment1, 'author', 'decent-username')
   db.add(comment1, 'body', 'Do you plan to implement a FoundationDB backend?')

   comment2 = uuid4()
   db.add(comment2, 'kind', 'comment')
   db.add(comment2, 'post', post1)
   db.add(comment2, 'author', 'energizer')
   db.add(comment2, 'body', 'What about a redis backend?')

   comment3 = uuid4()
   db.add(comment3, 'kind', 'comment')
   db.add(comment3, 'post', post1)
   db.add(comment3, 'author', 'amz3')
   db.add(comment3, 'body', '@decent-username yes, definitly!')

   comment4 = uuid4()
   db.add(comment4, 'kind', 'comment')
   db.add(comment4, 'post', post1)
   db.add(comment4, 'author', 'amz3')
   db.add(comment4, 'body', '@energizer no. It does not make sens to implement that on top REDIS. Also I find REDIS useless nowdays!')

Now let's query the ``title`` of the blog:

.. code:: python

   title = list(db.FROM('blog', 'title', h.var('title')))[0]['title']

Let's query both the title and tagline:

.. code:: python

   config = list(h.compose(
       db.FROM('blog', 'title', h.var('title')),
       db.where('blog', 'tagline', h.var('tagline')),
   ))[0]

Let's query all the comments of a given blog post:

.. code:: python

   def get_comments(post_uid):
       comments = list(h.compose(
           db.FROM(h.var('uid'), 'kind', 'comment'),
	   db.where(h.var('uid'), 'post', post_uid),
	   db.where(h.var('uid'), 'body', h.var('body')),
	   h.pick('body')
       ))
       return comments

This will return:

.. code:: python

    expected = [
        '@energizer no. It does not make sens to implement that on top REDIS. Also I find REDIS useless nowdays!',  # noqa
        'What about a redis backend?',
        'Do you plan to implement a FoundationDB backend?',
        '@decent-username yes, definitly!'
    ]
    assert sorted(get_comments(post1)) == sorted(expected)

That is all folks!

..
   Indices and tables
   ==================

   * :ref:`genindex`
   * :ref:`modindex`
   * :ref:`search`

# AjguDB − Graph Database for everyday use

## Introduction

AjguDB is a graph database (graphdb) written in Python with
[Gremlin querying](http://s3.thinkaurelius.com/docs/titan/0.5.0/gremlin.htm).

The purpose of the project is both side:

- Getting your hands dirty with some graphitti awesomeness
- Getting your to explore Graph data

And hopefully enjoy some more Python.

The project is almost fully tested, 62% :P. Tests are mainly missing from gremlin querying.

## Demo

Want to get your hand dirty with vertices and edges?

```python
from ajgudb import AjguDB


graph = AjguDB('/tmp/ajgudb')

# create a bunch of messages
message0 = graph.vertex.create('message')
message0['body'] = '/me learning ajgudb'
message0.save()

message0 = graph.vertex.create('message')
message0['body'] = 'This how you create vertex'
message0.save()

# who say that?
user = graph.vertex.create('user')
user['username'] = 'maybe4u'
user['bio'] = 'Learning how a to graphitti my app'
user.save()

edge = user.link('created', message0)
edge.save()

# you can also do
user.link('created', message1).save()
```

Now chase the dream with gremlin querying!

```python
from ajgudb import gremlin


# try to guess
query = gremlin.query(gremlin.outgoings, gremlin.end, gremlin.count) 
# guess harder
count = query(user)
```

You can do it backward of course :)

```python
query = gremlin.query(gremlin.incomings, gremlin.start, gremlin.get)
maybe4u = query(message0)[0]
```

Graph are very awesome!

## Walkthrough

In a graph there is four important things:

- The Graph
- Vertices
- Edges
- a way to query the above

The API is split following those ideas.

Mind the fact that you never initiliaze via Python class instantiation
`Vertex` or `Edge` objects but only via method call of the `AjguDB` object.

### The graph aka. `AjguDB` 

`from ajgudb import AjguDB`

To create a database you only require to know where to store it:

```python
graphdb = AjguDB('/path/to/store')
```

The graph API delegates it's edges and vertices manipulation method to two managers
`AjguDB.vertex` and `AjguDB.edge`. We will see that later...

You can also interact with the graphdb as key/value store aka. dictionary.

```python
graphdb.set('count', 42)
graphdb.get('count')
graphdb.remove('count')
```

Vertices, Edges and key/value items are stored in different namespace. They can not clash.

To create a vertex you use `graphdb.vertex.create(label, **properties)`:

```python
like = graphdb.vertex.create('word', word='like', kind='verb')
love = graphdb.vertex.create('word', word='love', kind='verb')
```

Don't forget to save!

```
like.save()
love.save()
```

To create an edge you can `Vertex.link(label, other, **properties)` method:

```python
like.link('is related to', love).save()
```

### Edge aka. `graphdb.edge`

If you already know an edge for some reason, you can get it:

```python
graphdb.edge.get(edge.uid)
```

If you know an edge with particular characteristics you can retrieve
it with `graphdb.edge.one(label, **properties)`

```python
edge = graphdb.edge.one('is related to')
```

`edge` is None if there is no such edge.

Each edge has an `edge.uid` property and a few methods:

- `Edge.start()` will retrieve the edge starting vertex
- `Edge.end()` will retrieve the edge ending vertex

And:

- `Edge.save()` will save the edge new properties
- `Edge.delete()` will delete the edge


### Vetex aka. `graphdb.vertex`

Vertex api is kind of symmetric.

If you already know a vertex for some reason, you can get it:

```python
graphdb.edge.get(love.uid)
```

If you know a vertex with particular characteristics you can retrieve
it with `graphdb.vertex.one(label, **properties)`

```python
love = graphdb.vertex.one('word', word='love')
```

`love` will be `None` if there is such word.

Each vertex has an `vertex.uid` property and a few methods:

- `Vertex.outgoings()` will retrieve the edges starting at this vertex
- `Vertex.incomings()` will retrieve the edges ending at this vertex

And:

- `Vertex.save()` will save the vertex new properties
- `Vertex.delete()` will delete the vertex

There is also a `Vertex.link(label, end, **properties)` build a edge
beetween the current vertex and `end`.

### Graph navigation with gremlin

Gremlin querying from tinkerpop fame is a great way to navigate and
query a graph. It's implemented as a composition of Python generators
called a pipeline.  Each generator is called a step. There is two kind
of steps. Some steps create values, others will change the value. A
pipeline may or not starts with a steps creating values.

#### `query(*steps)`

The way to compose steps. It returns a procedure that takes the
`graphdb` object an possibly a value to feed/start the pipeline.

#### `vertices(label='')`

Create step, that generates vertices of the given `label`. All vertices
are generated if no label is provided.

#### `edges(label='')`

Create step, that generates edges of the given `label`. All edges
are generated if no label is provided.

#### `where(**kwargs)`

Filter step, that consume the pipeline and retain only `Vertex` or `Edge` objects
whose properties match `kwargs`.

#### `skip(count)`

Forget the first `count` objects found in the pipeline.

#### `limit(count)`

Keep the first `count` objects found in the pipeline forget the remaining.

#### `paginator(count)`

Build lists of `count` objects from the pipeline.

#### `count`

Count the number of items in the pipeline.

#### `incomings`

Return a generator over incomings edges of the vertices from the pipeline.

#### `outgoings`

Return a generator over outgoings edges of the vertices from the pipeline.

#### `start`

Return a generator over the starting vertices of edges from the pipeline. 

#### `end`

Return a generator over the ending vertices of edges from the pipeline. 

#### `each(proc)`

Returns a generator made of the output of `proc(graphdb, x)` where `x`
is one value from the pipeline.

#### `value`

Returns a generator made of the value found in the pipeline.

#### `get`

Returns a list made of `Vertex` or `Edge` objects from the pipeline.

#### `sort(key=lambda g, x: x, reverse=False)`

Sort the pipeline based on `key`.

#### `key(name)`

Retrieve properties value for `name`.

#### `keys(*names)`

Retrieve properties values for `names`.

#### `unique`

Uniquify the pipeline content

#### `filter(predicate)`

Call `predicate(graphdb, item)` where `item` is an item from the
pipeline, and keep the item only if the predicate returns true.

#### `back`

Retrieve the parent item result from every element in the pipeline.

#### `path(steps)`

Return the elements from the path for `steps` number of steps.

#### `mean`

Compute the mean value.

#### `group_count`

#### `scatter`

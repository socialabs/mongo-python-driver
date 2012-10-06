.. _motor-tutorial:

Motor Tutorial
==============

.. testsetup::

  import pymongo
  pymongo.Connection().test_database.test_collection.remove()
  import motor
  import tornado.web
  from tornado.ioloop import IOLoop
  from tornado import gen

.. testcleanup::

  pymongo.Connection().test_database.test_collection.remove()

We'll guide you through using **MongoDB** and **Tornado** using **Motor**, the
non-blocking driver.

Tutorial Prerequisites
----------------------
Make sure that yo've
:doc:`installed the version of PyMongo that includes Motor <motor_installation>`.
In the Python shell, the following should run without raising an exception:

  >>> import motor

This tutorial also assumes that a MongoDB instance is running on the
default host and port. Assuming you have `downloaded and installed
<http://www.mongodb.org/display/DOCS/Getting+Started>`_ MongoDB, you
can start it like so:

.. code-block:: bash

  $ mongod

Making a Connection
-------------------
You typically create a single instance of either :class:`~motor.MotorConnection`
or :class:`~motor.MotorReplicaSetConnection` at the time your application starts
up, before accepting HTTP requests from clients. This single instance of the
connection class persists for the life time of the process. You must call
:meth:`~motor.MotorConnection.open_sync` before any other operation on the
connection:

  >>> connection = motor.MotorConnection().open_sync()

This connects to a ``mongod`` listening on the default host and port. We can
specify the host and port like:

  >>> connection = motor.MotorConnection('localhost', 27017).open_sync()

A single instance of MongoDB can support multiple independent
`databases <http://www.mongodb.org/display/DOCS/Databases>`_. From an open
connection, you can get a reference to a particular database with dot-notation
or bracket-notation:

  >>> db = connection.test_database
  >>> db = connection['test_database']

Creating a reference to a database does no I/O and does not require a callback.

:meth:`~motor.MotorConnection.open_sync` is a blocking operation so it should
be called before listening for HTTP requests. Here's an example startup
sequence for a Tornado web application:

.. code-block:: python

    db = motor.MotorConnection().open_sync().test_database

    application = tornado.web.Application([
            (r'/', MainHandler)
        ],
        db=db
    )

    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()

Passing the database as the ``db`` keyword argument to ``Application`` makes it
available to request handlers:

.. code-block:: python

    class MainHandler(tornado.web.RequestHandler):
        def get(self):
            db = self.settings['db']

Getting a Collection
--------------------
A `collection <http://www.mongodb.org/display/DOCS/Collections>`_ is a
group of documents stored in MongoDB, and can be thought of as roughly
the equivalent of a table in a relational database. Getting a
collection in Motor works the same as getting a database:

  >>> collection = db.test_collection
  >>> collection = db['test_collection']

Just like getting a reference to a database, getting a reference to a
collection does no I/O and doesn't require a callback.

Inserting a Document
--------------------
:ref:`As in PyMongo <tutorial-documents>`, Motor represents MongoDB documents
with Python dictionaries. To store a document in MongoDB, call
:meth:`~motor.MotorCollection.insert` with a document and a callback, and
start Tornado's IOLoop:

  >>> from tornado.ioloop import IOLoop
  >>> def my_callback(result, error):
  ...     print 'result', repr(result)
  ...     IOLoop.instance().stop()
  ...
  >>> document = {'key': 'value'}
  >>> db.test_collection.insert(document, callback=my_callback)
  >>> IOLoop.instance().start()
  result ObjectId('...')

There are several differences to note between Motor and PyMongo. One is that,
unlike PyMongo's :meth:`~pymongo.collection.Collection.insert`, Motor's has no
return value. Another is that ``insert`` accepts an optional callback function.
The function must take two arguments and it must be passed to ``insert`` as a
keyword argument, like:

.. code-block:: python

  db.test_collection.insert(document, callback=some_function)

Passing the function using the ``callback=`` syntax is required.

:meth:`insert` is *asynchronous*. This means it returns immediately, and the
actual work of inserting the document into the collection is performed in the
background. When it completes, the callback is executed. If the
insert succeeded, the ``result`` parameter is the new document's unique id
and the ``error`` parameter is ``None``. If there was an error, ``result`` is
``None`` and ``error`` is an ``Exception`` object. For example, we can
trigger a duplicate-key error by trying to insert two documents with the same
unique id:

  >>> ncalls = 0
  >>> def my_callback(result, error):
  ...     global ncalls
  ...     print 'result', repr(result), 'error', repr(error)
  ...     ncalls += 1
  ...     if ncalls == 2:
  ...         IOLoop.instance().stop()
  ...
  >>> document = {'_id': 1}
  >>> db.test_collection.insert(document, callback=my_callback)
  >>> db.test_collection.insert(document, callback=my_callback)
  >>> IOLoop.instance().start()
  result 1 error None
  result None error DuplicateKeyError('E11000 duplicate key error index: test_database.test_collection.$_id_  dup key: { : 1 }',)

The first insert results in ``my_callback`` being called with result 1 and
error ``None``. The second insert triggers ``my_callback`` with result None and
a :class:`~pymongo.errors.DuplicateKeyError`.

.. seealso:: :ref:`Acknowledged writes in Motor <motor-acknowledged-writes>`

A typical beginner's mistake with Motor is to insert documents in a loop,
not waiting for each insert to complete before beginning the next:

.. code-block:: python

  >>> for i in range(2000):
  ...     db.test_collection.insert({'i': i})

In PyMongo this would insert each document in turn using a single socket,
but Motor attempts to run all the :meth:`insert` operations at once. This
requires 2000 open sockets connected to MongoDB, which taxes the client and
server, and exceeds the file-descriptor limit on Mac OS X. To ensure instead
that all inserts use a single connection, wait for acknowledgment of each. This
is a bit complex using callbacks:

  >>> i = 0
  >>> def do_insert(result, error):
  ...     global i
  ...     if error:
  ...         raise error
  ...     i += 1
  ...     if i < 2000:
  ...         db.test_collection.insert({'i': i}, callback=do_insert)
  ...     else:
  ...         IOLoop.instance().stop()
  ...
  >>> # Start
  >>> db.test_collection.insert({'i': i}, callback=do_insert)
  >>> IOLoop.instance().start()

You can simplify this code with ``gen.engine``.

Using Motor with `gen.engine`
-----------------------------
The `tornado.gen module`_
lets you use generators to simplify asynchronous code, combining operations and
their callbacks in a single function. You must decorate the function with
``@gen.engine`` and yield ``gen.Task`` instances to wait for operations to
complete:

  >>> @gen.engine
  >>> def do_insert():
  ...     for i in range(2000):
  ...         arguments = yield gen.Task(db.test_collection.insert, {'i': i})
  ...         result, error = arguments.args
  ...         if error:
  ...             raise error
  ...     IOLoop.instance().stop()
  ...
  >>> # Start
  >>> do_insert()
  >>> IOLoop.instance().start()

Here ``arguments`` is an instance of `tornado.gen.Arguments`_
containing the arguments :meth:`insert` passed to its callback function.
Motor provides :class:`~motor.Op` to further simplify asynchronous operations
with ``gen.engine``:

  >>> @gen.engine
  >>> def do_insert():
  ...     for i in range(2000):
  ...         result = yield motor.Op(db.test_collection.insert, {'i': i})
  ...     IOLoop.instance().stop()
  ...
  >>> do_insert()
  >>> IOLoop.instance().start()

:class:`~motor.Op` receives the ``result`` and ``error`` parameters and either
raises the error or returns the result. In the code above, ``result`` is the
``_id`` of each inserted document.

.. seealso:: :ref:`Bulk inserts <bulk-inserts>`

.. seealso:: :ref:`Detailed example of Motor and gen.engine <generator-interface-example>`

.. _tornado.gen module: http://www.tornadoweb.org/documentation/gen.html

.. _tornado.gen.Arguments: http://www.tornadoweb.org/documentation/gen.html#tornado.gen.Arguments

.. mongodoc:: insert

Getting a Single Document With :meth:`~motor.MotorCollection.find_one`
----------------------------------------------------------------------
Use :meth:`~motor.MotorCollection.find_one` to get the first document that
matches a query. For example, to get a document where the value for key "i" is
less than 2:

  >>> @gen.engine
  >>> def do_find_one():
  ...     document = yield motor.Op(
  ...         db.test_collection.find_one, {'i': {'$lt': 2}})
  ...     print document
  ...     IOLoop.instance().stop()
  ...
  >>> do_find_one()
  >>> IOLoop.instance().start()
  {'i': 0, '_id': ObjectId('...')}

The result is a dictionary matching the one that we inserted previously.

.. note:: The returned document contains an ``"_id"``, which was
   automatically added on insert.

.. mongodoc:: find

Querying for More Than One Document
-----------------------------------
Use :meth:`~motor.MotorCollection.find` to query for a set of documents.
:meth:`~motor.MotorCollection.find` does no I/O and does not take a callback,
it merely creates a :class:`~motor.MotorCursor` instance. The query is actually
executed on the server when you call :meth:`~motor.MotorCursor.to_list`,
:meth:`~motor.MotorCursor.each`, or :meth:`~motor.MotorCursor.next_object`.
All three methods require a callback.

To find all documents with "i" less than 5:

  >>> @gen.engine
  >>> def do_find():
  ...     cursor = db.test_collection.find({'i': {'$lt': 5}})
  ...     for document in (yield motor.Op(cursor.to_list)):
  ...         print document
  ...     IOLoop.instance().stop()
  ...
  >>> do_find()
  >>> IOLoop.instance().start()
  {'i': 0, '_id': ObjectId('...')}
  {'i': 1, '_id': ObjectId('...')}
  {'i': 2, '_id': ObjectId('...')}
  {'i': 3, '_id': ObjectId('...')}
  {'i': 4, '_id': ObjectId('...')}

To iterate over a large result set without holding all the documents in memory
at once, get one document at a time with :meth:`~motor.MotorCursor.next_object`:

  >>> @gen.engine
  >>> def do_find():
  ...     cursor = db.test_collection.find({'i': {'$lt': 5}})
  ...     document = yield motor.Op(cursor.next_object)
  ...     while document:
  ...         print document
  ...         document = yield motor.Op(cursor.next_object)
  ...     IOLoop.instance().stop()
  ...
  >>> do_find()
  >>> IOLoop.instance().start()
  {'i': 0, '_id': ObjectId('...')}
  {'i': 1, '_id': ObjectId('...')}
  {'i': 2, '_id': ObjectId('...')}
  {'i': 3, '_id': ObjectId('...')}
  {'i': 4, '_id': ObjectId('...')}

You can apply a sort, limit, or skip to a query before you begin iterating:

  >>> @gen.engine
  >>> def do_find():
  ...     cursor = db.test_collection.find({'i': {'$lt': 5}})
  ...     # Modify the query before iterating
  ...     cursor.sort([('i', pymongo.DESCENDING)]).limit(2).skip(2)
  ...     document = yield motor.Op(cursor.next_object)
  ...     while document:
  ...         print document
  ...         document = yield motor.Op(cursor.next_object)
  ...     IOLoop.instance().stop()
  ...
  >>> do_find()
  >>> IOLoop.instance().start()
  {'i': 2, '_id': ObjectId('...')}
  {'i': 1, '_id': ObjectId('...')}

Counting Documents
------------------
Use :meth:`~motor.MotorCursor.count` to determine the number of documents in
a collection, or the number of documents that match a query:

  >>> @gen.engine
  >>> def do_count():
  ...     n = yield motor.Op(db.test_collection.find().count)
  ...     print n, 'documents in collection'
  ...     n = yield motor.Op(
  ...         db.test_collection.find({'i': {'$gt': 1000}}).count)
  ...     print n, 'documents where i > 1000'
  ...     IOLoop.instance().stop()
  ...
  >>> do_count()
  >>> IOLoop.instance().start()
  2000 documents in collection
  999 documents where i > 1000

:meth:`~motor.MotorCursor.count` uses the *count command* internally; we'll
commands_ below.

.. seealso:: `Count command <http://www.mongodb.org/display/DOCS/Aggregation#Aggregation-Count>`_

Updating Documents
------------------
:meth:`~motor.MotorCollection.update` changes documents. It requires two
parameters: a *query* that specifies which documents to update, and an update
document. The query follows the same syntax as for :meth:`find` or
:meth:`find_one`. The update document has two modes: it can replace the whole
document, or it can update some fields of a document. To replace a document:

  >>> @gen.engine
  >>> def do_replace():
  ...     coll = db.test_collection
  ...     old_document = yield motor.Op(coll.find_one, {'i': 50})
  ...     print 'found document:',
  ...     result = yield motor.Op(coll.update, {'_id': _id}, {'key': 'value'})
  ...     print 'replaced', result['n'], 'document'
  ...     new_document = yield motor.Op(coll.find_one, {'_id': _id})
  ...     print 'document is now', new_document
  ...     IOLoop.instance().stop()
  ...
  >>> do_replace()
  >>> IOLoop.instance().start()
  found document: {'_id': ObjectId('...'), 'i': 50}
  replaced 1 document
  document is now {'_id': ObjectId('...'), 'key': 'value'}

You can see that :meth:`update` replaced everything in the old document except
its ``_id`` with the new document.

Use MongoDB's modifier operators to update part of a document and leave the
rest intact. We'll find the document whose "i" is 51 and use the ``$set``
operator to set "key" to "value":

  >>> @gen.engine
  >>> def do_update():
  ...     coll = db.test_collection
  ...     result = yield motor.Op(coll.update,
  ...         {'i': 51}, {'$set': {'key': 'value'}})
  ...     print 'updated', result['n'], 'document'
  ...     new_document = yield motor.Op(coll.find_one, {'i': 51})
  ...     print 'document is now', new_document
  ...     IOLoop.instance().stop()
  ...
  >>> do_update()
  >>> IOLoop.instance().start()
  updated 1 document
  document is now {'i': 51, '_id': ObjectId('...'), 'key': 'value'}

"key" is set to "value" and "i" is still 51.

By default :meth:`update` only affects the first document it finds, you can
update all of them with the ``multi`` flag:

.. code-block:: python

    yield motor.Op(coll.update,
        {'i': {'$gt': 100}}, {'$set': {'key': 'value'}}, multi=True)

.. mongodoc:: update

Removing Documents
------------------

:meth:`~motor.MotorCollection.remove` takes a query with the same syntax as
:meth:`~motor.MotorCollection.find`.
:meth:`remove` immediately removes all matching documents.

  >>> @gen.engine
  >>> def do_remove():
  ...     coll = db.test_collection
  ...     n = yield motor.Op(coll.count)
  ...     print n, 'documents before calling remove()'
  ...     result = yield motor.Op(db.test_collection.remove,
  ...         {'i': {'$gte': 1000}})
  ...     print (yield motor.Op(coll.count)), 'documents after'
  ...     IOLoop.instance().stop()
  ...
  >>> do_remove()
  >>> IOLoop.instance().start()
  2000 documents before calling remove()
  1001 documents after

.. mongodoc:: remove

Commands
--------
Besides the "CRUD" operations--insert, update, remove, and find--all other
operations on MongoDB are commands. Run them using
the :meth:`~motor.MotorDatabase.command` method on :class:`~motor.MotorDatabase`:

  >>> @gen.engine
  >>> def use_count_command():
  ...     response = yield motor.Op(db.command, {"count": "test_collection"})
  ...     print 'response:', response
  ...     IOLoop.instance().stop()
  ...
  >>> use_count_command()
  >>> IOLoop.instance().start()
  {'ok': 1.0, u'n': 1000.0}

Many commands have special helper methods, such as
:meth:`~motor.MotorDatabase.create_collection` or
:meth:`~motor.MotorCollection.aggregate`, but these are just conveniences atop
the basic :meth:`command` method.

.. mongodoc:: commands

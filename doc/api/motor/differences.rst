Differences between Motor and PyMongo
=====================================

Major differences
=================

Creating a connection
---------------------

PyMongo's :class:`~pymongo.connection.Connection` and
:class:`~pymongo.replica_set_connection.ReplicaSetConnection` constructors
block until they have established a connection to MongoDB. A
:class:`~motor.MotorConnection` or :class:`~motor.MotorReplicaSetConnection`,
however, is created unconnected. One should call
:meth:`~motor.MotorConnection.open_sync` at the beginning of a Tornado web
application, before accepting requests:

.. code-block:: python

    import motor
    connection = motor.MotorConnection().open_sync()

To make a connection asynchronously once the application is running, call
:meth:`~motor.MotorConnection.open`:

.. code-block:: python

    def connected(connection, error):
        if error:
            print 'Error connecting!', error
        else:
            # Use the connection
            pass

    motor.MotorConnection().open(connected)

Callbacks
---------

Motor supports nearly every method PyMongo does, but Motor methods that
do network I/O take a callback function. The callback must accept two
parameters:

.. code-block:: python

    def callback(result, error):
        pass

Motor's asynchronous methods return ``None`` immediately, and execute the
callback, with either a result or an error, when the operation has completed.

For example, one uses
:meth:`~pymongo.collection.Collection.find_one` in PyMongo like:

.. code-block:: python

    db = Connection().test
    user = db.users.find_one({'name': 'Jesse'})
    print user

But Motor's :meth:`~motor.MotorCollection.find_one` method works asynchronously:

.. code-block:: python

    db = MotorConnection().open_sync().test

    def got_user(user, error):
        if error:
            print 'error getting user!', error
        else:
            print user

    db.users.find_one({'name': 'Jesse'}, callback=got_user)

The callback must be passed as a keyword argument, not a positional argument.

To find multiple documents, Motor provides :meth:`~motor.MotorCursor.each` and
:meth:`~motor.MotorCursor.to_list`.

.. code-block:: python

    def each_user(user, error):
        if error:
            print 'error getting user!', error
        elif user:
            print user
        else:
            # Iteration complete
            print 'Done'

    db.users.find().each(callback=each_user)

Safe writes
-----------

PyMongo's default behavior for
:meth:`~pymongo.collection.Collection.insert`,
:meth:`~pymongo.collection.Collection.update`,
:meth:`~pymongo.collection.Collection.save`, and
:meth:`~pymongo.collection.Collection.remove` is unacknowledged writes:
the driver does not request nor await a response from the server unless the
method is passed ``safe=True`` or another
`getLastError option <http://www.mongodb.org/display/DOCS/getLastError+Command>`_.

In Motor, writes are acknowledged (they are "safe writes") if passed a callback:

.. code-block:: python

    def inserted(result, error):
        if error:
            print 'error inserting!', error
        else:
            print 'added user'

    db.users.insert({'name': 'Bernie'}, callback=inserted)

The ``result`` parameter to the callback contains the ``_id`` of the document
for `insert` or `save`, and MongoDB's `getLastError` response for `update` or
`remove`.

With no callback, Motor does unacknowledged writes.

One can pass ``safe=False`` explicitly, along with a callback, to perform an
unacknowledged write:

.. code-block:: python

    db.users.insert({'name': 'Jesse'}, callback=inserted, safe=False)

In this case the callback is executed as soon as the message has been written to
the socket connected to MongoDB, but no response is expected from the server.
Passing a callback and ``safe=False`` can be useful to do fast writes without
overrunning the output buffer.

Requests
--------

Motor does not support :doc:`requests </examples/requests>`. Requests are
intended in PyMongo to ensure that a series of operations are performed in
order by the MongoDB server. In Motor, ordering can be guaranteed by
registering a callback for each operation and performing the next operation in
the callback.

Motor ignores the ``auto_start_request`` parameter to
:class:`~motor.MotorConnection` or :class:`~motor.MotorReplicaSetConnection`.

Minor differences
=================

is_locked
---------

:meth:`~motor.MotorConnection.is_locked` in Motor is a method requiring a
callback, whereas in PyMongo it is a property of
:class:`~pymongo.connection.Connection`.

system_js
---------

PyMongo supports Javascript procedures stored in MongoDB with syntax like:

.. code-block:: python

    >>> db.system_js.my_func = 'function(x) { return x * x; }'
    >>> db.system_js.my_func(2)
    4.0

Motor does not. One should use ``system.js`` as a regular collection with Motor:

.. code-block:: python

    def saved(result, error):
        if error:
            print 'error saving function!', error
        else:
            db.eval('my_func(2)', callback=evaluated)

    def evaluated(result, error):
        if error:
            print 'eval error!', error
        else:
            print 'eval result:', result # This will be 4.0

    db.system.js.save(
        {'_id': 'my_func', 'value': Code('function(x) { return x * x; }')},
        callback=saved)

.. seealso:: `Server-side code execution <http://www.mongodb.org/display/DOCS/Server-side+Code+Execution>`_

Cursor slicing
--------------

In Pymongo, the following raises an ``IndexError`` if the collection has fewer
than 101 documents:

.. code-block:: python

    db.collection.find()[100]

In Motor, however, no exception is raised. The query simply has no results:

.. code-block:: python

    def callback(result, error):
        # 'result' and 'error' are both None
        print result, error

    db.collection.find()[100].next(callback)

The difference arises because the PyMongo :class:`~pymongo.cursor.Cursor`'s
slicing operator blocks until it has queried the MongoDB server, and determines
if a document exists at the desired offset; Motor simply returns a new
:class:`~motor.MotorCursor` with a skip and limit applied.
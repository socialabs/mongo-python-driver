Differences between Motor and PyMongo
=====================================

Callbacks
---------

Motor supports nearly every method PyMongo does, but every Motor method that
does network I/O takes a callback function like:

.. code-block:: python

    def callback(result, error):
        pass

The Motor method returns ``None`` immediately, and executes the callback
asynchronously, with either a result or an error, when the operation has
completed. For example, one uses
:meth:`~pymongo.collection.Collection.find_one` in PyMongo like:

.. code-block:: python

    db = Connection().test
    user = db.users.find_one({'name': 'Jesse'})
    print user

Motor's :meth:`~motor.MotorCollection.find_one` method works asynchronously:

.. code-block:: python

    db = MotorConnection().open_sync().test

    def got_user(user, error):
        if error:
            print 'error getting user!', error
        else:
            print user

    db.users.find_one({'name': 'Jesse'}, callback=got_user)

The callback must be passed as a keyword argument, not a positional argument.

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

In Motor, writes are acknowledged ("safe") if passed a callback:

.. code-block:: python

    def inserted(result, error):
        if error:
            print 'error inserting!', error
        else:
            print 'added user'

    db.users.insert({'name': 'Bernie'}, callback=inserted)

One can pass ``safe=False`` explicitly, along with a callback, to perform an
unacknowledged write.

Requests
--------

Motor does not support :doc:`requests </examples/requests>`. Requests are
intended in PyMongo to ensure that a series of operations are performed in
order by the MongoDB server; in Motor, ordering can be guaranteed by
registering a callback for each operation and performing the next operation in
the callback.


:mod:`motor` -- Asynchronous Python driver for Tornado and MongoDB
==================================================================

.. automodule:: motor
   :synopsis: Asynchronous Python driver for Tornado and MongoDB

.. testsetup::

  from pymongo import Connection
  Connection().test.test_collection.remove(safe=True)

.. doctest::

  >>> from tornado.ioloop import IOLoop
  >>> import motor
  >>> connection = motor.MotorConnection()
  >>> connection.open_sync()
  MotorConnection(Connection('localhost', 27017))
  >>> collection = connection.test.test_collection
  >>>
  >>> def inserted(result, error):
  ...     if error:
  ...         raise error
  ...
  ...     # result is the new document's _id
  ...     collection.update(
  ...         {'_id': result}, {'$set': {'b': 2}}, callback=updated)
  ...
  >>> def updated(result, error):
  ...     if error:
  ...         raise error
  ...     collection.find().each(callback=each)
  ...
  >>> def each(result, error):
  ...     if error:
  ...         raise error
  ...     elif result:
  ...         print 'result', result
  ...     else:
  ...         # Iteration complete
  ...         collection.remove(callback=done)
  ...
  >>> def done(result, error):
  ...     IOLoop.instance().stop()
  ...     if error:
  ...         raise error
  ...     print 'done'
  ...
  >>> collection.insert({'a': 1}, callback=inserted)
  >>> IOLoop.instance().start()
  result {u'a': 1, u'_id': ObjectId('...'), u'b': 2}
  done

Limitations:

* PyMongo's :class:`~pymongo.master_slave_connection.MasterSlaveConnection`
  is not supported.

Classes:

.. toctree::

   motor_connection
   motor_replica_set_connection
   motor_database
   motor_collection
   motor_cursor

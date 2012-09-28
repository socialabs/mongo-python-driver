:class:`MotorReplicaSetConnection` -- Connection to MongoDB replica set
=======================================================================

.. currentmodule:: motor

.. autoclass:: motor.MotorReplicaSetConnection

  .. automethod:: open
  .. automethod:: open_sync
  .. method:: disconnect

     Disconnect from MongoDB.

     Disconnecting will close all underlying sockets in the
     connection pool. If the :class:`MotorReplicaSetConnection` is used again it
     will be automatically re-opened.

  .. method:: close

     Alias for :meth:`disconnect`.

  .. describe:: c[db_name] || c.db_name

     Get the `db_name` :class:`MotorDatabase` on :class:`MotorReplicaSetConnection` `c`.

     Raises :class:`~pymongo.errors.InvalidName` if an invalid database name is used.
     Raises :class:`~pymongo.errors.InvalidOperation` if connection isn't opened yet.

  .. autoattribute:: connected
  .. automotorattribute:: seeds
  .. automotorattribute:: hosts
  .. automotorattribute:: arbiters
  .. automotorattribute:: primary
  .. automotorattribute:: secondaries
  .. automotorattribute:: read_preference
  .. automotorattribute:: max_pool_size
  .. automotorattribute:: document_class
  .. automotorattribute:: tz_aware
  .. automotorattribute:: safe
  .. method:: sync_connection

     Get a :class:`~pymongo.replica_set_connection.ReplicaSetConnection`
     with the same configuration as this :class:`MotorReplicaSetConnection`

  .. automotormethod:: get_lasterror_options
  .. automotormethod:: set_lasterror_options
  .. automotormethod:: unset_lasterror_options
  .. automotormethod:: database_names
  .. automotormethod:: drop_database
  .. automotormethod:: copy_database(from_name, to_name[, from_host=None[, username=None[, password=None[, callback=None]]]])
  .. automotormethod:: close_cursor

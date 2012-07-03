Limitations
===========

* Multithreading and forking are not supported; Motor is intended to be part of
  a single-threaded Tornado application.
* PyMongo's :class:`~pymongo.master_slave_connection.MasterSlaveConnection`
  is not supported.
* Only Python 2.7 has been tested. We plan to support the same Python versions
  as Tornado does.

import os
import time
from tornado.ioloop import IOLoop


import pymongo as sync_pymongo
from pymongo.tornado_async import async

# So that synchronous unittests can import these names from fake_pymongo,
# thinking it's really pymongo
from pymongo import ASCENDING, DESCENDING, GEO2D, GEOHAYSTACK, ReadPreference
from pymongo.errors import ConnectionFailure


__all__ = [
    'ASCENDING', 'DESCENDING', 'GEO2D', 'GEOHAYSTACK', 'ReadPreference',
    'Connection', 'ReplicaSetConnection', 'Database', 'Collection',
    'Cursor',
]


timeout_sec = float(os.environ.get('TIMEOUT_SEC', 5))

class StopAndFail(object):
    # TODO: doc
    def __init__(self, exc):
        self.exc = exc
        self.abort = False

    def __call__(self, *args, **kwargs):
        if not self.abort:
            IOLoop.instance().stop()
            raise self.exc


def loop_timeout(seconds, kallable, exc):
    fail_func = StopAndFail(exc)
    loop = IOLoop.instance()
    loop.add_timeout(time.time() + seconds, fail_func)

    def success_func(*args, **kwargs):
        loop.stop()
        fail_func.abort = True

    kallable(callback=success_func)


class Connection(object):
    tornado_connection_class = async.TornadoConnection

    def __init__(self, host, port, *args, **kwargs):
        self.host = host
        self.port = port
        self._tconn = self.tornado_connection_class(host, port, *args, **kwargs)

        # Try to connect the TornadoConnection before continuing
        loop = IOLoop.instance()
        loop_timeout(timeout_sec, self._tconn.open, ConnectionFailure(
            "fake_pymongo.Connection: Can't connect to %s:%s" % (
                host, port
            )
        ))

        loop.start()

    def __getattr__(self, name):
        tornado_attr = getattr(self._tconn, name)
        if isinstance(tornado_attr, async.TornadoDatabase):
            return Database(self, name=name)
        else:
            return tornado_attr


class ReplicaSetConnection(Connection):
    # fake_pymongo.ReplicaSetConnection is just like fake_pymongo.Connection,
    # except it wraps a TornadoReplicaSetConnection instead of a
    # TornadoConnection.
    tornado_connection_class = async.TornadoReplicaSetConnection


class Database(object):
    def __init__(self, connection, name):
        assert isinstance(connection, Connection)
        self.name = name
        self.connection = connection

        # Get a TornadoDatabase
        self._tdb = getattr(connection._tconn, name)
        assert isinstance(self._tdb, async.TornadoDatabase)

    def __getattr__(self, name):
        return Collection(self, name)

    __getitem__ = __getattr__

#    def command(self, *args, **kwargs):
#        real_method = self._tdb.command
#        def method(*args, **kwargs):
#            results = {}
#            loop = IOLoop.instance()
#
#            def callback(result, error):
#                results['result'] = result
#                results['error'] = error
#                loop.stop()
#
#            assert 'callback' not in kwargs
#            kwargs['callback'] = callback
#            real_method(*args, **kwargs)
#
#            # IOLoop's start() will exit once the callback is called and calls
#            # stop(), or after the timeout
#            def stop_and_fail():
#                loop.stop()
#                raise Exception("Callback not called before timeout")
#
#            timeout_sec = os.environ.get('TIMEOUT_SEC', 5)
#            loop.add_timeout(time.time() + timeout_sec, stop_and_fail)
#            loop.start()
#
#            if results['error']:
#                raise results['error']
#
#            return results['result']
#
#        return method


class Collection(object):
    async_collection_ops = async.async_collection_ops.union(set([
        'find', 'find_one'
    ]))

    def __init__(self, database, name):
        assert isinstance(database, Database)
        self.name = name
        self.database = database

        # Get a TornadoCollection
        self._tcoll = getattr(database._tdb, name)
        assert isinstance(self._tcoll, async.TornadoCollection)

    def __getattr__(self, name):
        """
        @param name:            Like 'find', 'remove', 'update', ...
        @return:                A proxy method that will implement the
                                operation synchronously
        """
        real_method = getattr(self._tcoll, name)

        if name not in self.async_collection_ops:
            if isinstance(real_method, sync_pymongo.collection.Collection):
                # This is dotted collection access, e.g. "db.system.indexes"
                assert isinstance(self._tcoll, async.TornadoCollection)
                return Collection(self.database, u"%s.%s" % (self.name, name))
            else:
                return real_method
        else:
            def method(*args, **kwargs):
                results = {}
                loop = IOLoop.instance()

                def callback(result, error):
                    results['result'] = result
                    results['error'] = error
                    loop.stop()

                assert 'callback' not in kwargs
                kwargs['callback'] = callback
                real_method(*args, **kwargs)

                # IOLoop's start() will exit once the callback is called and
                # calls stop(), or after the timeout
                def stop_and_fail():
                    loop.stop()
                    raise Exception("Callback not called before timeout")

                loop.add_timeout(time.time() + timeout_sec, stop_and_fail)
                loop.start()

                if results['error']:
                    raise results['error']

                return results['result']

            return method

    def __getitem__(self, name):
        return self.__getattr__(name)

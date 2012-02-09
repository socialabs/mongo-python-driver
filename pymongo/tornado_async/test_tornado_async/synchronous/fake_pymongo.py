import os
import time
from tornado.ioloop import IOLoop


# So that synchronous unittests can import these names from fake_pymongo,
# thinking it's really pymongo
from pymongo import ASCENDING, DESCENDING, GEO2D, GEOHAYSTACK, ReadPreference
from pymongo.tornado_async import async
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
        self._tc = self.tornado_connection_class(host, port, *args, **kwargs)

        # Try to connect the TornadoConnection before continuing
        loop = IOLoop.instance()
        loop_timeout(timeout_sec, self._tc.open, ConnectionFailure(
            "fake_pymongo.Connection: Can't connect to %s:%s" % (
                host, port
            )
        ))

        loop.start()

    def __getattr__(self, dbname):
        tornado_attr = getattr(self._tc, dbname)
        if isinstance(tornado_attr, async.TornadoDatabase):
            return Database(self._tc, dbname=dbname)
        else:
            return getattr(self._tc, dbname)


class ReplicaSetConnection(Connection):
    tornado_connection_class = async.TornadoReplicaSetConnection


class Database(object):
    def __init__(self, tornado_connection, dbname):
        # Get a TornadoDatabase
        self.__td = getattr(tornado_connection, dbname)
        self.dbname = dbname

    def __getattr__(self, collection_name):
        return Collection(self.__td, collection_name)

    def __getitem__(self, collection_name):
        return Collection(self.__td, collection_name)

#    def command(self, *args, **kwargs):
#        real_method = self.__td.command
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
    def __init__(self, tornado_database, collection_name):
        # Get a TornadoDatabase
        self.__tc = getattr(tornado_database, collection_name)
        self.collection_name = collection_name

    def __getattr__(self, operation_name):
        """
        @param operation_name:  Like 'find', 'remove', 'update', ...
        @return:                A proxy method that will implement the
                                operation synchronously
        """
        real_method = getattr(self.__tc, operation_name)
        if operation_name not in async.async_collection_ops:
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

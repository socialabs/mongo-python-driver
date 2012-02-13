from collections import deque
import os
import time
from tornado.ioloop import IOLoop

# TODO doc WTF this module does

import pymongo as sync_pymongo
from pymongo.tornado_async import async
from pymongo.errors import ConnectionFailure, TimeoutError

# So that synchronous unittests can import these names from fake_pymongo,
# thinking it's really pymongo
from pymongo import ASCENDING, DESCENDING, GEO2D, GEOHAYSTACK, ReadPreference

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


# TODO: better name or iface, document
def loop_timeout(kallable, outcome, exc=None, seconds=timeout_sec):
    assert isinstance(outcome, dict)

    # Make sure outcome starts with result and error as None, and that caller
    # isn't reusing an 'outcome' dict from previous run
    assert outcome.setdefault('result') is None
    assert outcome.setdefault('error') is None

    fail_func = StopAndFail(exc or TimeoutError("timeout"))
    loop = IOLoop.instance()
    loop.add_timeout(time.time() + seconds, fail_func)

    def callback(result, error):
        outcome['result'] = result
        outcome['error'] = error
        fail_func.abort = True
        loop.stop()

    return kallable(callback)


def synchronize(async_method):
    """
    @param async_method:  An asynchronous method defined on a TornadoConnection,
                          TornadoDatabase, etc.
    @return:              A synchronous wrapper around the method
    """
    def synchronized_method(*args, **kwargs):
        outcome = {}
        loop = IOLoop.instance()

        assert 'callback' not in kwargs

        async_rv = loop_timeout(
            lambda cb: async_method(*args, callback=cb, **kwargs),
            outcome
        )

        assert async_rv is None, (
            "Can't use synchronize() if you need to use return value for %s" % (
                async_method
            )
        )

        loop.start()

        if outcome['error']:
            raise outcome['error']

        return outcome['result']

    return synchronized_method


class Connection(object):
    tornado_connection_class = async.TornadoConnection

    def __init__(self, host, port, *args, **kwargs):
        self.host = host
        self.port = port
        self._tconn = self.tornado_connection_class(host, port, *args, **kwargs)

        # Try to connect the TornadoConnection before continuing
        loop_timeout(
            kallable=self._tconn.open,
            outcome={},
            exc=ConnectionFailure(
                "fake_pymongo.Connection: Can't connect to %s:%s" % (
                    host, port
                )
            )
        )

        IOLoop.instance().start()

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
        real_method = getattr(self._tdb, name)

        if name not in async.async_db_ops:
            if isinstance(real_method, async.TornadoCollection):
                return Collection(self, name)
            else:
                # Non-socket operation on a pymongo Database, like
                # database.system_js or _fix_outgoing()
                return real_method
        else:
            return synchronize(real_method)

    # TODO: some way to find a collection called, e.g., 'drop_collection'
    # like db['drop_collection']?
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
    # If 'find' or 'find_one' are turned into regular async_collection_ops
    # instead of special cases, we'll need to update this
    assert 'find' not in async.async_collection_ops
    assert 'find_one' not in async.async_collection_ops
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

    def find(self, *args, **kwargs):
        # Return a fake Cursor that wraps the call to TornadoCollection.find()
        return Cursor(self._tcoll, *args, **kwargs)

    def __getattr__(self, name):
        """
        @param name:            Like 'find', 'remove', 'update', ...
        @return:                A proxy method that will implement the
                                operation synchronously
        """
        real_method = getattr(self._tcoll, name)

        if name not in self.async_collection_ops:
            if isinstance(real_method, async.TornadoCollection):
                # This is dotted collection access, e.g. "db.system.indexes"
                assert isinstance(self._tcoll, async.TornadoCollection)
                return Collection(self.database, u"%s.%s" % (self.name, name))
            else:
                return real_method

        else:
            return synchronize(real_method)

    __getitem__ = __getattr__

    def __cmp__(self, other):
        return cmp(self._tcoll, other._tcoll)


class Cursor(object):
    def __init__(self, tornado_coll, spec=None, fields=None, *args, **kwargs):
        self.tornado_coll = tornado_coll
        self.spec = spec
        self.fields = fields
        self.args = args
        self.kwargs = kwargs
        self.tornado_cursor = None
        self.data = deque()

    def __iter__(self):
        return self

    def next(self):
        if self.data:
            return self.data.popleft()

        outcome = {}

        if not self.tornado_cursor:
            # Start the query
            self.tornado_cursor = loop_timeout(
                kallable=lambda callback: self.tornado_coll.find(
                    *self.args, callback=callback, **self.kwargs
                ),
                outcome=outcome,
            )
        else:
            if not self.tornado_cursor.alive:
                raise StopIteration

            # Continue the query
            loop_timeout(
                kallable=self.tornado_cursor.get_more,
                outcome=outcome,
            )

        IOLoop.instance().start()

        if outcome.get('error'):
            raise outcome['error']

        self.data += outcome['result']

        return self.data.popleft()

    def count(self):
        command = {"query": self.spec, "fields": self.fields}
        outcome = {}
        loop_timeout(
            kallable=lambda callback: self.tornado_coll.database.command(
                "count", self.tornado_coll.name,
                allowable_errors=["ns missing"],
                callback=callback,
                **command
            ),
            outcome=outcome
        )

        IOLoop.instance().start()

        if outcome.get('error'):
            raise outcome['error']

        if outcome['result'].get("errmsg", "") == "ns missing":
            return 0
        return int(outcome['result']['n'])



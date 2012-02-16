from collections import deque
import inspect
import os
import time
from tornado.ioloop import IOLoop

# TODO doc WTF this module does
# TODO sometimes I refer to things as 'async', sometimes as 'tornado' -- maybe
# everything should be called 'motor'?

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
    # TODO: unnecessary with IOLoop.remove_timeout()?
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

        loop_timeout(
            lambda cb: async_method(*args, callback=cb, **kwargs),
            outcome
        )

        loop.start()

        # Ignore errors if caller explicitly passed safe=False; synchronous
        # pymongo wouldn't have known the operation failed.
        if outcome['error'] and kwargs.get('safe'):
            raise outcome['error']

        return outcome['result']

    return synchronized_method


class Fake(object):
    """
    Wraps a TornadoConnection, TornadoDatabase, or TornadoCollection and
    makes it act like the synchronous pymongo equivalent
    """
    def __getattr__(self, name):
        async_obj = getattr(self, self.async_attr, None)
        async_attr = getattr(async_obj, name)

        if name in self.async_ops:
            # async_attr is an async method on a TornadoConnection or something
            return synchronize(async_attr)
        else:
            # If this is like connection.db, or db.test, then wrap the
            # outgoing object in a Fake
            if isinstance(async_attr, async.TornadoDatabase):
                return Database(self, name)
            elif isinstance(async_attr, async.TornadoCollection):
                if isinstance(self, Collection):
                    # Dotted access, like db.test.mike
                    return Collection(self.database, self.name + '.' + name)
                else:
                    return Collection(self, name)
            else:
                # Non-socket operation on a pymongo Database, like
                # database.system_js or _fix_outgoing()
                return async_attr

    __getitem__ = __getattr__

class Connection(Fake):
    async_attr = '_tconn'
    async_connection_class = async.TornadoConnection
    async_ops = async.TornadoConnection.async_ops

    def __init__(self, host, port, *args, **kwargs):
        self.host = host
        self.port = port
        self._tconn = self.async_connection_class(host, port, *args, **kwargs)

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


class ReplicaSetConnection(Connection):
    # fake_pymongo.ReplicaSetConnection is just like fake_pymongo.Connection,
    # except it wraps a TornadoReplicaSetConnection instead of a
    # TornadoConnection.
    async_connection_class = async.TornadoReplicaSetConnection


class Database(Fake):
    async_attr = '_tdb'
    async_ops = async.TornadoDatabase.async_ops

    def __init__(self, connection, name):
        assert isinstance(connection, Connection)
        self.connection = connection

        # Get a TornadoDatabase
        self._tdb = getattr(connection._tconn, name)
        assert isinstance(self._tdb, async.TornadoDatabase)

class Collection(Fake):
    # If async_ops changes, we'll need to update this
    assert 'find' not in async.TornadoCollection.async_ops
    assert 'find_one' not in async.TornadoCollection.async_ops
    assert 'save' not in async.TornadoCollection.async_ops
    assert 'command' not in async.TornadoCollection.async_ops

    async_attr = '_tcoll'
    async_ops = async.TornadoCollection.async_ops.union(set([
        'find', 'find_one', 'save', 'command'
    ]))

    def __init__(self, database, name):
        assert isinstance(database, Database)
        # Get a TornadoCollection
        self._tcoll = getattr(database._tdb, name)
        assert isinstance(self._tcoll, async.TornadoCollection)

        self.database = database

    def find(self, *args, **kwargs):
        # Return a fake Cursor that wraps the call to TornadoCollection.find()
        return Cursor(self._tcoll, *args, **kwargs)

    def __cmp__(self, other):
        return cmp(self._tcoll, other._tcoll)

    # Delegate to TornadoCollection's uuid_subtype property -- TornadoCollection
    # in turn delegates to pymongo.collection.Collection. This is all to get
    # test_uuid_subtype to pass.
    def __get_uuid_subtype(self):
        return self._tcoll.uuid_subtype

    def __set_uuid_subtype(self, subtype):
        self._tcoll.uuid_subtype = subtype

    uuid_subtype = property(__get_uuid_subtype, __set_uuid_subtype)

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
            kallable=lambda callback: self.tornado_coll._tdb.command(
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

    def explain(self):
        if '$query' not in self.spec:
            spec = {'$query': self.spec}
        else:
            spec = self.spec

        spec['$explain'] = True

        return synchronize(self.tornado_coll.find)(spec)[0]

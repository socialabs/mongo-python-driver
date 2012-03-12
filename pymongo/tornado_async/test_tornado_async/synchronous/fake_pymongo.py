from collections import deque
import collections
import inspect
import os
import time
from tornado.ioloop import IOLoop

# TODO doc WTF this module does
# TODO sometimes I refer to things as 'async', sometimes as 'tornado' -- maybe
# everything should be called 'motor'?

import bson
import pymongo as sync_pymongo
from pymongo.tornado_async import async
from pymongo.errors import ConnectionFailure, TimeoutError, OperationFailure

# So that synchronous unittests can import these names from fake_pymongo,
# thinking it's really pymongo
from pymongo import (
    ASCENDING, DESCENDING, GEO2D, GEOHAYSTACK, ReadPreference,
    ALL, helpers, OFF, SLOW_ONLY
)

__all__ = [
    'ASCENDING', 'DESCENDING', 'GEO2D', 'GEOHAYSTACK', 'ReadPreference',
    'Connection', 'ReplicaSetConnection', 'Database', 'Collection',
    'Cursor', 'ALL', 'helpers', 'OFF', 'SLOW_ONLY',
]


timeout_sec = float(os.environ.get('TIMEOUT_SEC', 5))


# TODO: better name or iface, document
def loop_timeout(kallable, exc=None, seconds=timeout_sec, name="<anon>"):
    loop = IOLoop.instance()
    outcome = {}

    def raise_timeout_err():
        loop.stop()
        outcome['error'] = (exc or TimeoutError("timeout"))

    timeout = loop.add_timeout(time.time() + seconds, raise_timeout_err)

    def callback(result, error):
        loop.stop()
        loop.remove_timeout(timeout)
        outcome['result'] = result
        outcome['error'] = error

    kallable(callback)
    assert not loop.running(), "Loop already running in method %s" % name
    loop.start()
    if outcome.get('error'):
        raise outcome['error']

    return outcome['result']


# Methods that don't take a 'safe' argument
methods_without_safe_arg = collections.defaultdict(set)

for klass in (
    sync_pymongo.connection.Connection,
    sync_pymongo.database.Database,
    sync_pymongo.collection.Collection,
    sync_pymongo.cursor.Cursor,
):
    for method_name, method in inspect.getmembers(
        klass,
        inspect.ismethod
    ):
        if 'safe' not in inspect.getargspec(method).args:
            methods = methods_without_safe_arg['Tornado' + klass.__name__]
            methods.add(method.func_name)


def synchronize(async_method):
    """
    @param async_method:  An asynchronous method defined on a TornadoConnection,
                          TornadoDatabase, etc.
    @return:              A synchronous wrapper around the method
    """
    def synchronized_method(*args, **kwargs):
        assert 'callback' not in kwargs
        class_name = async_method.im_self.__class__.__name__
        has_safe_arg = (
            async_method.func_name not in methods_without_safe_arg[class_name]
        )

        if 'safe' not in kwargs and has_safe_arg:
            # By default, Motor passes safe=True if there's a callback, but
            # we don't want that, so we explicitly override.
            kwargs['safe'] = False

        rv = None
        try:
            rv = loop_timeout(
                lambda cb: async_method(*args, callback=cb, **kwargs),
                name=async_method.func_name
            )
        except OperationFailure:
            # Ignore OperationFailure for unsafe writes; synchronous pymongo
            # wouldn't have known the operation failed.
            if kwargs.get('safe') or not has_safe_arg:
                raise

        return rv

    return synchronized_method


class Fake(object):
    """
    Wraps a TornadoConnection, TornadoDatabase, or TornadoCollection and
    makes it act like the synchronous pymongo equivalent
    """
    def __getattr__(self, name):
        async_obj = getattr(self, self.async_attr, None)

        # This if-else seems to replicate the logic of getattr(), except,
        # weirdly, for non-ASCII names like in
        # TestCollection.test_messages_with_unicode_collection_names().
        if name in dir(async_obj):
            async_attr = getattr(async_obj, name)
        else:
            async_attr = async_obj.__getattr__(name)

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

    def __setattr__(self, key, value):
        if key in ('document_class',):
            async_attr = getattr(self, self.async_attr)
            setattr(async_attr, key, value)
        else:
            object.__setattr__(self, key, value)

    def __cmp__(self, other):
        return cmp(
            getattr(self, self.async_attr, None),
            getattr(other, self.async_attr, None),
        )


class Connection(Fake):
    async_attr = '_tconn'
    async_connection_class = async.TornadoConnection
    async_ops = async.TornadoConnection.async_ops
    HOST = 'localhost'
    PORT = 27017

    def __init__(self, host=None, port=None, *args, **kwargs):
        # So that TestConnection.test_constants and test_types work
        self.host = host if host is not None else self.HOST
        self.port = port if port is not None else self.PORT
        self._tconn = self.async_connection_class(
            self.host, self.port, *args, **kwargs
        )

        # Try to connect the TornadoConnection before continuing
        exc = ConnectionFailure(
            "fake_pymongo.Connection: Can't connect to %s:%s" % (host, port)
        )

        loop_timeout(kallable=self._tconn.open, exc=exc)

    def drop_database(self, name_or_database):
        # Special case, since pymongo.Connection.drop_database does
        # isinstance(name_or_database, database.Database)
        if isinstance(name_or_database, Database):
            name_or_database = name_or_database._tdb.name

        drop = super(Connection, self).__getattr__('drop_database')
        return drop(name_or_database)

    # HACK!: For unittests that examine this attribute
    @property
    def _Connection__pool(self):
        return self._tconn.sync_connection._Connection__pool

    @property
    def is_locked(self):
        ops = self.admin.current_op()
        return bool(ops.get('fsyncLock', 0))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._tconn.disconnect()

class ReplicaSetConnection(Connection):
    # fake_pymongo.ReplicaSetConnection is just like fake_pymongo.Connection,
    # except it wraps a TornadoReplicaSetConnection instead of a
    # TornadoConnection.
    async_connection_class = async.TornadoReplicaSetConnection


class Database(Fake):
    async_attr = '_tdb'
    async_ops = async.TornadoDatabase.async_ops

    def __init__(self, connection, name):
        assert isinstance(connection, Connection), (
            "Expected Connection, got %s" % repr(connection)
        )
        self.connection = connection

        # Get a TornadoDatabase
        self._tdb = getattr(connection._tconn, name)
        assert isinstance(self._tdb, async.TornadoDatabase)

class Collection(Fake):
    # If async_ops changes, we'll need to update this
    assert 'find' not in async.TornadoCollection.async_ops

    async_attr = '_tcoll'
    async_ops = async.TornadoCollection.async_ops.union(set([
        'find', 'map_reduce'
    ]))

    def __init__(self, database, name):
        assert isinstance(database, Database)
        # Get a TornadoCollection
        self._tcoll = database._tdb.__getattr__(name)
        assert isinstance(self._tcoll, async.TornadoCollection)

        self.database = database

    def find(self, *args, **kwargs):
        # Return a fake Cursor that wraps the call to TornadoCollection.find()
        return Cursor(self._tcoll, *args, **kwargs)

    def map_reduce(self, *args, **kwargs):
        # We need to override map_reduce specially, because we have to wrap the
        # TornadoCollection it returns in a fake Collection.
        fake_map_reduce = super(Collection, self).__getattr__('map_reduce')
        rv = fake_map_reduce(*args, **kwargs)
        if isinstance(rv, async.TornadoCollection):
            return Collection(self.database, rv.name)
        else:
            return rv

    # Delegate to TornadoCollection's uuid_subtype property -- TornadoCollection
    # in turn delegates to pymongo.collection.Collection. This is all to get
    # test_uuid_subtype to pass.
    def __get_uuid_subtype(self):
        return self._tcoll.uuid_subtype

    def __set_uuid_subtype(self, subtype):
        self._tcoll.uuid_subtype = subtype

    uuid_subtype = property(__get_uuid_subtype, __set_uuid_subtype)

class Cursor(Fake):
    async_attr = 'tornado_cursor'
    async_ops = async.TornadoCursor.async_ops

    def __init__(self, tornado_coll, spec=None, *args, **kwargs):
        self._tcoll = tornado_coll
        self._tcursor = None
        self.args = args
        self.kwargs = kwargs
        self.data = None
        if spec and "$query" in spec:
            self.__spec = spec
        elif spec:
            self.__spec = {"$query": spec}
        else:
            self.__spec = {}

    @property
    def tornado_cursor(self):
        if not self._tcursor:
            # Start the query
            spec = self.__spec
            if "$query" not in spec:
                spec = {"$query":spec}

            self._tcursor = self._tcoll.find(
                spec, *self.args, **self.kwargs
            )

        return self._tcursor

    def __iter__(self):
        return self

    def next(self):
        # The first access of tornado_cursor here will create the cursor
        rv = loop_timeout(self.tornado_cursor.each)
        if rv is not None:
            return rv
        else:
            raise StopIteration

    def where(self, code):
        if not isinstance(code, bson.code.Code):
            code = bson.code.Code(code)

        self.__spec["$where"] = code
        return self
#
#    def count(self):
#        command = {"query": self.spec, "fields": self.fields}
#        return loop_timeout(
#            kallable=lambda callback: self.tornado_coll._tdb.command(
#                "count", self.tornado_coll.name,
#                allowable_errors=["ns missing"],
#                callback=callback,
#                **command
#            )
#        )
#
#    def distinct(self, key):
#        command = {"query": self.spec, "fields": self.fields, "key": key}
#        outcome = {}
#        loop_timeout(
#            kallable=lambda callback: self.tornado_coll._tdb.command(
#                "distinct", self.tornado_coll.name,
#                callback=callback,
#                **command
#            ),
#            outcome=outcome
#        )
#
#        IOLoop.instance().start()
#
#        if outcome.get('error'):
#            raise outcome['error']
#
#        return outcome['result']['values']
#
#    def explain(self):
#        if '$query' not in self.spec:
#            spec = {'$query': self.spec}
#        else:
#            spec = self.spec
#
#        spec['$explain'] = True
#
#        return synchronize(self.tornado_coll.find)(spec)[0]

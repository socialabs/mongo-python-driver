# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Fake PyMongo implementation built on top of Motor, for the sole purpose of
checking that Motor passes the same unittests as PyMongo.

DO NOT USE THIS MODULE.
"""

import collections
import inspect
import os
import time
from tornado.ioloop import IOLoop

# TODO doc WTF this module does
# TODO sometimes I refer to things as 'async', sometimes as 'tornado' -- maybe
# everything should be called 'motor'?

import pymongo as sync_pymongo
from pymongo import son_manipulator
from pymongo.tornado_async import async
from pymongo.tornado_async.delegate import *
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
# TODO: maybe just get rid of this and put it all in synchronize()?
def loop_timeout(kallable, exc=None, seconds=timeout_sec, name="<anon>"):
    loop = IOLoop.instance()
    loop._callbacks[:] = []
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

        # Special case: return False to stop iteration if this callback is
        # being used in Motor's find().each()
        return False

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


class Synchronized(DelegateProperty):
    def __init__(self, safe):
        """
        @param safe: Whether the method takes a 'safe' argument
        """
        super(Synchronized, self).__init__(readonly=True)
        self.safe = safe

    def make_attr(self, cls, name):
        return synchronize(name, self.safe)


def synchronize(method_name, safe):
    """
    @param method_name:         The name of an asynchronous method defined on class
                                TornadoConnection, TornadoDatabase, etc.
    @param safe:     Whether the method takes a 'safe' argument
    @return:                    A synchronous wrapper around the method
    """
    def synchronized_method(self, *args, **kwargs):
        assert 'callback' not in kwargs

        safe_arg_passed = (
            'safe' in kwargs or 'w' in kwargs or 'j' in kwargs
            or 'wtimeout' in kwargs or self.delegate.safe
        )
        
        if not safe_arg_passed and safe:
            # By default, Motor passes safe=True if there's a callback, but
            # we don't want that, so we explicitly override.
            kwargs['safe'] = False

        async_method = getattr(self.delegate, method_name)
        rv = None
        try:
            # TODO: document that all Motor methods accept a callback, but only
            # some require them. Get that into Sphinx somehow.
            rv = loop_timeout(
                lambda cb: async_method(*args, callback=cb, **kwargs),
                name=method_name
            )
        except OperationFailure:
            # Ignore OperationFailure for unsafe writes; synchronous pymongo
            # wouldn't have known the operation failed.
            if safe_arg_passed or not safe:
                raise

        return rv

    synchronized_method.func_name = method_name
    return synchronized_method

# TODO: change name to "Synchronized"? then the current Synchronized will need
# to be renamed. But "Fake" sounds too general for what this does, which is
# specifically to synchronize Motor. I like "Synchro" to extend the "Motor"
# naming scheme.
class FakeMeta(type):
    def __new__(cls, name, bases, attrs):
        # Create the class.
        new_class = type.__new__(cls, name, bases, attrs)

        delegate_class = new_class.__delegate_class__

        if delegate_class:
            delegated_attrs = {}

            # delegated_attrs is created by DelegateMeta
            # TODO: move this loop into Delegator as a method
            for klass in reversed(inspect.getmro(delegate_class)):
                if hasattr(klass, 'delegated_attrs'):
                    delegated_attrs.update(klass.delegated_attrs)

            for name, delegate_attr in delegated_attrs.items():
                if isinstance(delegate_attr, async.Asynchronized):
                    safe = delegate_attr.safe
                    synchronized = Synchronized(safe=safe)
                    sync_attr = synchronized.make_attr(new_class, name)
                    setattr(new_class, name, sync_attr)
                elif isinstance(delegate_attr, DelegateProperty):
                    prop = DelegateProperty(readonly=delegate_attr.readonly)
                    sync_attr = prop.make_attr(new_class, name)
                    setattr(new_class, name, sync_attr)

        return new_class


class Fake(object):
    """
    Wraps a TornadoConnection, TornadoDatabase, or TornadoCollection and
    makes it act like the synchronous pymongo equivalent
    """
    __metaclass__ = FakeMeta
    __delegate_class__ = None

    def __init__(self, delegate):
        self.delegate = delegate

    def __cmp__(self, other):
        return cmp(self.delegate, other.delegate)

#    document_class = DelegateProperty()
#    slave_okay = DelegateProperty()
#    safe = DelegateProperty()
#    get_lasterror_options = DelegateProperty()
#    set_lasterror_options = DelegateProperty()
#    unset_lasterror_options = DelegateProperty()
#    _BaseObject__set_slave_okay = DelegateProperty()
#    _BaseObject__set_safe = DelegateProperty()


class Connection(Fake):
    HOST = 'localhost'
    PORT = 27017

    __delegate_class__ = async.TornadoConnection

    def __init__(self, host=None, port=None, *args, **kwargs):
        # So that TestConnection.test_constants and test_types work
        self.host = host if host is not None else self.HOST
        self.port = port if port is not None else self.PORT
        tornado_connection = self.__delegate_class__(
            self.host, self.port, *args, **kwargs
        )

        super(Connection, self).__init__(delegate=tornado_connection)

        # Try to connect the TornadoConnection before continuing
        exc = ConnectionFailure(
            "fake_pymongo.Connection: Can't connect to %s:%s" % (host, port)
        )

        loop_timeout(kallable=self.delegate.open, exc=exc)

    def drop_database(self, name_or_database):
        # Special case, since pymongo Connection.drop_database does
        # isinstance(name_or_database, database.Database)
        if isinstance(name_or_database, Database):
            name_or_database = name_or_database.delegate

        synchronize('drop_database', safe=False)(self, name_or_database)

    # TODO: document how this is implemented by Motor
    @property
    def is_locked(self):
        ops = self.admin.current_op()
        return bool(ops.get('fsyncLock', 0))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.delegate.disconnect()

    def __getattr__(self, name):
        # If this is like connection.db, then wrap the outgoing object in a Fake
        return Database(self, name)

    __getitem__ = __getattr__
#
#    # HACK!: For unittests that examine this attribute
#    _Connection__pool = DelegateProperty()
#    unlock = Synchronized(safe=False)


class ReplicaSetConnection(Connection):
    # fake_pymongo.ReplicaSetConnection is just like fake_pymongo.Connection,
    # except it wraps a TornadoReplicaSetConnection instead of a
    # TornadoConnection.
    __delegate_class__ = async.TornadoReplicaSetConnection


class Database(Fake):
    __delegate_class__ = async.TornadoDatabase

    def __init__(self, connection, name):
        assert isinstance(connection, Connection), (
            "Expected Connection, got %s" % repr(connection)
        )
        self.connection = connection

        tornado_db = getattr(connection.delegate, name)
        assert isinstance(tornado_db, async.TornadoDatabase)
        super(Database, self).__init__(delegate=tornado_db)

    def add_son_manipulator(self, manipulator):
        if isinstance(manipulator, son_manipulator.AutoReference):
            db = manipulator.database
            if isinstance(db, Database):
                manipulator.database = db.delegate.sync_database

        self.delegate.add_son_manipulator(manipulator)

    # TODO: refactor, maybe something like fix_incoming or fix_outgoing?
    def drop_collection(self, name_or_collection):
        # Special case, since pymongo Database.drop_collection does
        # isinstance(name_or_collection, collection.Collection)
        if isinstance(name_or_collection, Collection):
            name_or_collection = name_or_collection.delegate

        return synchronize('drop_collection', safe=False)(self, name_or_collection)

    # TODO: refactor
    def validate_collection(self, name_or_collection, *args, **kwargs):
        # Special case, since pymongo Database.validate_collection does
        # isinstance(name_or_collection, collection.Collection)
        if isinstance(name_or_collection, Collection):
            name_or_collection = name_or_collection.delegate

        return synchronize('validate_collection', safe=False)(
            self, name_or_collection, *args, **kwargs
        )

    # TODO: refactor
    def create_collection(self, name, *args, **kwargs):
        # Special case, since TornadoDatabase.create_collection returns a
        # TornadoCollection
        collection = synchronize('create_collection', safe=False)(
            self, name, *args, **kwargs
        )

        if isinstance(collection, async.TornadoCollection):
            collection = Collection(self, name)

        return collection

    current_op = Synchronized(safe=False)
    fsync = Synchronized(safe=False)
    command = Synchronized(safe=True)

    def __getattr__(self, name):
        return Collection(self, name)

    __getitem__ = __getattr__


class Collection(Fake):
    __delegate_class__ = async.TornadoCollection
    (find, map_reduce, remove) = [Synchronized(safe=False)] * 3

    def __init__(self, database, name):
        assert isinstance(database, Database)
        self.database = database

        tornado_collection = database.delegate.__getattr__(name)
        assert isinstance(tornado_collection, async.TornadoCollection)
        super(Collection, self).__init__(delegate=tornado_collection)

    def find(self, *args, **kwargs):
        # Return a fake Cursor that wraps the call to TornadoCollection.find()
        return Cursor(self.delegate, *args, **kwargs)

    def map_reduce(self, *args, **kwargs):
        # We need to override map_reduce specially, because we have to wrap the
        # TornadoCollection it returns in a fake Collection.
        # TODO: this getattr won't work any more
        fake_map_reduce = super(Collection, self).__getattr__('map_reduce')
        rv = fake_map_reduce(*args, **kwargs)
        if isinstance(rv, async.TornadoCollection):
            return Collection(self.database, rv.name)
        else:
            return rv

    uuid_subtype = DelegateProperty()
    find_one = Synchronized(safe=True)
    insert = Synchronized(safe=True)

    def __getattr__(self, name):
        # Access to collections with dotted names, like db.test.mike
        this_collection_name = self.delegate.delegate.name
        return Collection(self.database, this_collection_name + '.' + name)

    __getitem__ = __getattr__

    
class Cursor(Fake):
    __delegate_class__ = async.TornadoCursor

    def __init__(self, tornado_coll, *args, **kwargs):
        tornado_cursor = tornado_coll.find(*args, **kwargs)
        super(Cursor, self).__init__(delegate=tornado_cursor)

    def __iter__(self):
        return self

    def next(self):
        rv = loop_timeout(self.delegate.each)
        if rv is not None:
            return rv
        else:
            raise StopIteration

    # TODO: refactor these
    def where(self, code):
        self.delegate.where(code)
        return self

    def sort(self, *args, **kwargs):
        self.delegate.sort(*args, **kwargs)
        return self

    def explain(self):
        return loop_timeout(self.delegate.explain)

    def __getitem__(self, item):
        rv = loop_timeout(self.delegate.to_list)
        return rv[item]

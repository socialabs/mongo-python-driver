# Copyright 2011-2012 10gen, Inc.
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

"""Tornado asynchronous Python driver for MongoDB.

Not implemented:
Various methods on pymongo Cursor, like explain(), count(), distinct(), hint(),
or where() on a cursor -- use arguments to find() or database commands directly
instead.
"""
import functools
import inspect
import socket

import tornado.ioloop, tornado.iostream
from tornado import stack_context
import greenlet

from bson.binary import OLD_UUID_SUBTYPE
from bson.son import SON

import pymongo
from pymongo.errors import InvalidOperation


have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False


__all__ = ['TornadoConnection', 'TornadoReplicaSetConnection']

# TODO: sphinx-formatted docstrings
# TODO: note you can't use from multithreaded app, consider special checks
# to prevent it?
# TODO: convenience method for count()? How does Node do it?
# TODO: maybe Tornado classes should inherit from pymongo classes after all
# TODO: change all asserts into pymongo.error exceptions

def check_callable(kallable, required=False):
    if required and not kallable:
        raise TypeError("callable is required")
    if kallable is not None and not callable(kallable):
        raise TypeError("callback must be callable")

class TornadoSocket(object):
    """
    Replace socket with a class that yields from the current greenlet, if we're
    on a child greenlet, when making blocking calls, and uses Tornado IOLoop to
    schedule child greenlet for resumption when I/O is ready.

    We only implement those socket methods actually used by pymongo.
    """
    def __init__(self, sock, use_ssl=False):
        self.use_ssl = use_ssl
        if self.use_ssl:
           self.stream = tornado.iostream.SSLIOStream(sock)
        else:
           self.stream = tornado.iostream.IOStream(sock)

    def setsockopt(self, *args, **kwargs):
        self.stream.socket.setsockopt(*args, **kwargs)

    def settimeout(self, timeout):
        """
        Do nothing -- IOStream calls socket.setblocking(False), which does
        settimeout(0.0). We must not allow pymongo to set timeout to some other
        value (a positive number or None) or the socket will start blocking
        again.
        """
        pass

    def connect(self, pair):
        """
        @param pair: A tuple, (host, port)
        """
        child_gr = greenlet.getcurrent()
        assert child_gr.parent, "Should be on child greenlet"

        # This is run by IOLoop on the main greenlet when socket has connected;
        # switch back to child to continue processing
        def connect_callback():
            child_gr.switch()

        self.stream.connect(pair, callback=connect_callback)

        # Resume main greenlet
        child_gr.parent.switch()

    def sendall(self, data):
        child_gr = greenlet.getcurrent()
        assert child_gr.parent, "Should be on child greenlet"

        # This is run by IOLoop on the main greenlet when data has been sent;
        # switch back to child to continue processing
        def sendall_callback():
            child_gr.switch()

        self.stream.write(data, callback=sendall_callback)

        # Resume main greenlet
        child_gr.parent.switch()

    def recv(self, num_bytes):
        """
        @param num_bytes:   Number of bytes to read from socket
        @return:            Data received
        """
        child_gr = greenlet.getcurrent()
        assert child_gr.parent, "Should be on child greenlet"

        # This is run by IOLoop on the main greenlet when socket has connected;
        # switch back to child to continue processing
        def recv_callback(data):
            child_gr.switch(data)

        self.stream.read_bytes(num_bytes, callback=recv_callback)

        # Resume main greenlet, returning the data received
        return child_gr.parent.switch()

    def close(self):
        self.stream.close()

    def fileno(self):
        """
        See connection._closed(sock), which checks if it can do select([sock])
        without error to determine if the socket is closed. select() requires
        a fileno() method on each object.
        """
        return self.stream.socket.fileno()

    def __del__(self):
        self.close()


class TornadoPool(pymongo.pool.Pool):
    """A simple connection pool of TornadoSockets.
    """
    def connect(self, pair):
        """Connect to Mongo and return a new connected TornadoSocket.
        """
        assert greenlet.getcurrent().parent, "Should be on child greenlet"
        self._check_pair_arg(pair)

        # Prefer IPv4. If there is demand for an option
        # to specify one or the other we can add it later.
        socket_types = (socket.AF_INET, socket.AF_INET6)
        for socket_type in socket_types:
            try:
                s = socket.socket(socket_type)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                break
            except socket.gaierror:
                # If that fails try IPv6
                continue
        else:
            # None of the socket types worked
            raise

        tornado_sock = TornadoSocket(s, use_ssl=self.use_ssl)

        # TornadoSocket will pause the current greenlet and resume it when
        # connection has completed
        tornado_sock.connect(pair or self.pair)
        return tornado_sock

    def _request_key(self):
        # Module-level variable set by RequestContext
        return current_request


def asynchronize(sync_method, callback_required, sync_attr):
    """
    @param sync_method: unbound method of a pymongo Collection, Database,
                        or Connection
    @param callback_required: If True, raise TypeError if no callback is passed
    @param sync_attr:   Like 'sync_connection', the async class's wrapped member
    """
    # TODO doc
    # TODO: staticmethod of base class for Tornado objects, add some custom
    #   stuff, like Connection can't do anything before open()
    def method(self, *args, **kwargs):
        """
        @param self:    A TornadoConnection, TornadoDatabase, or
                        TornadoCollection
        """
        assert isinstance(self, (
            TornadoConnection, TornadoDatabase, TornadoCollection
        ))

        callback = kwargs.get('callback')
        check_callable(callback, required=callback_required)

        if 'callback' in kwargs:
            # Don't pass callback to sync_method
            kwargs = kwargs.copy()
            del kwargs['callback']

        # TODO: find another way, or at least cache this info in case
        # getargspec() is expensive.
        # TODO: document that with a callback passed in, Motor's default is
        # to do SAFE writes, unlike PyMongo.
        # ALSO TODO: should Motor's default be safe writes, or no?
        if ('safe' not in kwargs
            and 'safe' in inspect.getargspec(sync_method).args
        ):
            kwargs['safe'] = bool(callback)

        def call_method():
            result, error = None, None
            try:
                sync_obj = getattr(self, sync_attr)
                result = sync_method(sync_obj, *args, **kwargs)
            except Exception, e:
                error = e

            # Schedule the callback to be run on the main greenlet
            if callback:
                tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: callback(result, error)
                )

        # Start running the operation on a greenlet
        # TODO: a possible optimization that doesn't start a greenlet if no
        # callback -- probably not a significant improvement....
        greenlet.greenlet(call_method).switch()

    return functools.wraps(sync_method)(method)


current_request = None
current_request_seq = 0


# TODO: better name, with 'Mongo' in it!
class TornadoConnection(object):
    # list of overridden async operations on a TornadoConnection instance
    async_ops = set([
        'drop_database', 'database_names', 'close_cursor', 'kill_cursors',
        'server_info', 'database_names', 'drop_database', 'copy_database',
        'fsync', 'unlock',
    ])

    def __init__(self, *args, **kwargs):
        # Store args and kwargs for when open() is called
        self._init_args = args
        self._init_kwargs = kwargs

        # The synchronous pymongo Connection
        self.sync_connection = None
        self.connected = False

    def open(self, callback):
        """
        Actually connect, passing self to a callback when connected.
        @param callback: Optional function taking parameters (connection, error)
        """
        # TODO: connect on demand? Remove open() as a public method?
        check_callable(callback)

        if self.connected:
            callback(self, None)
            return
        
        def connect():
            # Run on child greenlet
            # TODO: can this use asynchronize()?
            error = None
            try:
                self.sync_connection = pymongo.Connection(
                    *self._init_args,
                    _pool_class=TornadoPool,
                    **self._init_kwargs
                )
                
                self.connected = True
            except Exception, e:
                error = e

            if callback:
                # Schedule callback to be executed on main greenlet, with
                # (self, None) if no error, else (None, error)
                tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: callback(
                        None if error else self,
                        error
                    )
                )

        # Actually connect on a child greenlet
        greenlet.greenlet(connect).switch()

    def __getattr__(self, name):
        """
        Override pymongo Connection's attributes to replace blocking operations
        with async alternatives, and to get references to TornadoDatabase
        instances instead of Database.
        @param name:            Like 'drop_database', 'database_names', ...
        @return:                A proxy method that will implement the operation
                                asynchronously, and requires a 'callback' kwarg
        """
        if name in dir(pymongo.connection.Connection):
            return getattr(self.sync_connection, name)
        else:
            # We're getting a database from the connection
            if not self.connected:
                raise InvalidOperation(
                    "Can't access database on TornadoConnection before"
                    " calling open()"
                )
            return TornadoDatabase(self, name)

    __getitem__ = __getattr__

    def __repr__(self):
        return 'TornadoConnection(%s)' % (
            ','.join([
            i for i in [
                ','.join([str(i) for i in self._init_args]),
                ','.join(['%s=%s' % (k, v) for k, v in self._init_kwargs.items()]),
                ] if i
            ])
            )

    # Special case -- this is a property on the synchronous collection
    def is_locked(self, callback):
        def inner_cb(result, error):
            if error:
                callback(None, error)
            else:
                callback(bool(result.get('fsyncLock', 0)), None)

        self.current_op(inner_cb)

    def start_request(self):
        """Assigns a socket to the current thread or greenlet until it calls
        :meth:`end_request`

           Unlike a regular pymongo Connection, start_request() on a
           TornadoConnection *must* be used as a context manager:
        >>> connection = TorndadoConnection()
        >>> db = connection.test
        >>> def on_error(result, error):
        ...     print 'getLastError returned:', result
        ...
        >>> with connection.start_request():
        ...     # unsafe inserts, second one violates unique index on _id
        ...     db.collection.insert({'_id': 1})
        ...     db.collection.insert({'_id': 1})
        ...     # call getLastError. Because we're in a request, error() uses
        ...     # same socket as insert, and gets the error message from
        ...     # last insert.
        ...     db.error(callback=on_error)
        """
        # TODO: raise an informative NotImplementedError in end_request(),
        #   since Motor only supports the with-clause style; test that
        # TODO: this is so spaghetti & implicit & magic
        global current_request, current_request_seq

        # TODO: overflow? What about py3k?
        current_request_seq += 1
        request_id = current_request_seq
        current_request = request_id
        sync_connection = self.sync_connection
        sync_connection.start_request()
        return RequestContext(sync_connection, request_id)


class RequestContext(stack_context.StackContext):
    def __init__(self, sync_connection, request_id):
        class RequestContextFactoryFactory(object):
            def __del__(self):
                global current_request
                assert current_request is None

                # TODO: see how much this sucks & needs to be rewritten?
                current_request = request_id
                sync_connection.end_request()
                current_request = None

            def __call__(self):
                class RequestContextFactory(object):
                    def __enter__(self):
                        global current_request
                        current_request = request_id

                    def __exit__(self, type, value, traceback):
                        global current_request
                        assert current_request == request_id, (
                            "request_id %s does not match expected %s" % (
                                current_request, request_id
                            )
                        )
                        current_request = None
                        if value:
                            raise value

                return RequestContextFactory()

        super(RequestContext, self).__init__(RequestContextFactoryFactory())


# Replace synchronous methods like 'drop_database' with async versions
for op in TornadoConnection.async_ops:
    sync_method = getattr(pymongo.connection.Connection, op)
    setattr(TornadoConnection, op, asynchronize(
        sync_method, False, 'sync_connection')
    )


class TornadoReplicaSetConnection(object):
    def __init__(self, *args, **kwargs):
        # TODO
        pass


class TornadoDatabase(object):
    # list of overridden async operations on a TornadoDatabase instance
    async_ops = set([
        'create_collection', 'drop_collection', 'collection_names',
        'validate_collection', 'current_op', 'profiling_level',
        'set_profiling_level', 'profiling_info', 'error', 'last_status',
        'previous_error', 'reset_error_history', 'add_user', 'remove_user',
        'authenticate', 'logout', 'dereference', 'eval', 'command',
    ])

    # operations for which a callback is required
    callback_ops = set([
        'collection_names', 'validate_collection', 'current_op',
        'profiling_level', 'profiling_info', 'error', 'last_status',
        'previous_error', 'dereference', 'eval',
    ])

    def __init__(self, connection, name, *args, **kwargs):
        # *args and **kwargs are not currently supported by pymongo Database,
        # but it doesn't cost us anything to include them and future-proof
        # this method
        self.name = name
        if isinstance(connection, TornadoConnection):
            self.connection = connection
            self.sync_database = pymongo.database.Database(
                connection.sync_connection, name, *args, **kwargs
            )
        else:
            # TODO: should we support this?
            assert isinstance(connection, pymongo.connection.Connection)
            assert False, "Can't make TornadoDatabase from pymongo Connection"
            self.connection = TornadoConnection(connection)
            self.sync_database = pymongo.database.Database(
                connection, name, *args, **kwargs
            )

    def __getattr__(self, name):
        """
        Override pymongo Database's attributes to replace blocking operations
        with async alternatives, and to get references to TornadoCollection
        instances instead of Database.
        @param name:            Like 'drop_collection', 'collection_names', ...
        @return:                A proxy method that will implement the operation
                                asynchronously if provided a callback
        """
        if name not in dir(pymongo.database.Database):
            return TornadoCollection(self, name)
        else:
            # Just a regular attribute that doesn't use the network, e.g.
            # self.name
            return getattr(self.sync_database, name)

    def __getitem__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return self.__getattr__(name)

    def __repr__(self):
        return 'Tornado' + self.sync_database.__repr__()

    def __cmp__(self, other):
        return cmp(self.sync_database, other.sync_database)


# Replace synchronous methods like 'command' with async versions
for op in TornadoDatabase.async_ops:
    sync_method = getattr(pymongo.database.Database, op)
    setattr(TornadoDatabase, op, asynchronize(
        sync_method, op in TornadoDatabase.callback_ops,
        'sync_database'))


class TornadoCollection(object):
    # list of overridden async operations on a TornadoCollection instance
    async_ops = set([
        'update', 'insert', 'remove', 'create_index', 'index_information',
        'drop_indexes', 'drop_index', 'drop', 'count', 'ensure_index',
        'reindex', 'options', 'group', 'rename', 'distinct',
        'inline_map_reduce', 'find_and_modify', 'save', 'find_one',
    ])

    # operations for which a callback is required
    callback_ops = set([
        'index_information', 'count', 'options', 'group', 'distinct',
        'inline_map_reduce', 'find_one',
    ])

    def __init__(self, database, name, *args, **kwargs):
        if isinstance(database, TornadoDatabase):
            self._tdb = database
            self.sync_collection = pymongo.collection.Collection(
                self._tdb.sync_database, name
            )
        else:
            assert isinstance(database, pymongo.database.Database)
            assert False, (
                "TODO: support creating TornadoCollection from pymongo"
                "Database?"
            )

            self._tdb = TornadoDatabase(
                database.connection, name, *args, **kwargs
            )

            self.sync_collection = pymongo.collection.Collection(
                database, name
            )

    def __getattr__(self, name):
        """
        Override pymongo Collection's attributes to replace the basic CRUD
        operations with async alternatives.
        # TODO: Just override them explicitly?
        @param name:            Like 'find', 'remove', 'update', ...
        @return:                A proxy method that will implement the operation
                                asynchronously if provided a callback
        """
        # Get pymongo's synchronous method for this operation
        sync_method = getattr(self.sync_collection, name)

        # TODO: cheaper way to find if something's a collection than
        # actually instantiating one? what about:
        # name in dir(self.sync_collection)
        if isinstance(sync_method, pymongo.collection.Collection):
            # dotted collection name, like foo.bar
            return TornadoCollection(
                self._tdb,
                self.name + '.' + sync_method.name
            )

        return sync_method

    # Delegate to pymongo.collection.Collection's uuid_subtype property
    def __get_uuid_subtype(self):
        return self.sync_collection.uuid_subtype

    def __set_uuid_subtype(self, subtype):
        self.sync_collection.uuid_subtype = subtype

    uuid_subtype = property(__get_uuid_subtype, __set_uuid_subtype)

    def find(self, *args, **kwargs):
        """
        Run an async find(), and return a TornadoCursor, rather than returning a
        pymongo Cursor for synchronous operations.
        """
        callback = kwargs.get('callback')
        check_callable(callback, required=True)
        kwargs = kwargs.copy()
        del kwargs['callback']

        # Getting the cursor doesn't actually use a socket yet; it's get_more
        # that we have to make non-blocking
        cursor = self.sync_collection.find(*args, **kwargs)
        tornado_cursor = TornadoCursor(cursor)
        tornado_cursor.get_more(callback)

        # When the greenlet has sent the query on the socket, it will switch
        # back to the main greenlet, here, and we return to the caller.
        return tornado_cursor

    def map_reduce(self, *args, **kwargs):
        # We need to override map_reduce specially, rather than simply
        # include it in async_ops, because we have to wrap the Collection it
        # returns in a TornadoCollection.

        callback = kwargs.get('callback')
        check_callable(callback, required=False)
        if 'callback' in kwargs:
            kwargs = kwargs.copy()
            del kwargs['callback']

        def inner_cb(result, error):
            if isinstance(result, pymongo.collection.Collection):
                result = self._tdb[result.name]
            callback(result, error)

        async_mr = asynchronize(
            pymongo.collection.Collection.map_reduce,
            False,
            'sync_collection'
        )

        async_mr(self, *args, callback=inner_cb, **kwargs)

    def __repr__(self):
        return 'Tornado' + repr(self.sync_collection)

    def __cmp__(self, other):
        return cmp(self.sync_collection, other.sync_collection)


# Replace synchronous methods like 'insert' or 'drop' with async versions
for op in TornadoCollection.async_ops:
    sync_method = getattr(pymongo.collection.Collection, op)
    setattr(TornadoCollection, op, asynchronize(
        sync_method, op in TornadoCollection.callback_ops,
        'sync_collection'
    ))


class TornadoCursor(object):
    def __init__(self, cursor):
        """
        @param cursor:  Synchronous pymongo.Cursor
        """
        self.sync_cursor = cursor
        self.started = False

    def get_more(self, callback):
        """
        Get a batch of data asynchronously, either performing an initial query
        or getting more data from an existing cursor.
        @param callback:    Optional function taking parameters (result, error)
        """
        # TODO: shorten somehow?
        check_callable(callback)
        assert not self.sync_cursor._Cursor__killed
        if self.started and not self.alive:
            raise InvalidOperation(
                "Can't call get_more() on an TornadoCursor that has been"
                " exhausted or killed."
            )

        def _get_more():
            # This is executed on child greenlet
            result, error = None, None
            try:
                self.started = True
                self.sync_cursor._refresh()

                # TODO: Make this accessible w/o underscore hack
                result = self.sync_cursor._Cursor__data
                self.sync_cursor._Cursor__data = []
            except Exception, e:
                error = e

            # Execute callback on main greenlet
            tornado.ioloop.IOLoop.instance().add_callback(
                lambda: callback(result, error)
            )

        greenlet.greenlet(_get_more).switch()

        # When the greenlet has sent the query on the socket, it will switch
        # back to the main greenlet, here, and we return to the caller.
        return None

    @property
    def alive(self):
        """Does this cursor have the potential to return more data?"""
        return bool(
            self.sync_cursor.alive and self.sync_cursor._Cursor__id
        )

    def close(self):
        """Explicitly close this cursor.
        """
        greenlet.greenlet(self.sync_cursor.close).switch()

    def __del__(self):
        if self.alive:
            self.close()

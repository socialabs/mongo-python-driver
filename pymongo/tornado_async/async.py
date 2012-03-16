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
import time

import tornado.ioloop, tornado.iostream
from tornado import stack_context
import greenlet


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
# TODO: set default timeout to None, document that, ensure we're doing
#   timeouts as efficiently as possible
# TODO: examine & document what connection and network timeouts mean here

def check_callable(kallable, required=False):
    if required and not kallable:
        raise TypeError("callable is required")
    if kallable is not None and not callable(kallable):
        raise TypeError("callback must be callable")


def tornado_sock_method(method):
    @functools.wraps(method)
    def _tornado_sock_method(self, *args, **kwargs):
        child_gr = greenlet.getcurrent()
        assert child_gr.parent, "Should be on child greenlet"

        # We need to alter this value in inner functions, hence the list
        self_timeout = self.timeout
        timeout = [None]
        loop = tornado.ioloop.IOLoop.instance()
        if self_timeout:
            def timeout_err():
                timeout[0] = None
                self.stream.close()
                child_gr.throw(socket.timeout("timed out"))

            timeout[0] = loop.add_timeout(
                time.time() + self_timeout, timeout_err
            )

        # This is run by IOLoop on the main greenlet when socket has connected;
        # switch back to child to continue processing
        def callback(result=None, error=None):
            if timeout[0] or not self_timeout:
                # We didn't time out
                if timeout[0]:
                    loop.remove_timeout(timeout[0])

                if error:
                    child_gr.throw(error)
                else:
                    child_gr.switch(result)

        method(self, *args, callback=callback, **kwargs)

        # Resume main greenlet
        return child_gr.parent.switch()

    return _tornado_sock_method

class TornadoSocket(object):
    """
    Replace socket with a class that yields from the current greenlet, if we're
    on a child greenlet, when making blocking calls, and uses Tornado IOLoop to
    schedule child greenlet for resumption when I/O is ready.

    We only implement those socket methods actually used by pymongo.
    """
    def __init__(self, sock, use_ssl=False):
        self.use_ssl = use_ssl
        self.timeout = None
        if self.use_ssl:
           self.stream = tornado.iostream.SSLIOStream(sock)
        else:
           self.stream = tornado.iostream.IOStream(sock)

    def setsockopt(self, *args, **kwargs):
        self.stream.socket.setsockopt(*args, **kwargs)

    def settimeout(self, timeout):
        # IOStream calls socket.setblocking(False), which does settimeout(0.0).
        # We must not allow pymongo to set timeout to some other value (a
        # positive number or None) or the socket will start blocking again.
        # Instead, we simulate timeouts by interrupting ourselves with
        # callbacks.
        self.timeout = timeout

    @tornado_sock_method
    def connect(self, pair, callback):
        """
        @param pair: A tuple, (host, port)
        """
        self.stream.connect(pair, callback)

    @tornado_sock_method
    def sendall(self, data, callback):
        self.stream.write(data, callback)

    @tornado_sock_method
    def recv(self, num_bytes, callback):
        self.stream.read_bytes(num_bytes, callback)

    def close(self):
        self.stream.close()

    def fileno(self):
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
        tornado_sock.settimeout(self.conn_timeout)

        # TornadoSocket will pause the current greenlet and resume it when
        # connection has completed
        tornado_sock.connect(pair or self.pair)
        tornado_sock.settimeout(self.net_timeout)
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
    @functools.wraps(sync_method)
    def method(self, *args, **kwargs):
        """
        @param self:    A TornadoConnection, TornadoDatabase, or
                        TornadoCollection
        """
        assert isinstance(self, (
            TornadoConnection, TornadoDatabase, TornadoCollection,
            TornadoCursor
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
            elif error:
                # TODO: correct?
                raise error


        # Start running the operation on a greenlet
        # TODO: a possible optimization that doesn't start a greenlet if no
        # callback -- probably not a significant improvement....
        greenlet.greenlet(call_method).switch()

    return method


current_request = None
current_request_seq = 0


# TODO: better name, with 'Mongo' or 'Motor' in it!
class TornadoConnection(object):
    # list of overridden async operations on a TornadoConnection instance
    async_ops = set([
        'database_names', 'close_cursor', 'kill_cursors',
        'server_info', 'copy_database', 'fsync', 'unlock',
    ])

    # operations for which a callback is required
    # TODO: test all these w/ callbacks?
    callback_ops = set([
        'database_names', 'server_info', 'fsync', 'unlock'
    ])

    wrapped_attrs = set(['document_class'])

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
            if callback:
                callback(self, None)
            return
        
        def connect():
            # Run on child greenlet
            # TODO: can this use asynchronize()?
            error = None
            # TODO: reconsider, doc
            connectTimeoutMS = self._init_kwargs.get('connectTimeoutMS')
            socketTimeoutMS = self._init_kwargs.get('socketTimeoutMS')
            actual_timeout = max(connectTimeoutMS, socketTimeoutMS)

            try:
                self._init_kwargs['socketTimeoutMS'] = actual_timeout

                self.sync_connection = pymongo.Connection(
                    *self._init_args,
                    _pool_class=TornadoPool,
                    **self._init_kwargs
                )

                self.connected = True
            except Exception, e:
                error = e

            if self.sync_connection:
                # TODO: gross, and consider if what to do if it's an RSC
                self.sync_connection._Connection__net_timeout = socketTimeoutMS

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
        @param name:            Like 'database_names', 'server_info', ...
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

    def __setattr__(self, key, value):
        if key in self.wrapped_attrs:
            setattr(self.sync_connection, key, value)
        else:
            object.__setattr__(self, key, value)

    def __repr__(self):
        return 'TornadoConnection(%s)' % (
            ','.join([
            i for i in [
                ','.join([str(i) for i in self._init_args]),
                ','.join(['%s=%s' % (k, v) for k, v in self._init_kwargs.items()]),
                ] if i
            ])
        )

    # TODO: refactor
    def __cmp__(self, other):
        if isinstance(other, TornadoConnection):
            return cmp(self.sync_connection, other.sync_connection)
        return NotImplemented

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
        ...     db.collection.insert({'_id': 1}, callback=next_step)
        # TODO: update doc, or delete requests entirely from Motor
        >>> def next_step(result, error):
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

    # TODO: doc why we need to override this
    def drop_database(self, name_or_database, callback):
        name = name_or_database
        if isinstance(name, TornadoDatabase):
            name = name.sync_database.name

        sync_method = getattr(pymongo.connection.Connection, 'drop_database')
        async_method = asynchronize(sync_method, True, 'sync_connection')
        async_method(self, name, callback=callback)


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


# Replace synchronous methods like 'database_names' with async versions
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
        'create_collection', 'collection_names',
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
        @param name:            Like 'create_collection', 'collection_names', ...
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

    # TODO: refactor
    def __cmp__(self, other):
        if isinstance(other, TornadoDatabase):
            return cmp(self.sync_database, other.sync_database)
        return NotImplemented

    # TODO: doc why we need to override this
    def drop_collection(self, name_or_collection, callback):
        name = name_or_collection
        if isinstance(name, TornadoCollection):
            name = name.sync_collection.name

        sync_method = getattr(pymongo.database.Database, 'drop_collection')
        async_method = asynchronize(sync_method, True, 'sync_database')
        async_method(self, name, callback=callback)

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
        Get a TornadoCursor.
        """
        # TODO: better message
        assert 'callback' not in kwargs, (
            "Pass a callback to each() or to_list(), not find()"
        )

        cursor = self.sync_collection.find(*args, **kwargs)
        tornado_cursor = TornadoCursor(cursor)
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

    # TODO: refactor
    def __cmp__(self, other):
        if isinstance(other, TornadoDatabase):
            return cmp(self.sync_collection, other.sync_collection)
        return NotImplemented


# Replace synchronous methods like 'insert' or 'drop' with async versions
for op in TornadoCollection.async_ops:
    sync_method = getattr(pymongo.collection.Collection, op)
    setattr(TornadoCollection, op, asynchronize(
        sync_method, op in TornadoCollection.callback_ops,
        'sync_collection'
    ))


class TornadoCursor(object):
    # list of overridden async operations on a TornadoCursor instance
    async_ops = set([
        'count', 'distinct'
    ])

    def __init__(self, cursor):
        """
        @param cursor:  Synchronous pymongo Cursor
        """
        self.sync_cursor = cursor
        self.started = False

        # Number of documents buffered in sync_cursor
        self.batched_size = 0

    def _get_more(self, callback):
        """
        Get a batch of data asynchronously, either performing an initial query
        or getting more data from an existing cursor.
        @param callback:    function taking parameters (batch_size, error)
        """
        if self.started and not self.alive:
            raise InvalidOperation(
                "Can't call get_more() on an TornadoCursor that has been"
                " exhausted or killed."
            )

        self.started = True
        async_refresh = asynchronize(
            pymongo.cursor.Cursor._refresh, True, 'sync_cursor',
        )

        async_refresh(self, callback=callback)

    def each(self, callback):
        """Iterates over all the documents for this cursor. Return False from
        the callback to stop iteration. each returns immediately, and your
        callback is executed asynchronously for each document. callback is
        passed (None, None) when iteration is complete.

        @param callback: function taking (document, error)
        """
        check_callable(callback, required=True)

        # TODO: simplify, review

        # TODO: remove so we don't have to use double-underscore hack
        assert self.batched_size == len(self.sync_cursor._Cursor__data)

        while self.batched_size > 0:
            try:
                doc = self.sync_cursor.next()
            except StopIteration:
                # We're done
                callback(None, None)
                return
            except Exception, e:
                callback(None, e)
                return

            self.batched_size -= 1

            # TODO: remove so we don't have to use double-underscore hack
            assert self.batched_size == len(self.sync_cursor._Cursor__data)

            should_continue = callback(doc, None)

            # Quit if callback returns exactly False (not None)
            if should_continue is False:
                return

        if self.alive:
            def got_more(batch_size, error):
                if error:
                    callback(None, error)
                elif batch_size:
                    assert self.batched_size == 0
                    self.batched_size = batch_size
                    self.each(callback)
                else:
                    # Complete
                    callback(None, None)

            self._get_more(got_more)
        else:
            # Complete
            callback(None, None)

    def to_list(self, callback):
        """Get a list of documents. The caller is responsible for making sure
        that there is enough memory to store the results. to_list returns
        immediately, and your callback is executed asynchronously with the list
        of documents.

        @param callback: function taking (documents, error)
        """
        check_callable(callback, required=True)
        the_list = []

        def for_each(doc, error):
            if error:
                callback(None, error)

                # stop iteration
                return False
            elif doc is not None:
                assert isinstance(doc, dict)
                the_list.append(doc)
            else:
                # Iteration complete
                callback(the_list, None)

        self.each(for_each)

    @property
    def alive(self):
        """Does this cursor have the potential to return more data?"""
        return self.sync_cursor.alive

    def close(self):
        """Explicitly close this cursor.
        """
        # TODO: either use asynchronize() or explain why this works
        greenlet.greenlet(self.sync_cursor.close).switch()

    def __del__(self):
        if self.alive:
            self.close()


# Replace synchronous methods like 'count' or 'distinct' with async versions
for op in TornadoCursor.async_ops:
    sync_method = getattr(pymongo.cursor.Cursor, op)
    setattr(TornadoCursor, op, asynchronize(sync_method, True, 'sync_cursor'))

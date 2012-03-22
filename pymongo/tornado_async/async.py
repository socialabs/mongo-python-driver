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

"""Tornado asynchronous Python driver for MongoDB."""

import functools
import socket
import time

import tornado.ioloop, tornado.iostream
from tornado import stack_context
import greenlet


import pymongo
from pymongo.errors import InvalidOperation
from pymongo.tornado_async.delegate import *


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
# TODO: verify cursors are closed ASAP
# TODO: document use of TornadoConnection.delegate, TornadoDatabase.delegate, etc.
# TODO: check handling of safe and get_last_error_options() and kwargs in
#   Collection, make sure we respect them
# TODO: What's with this warning when running tests?:
#   "WARNING:root:Connect error on fd 6: [Errno 8] nodename nor servname provided, or not known"

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
    # TODO: move to get_connection() after merging Bernie's pool fix?
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


# TODO: rename 'safe' arg yet again
def asynchronize(method_name, safe, cb_required):
    """
    @param method_name: Name of a method on pymongo Collection, Database,
                        Connection, or Cursor
    @param safe:        Whether the method takes a 'safe' argument
    @param cb_required: If True, raise TypeError if no callback is passed
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
            TornadoConnection, TornadoDatabase, TornadoCollection,
            TornadoCursor
        ))

        callback = kwargs.get('callback')
        check_callable(callback, required=cb_required)

        if 'callback' in kwargs:
            # Don't pass callback to sync_method
            kwargs = kwargs.copy()
            del kwargs['callback']

        # TODO: document that with a callback passed in, Motor's default is
        # to do SAFE writes, unlike PyMongo.
        # ALSO TODO: should Motor's default be safe writes, or no?
        if 'safe' not in kwargs and safe:
            kwargs['safe'] = bool(callback)

        sync_method = getattr(self.delegate, method_name)
        def call_method():
            result, error = None, None
            try:
                result = sync_method(*args, **kwargs)
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

    method.func_name = method_name
    return method


current_request = None
current_request_seq = 0


class Asynchronized(DelegateProperty):
    def __init__(self, safe, cb_required):
        """
        @param safe:        Whether the method takes a 'safe' argument
        @param cb_required: Whether callback is required or optional
        """
        super(Asynchronized, self).__init__(readonly=True)
        self.safe = safe
        self.cb_required = cb_required

    def make_attr(self, cls, name):
        return asynchronize(
            name, safe=self.safe, cb_required=self.cb_required
        )


class TornadoBase(Delegator):
    def __cmp__(self, other):
        if isinstance(other, self.__class__):
            return cmp(self.delegate, other.delegate)
        return NotImplemented

    document_class = DelegateProperty()
    slave_okay = DelegateProperty()
    safe = DelegateProperty()
    get_lasterror_options = DelegateProperty()
    set_lasterror_options = DelegateProperty()
    unset_lasterror_options = DelegateProperty()
    _BaseObject__set_slave_okay = DelegateProperty()
    _BaseObject__set_safe = DelegateProperty()


# TODO: better name, with 'Mongo' or 'Motor' in it!
class TornadoConnection(TornadoBase):
    # list of overridden async operations on a TornadoConnection instance
    # TODO: for all async_ops and callback_ops, auto-gen Sphinx documentation
    # that pulls from PyMongo
    (
        close_cursor, kill_cursors, copy_database, is_locked
    ) = [Asynchronized(safe=False, cb_required=True)] * 4

    # operations for which a callback is required
    # TODO: test all these w/ callbacks?
    (
        database_names, server_info, fsync, unlock
    ) = [Asynchronized(safe=False, cb_required=False)] * 4

    wrapped_attrs = set(['document_class'])

    def __init__(self, *args, **kwargs):
        super(TornadoConnection, self).__init__()
        
        # Store args and kwargs for when open() is called
        self._init_args = args
        self._init_kwargs = kwargs
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

            try:
                self.delegate = pymongo.Connection(
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
        if not self.connected:
            msg = "Can't access database on TornadoConnection before"\
                " calling open()"
            raise InvalidOperation(msg)

        return TornadoDatabase(self, name)

    __getitem__ = __getattr__

    def __repr__(self):
        # TODO: simplify, test
        return 'TornadoConnection(%s)' % (
            ','.join([
            i for i in [
                ','.join([str(i) for i in self._init_args]),
                ','.join(['%s=%s' % (k, v) for k, v in self._init_kwargs.items()]),
                ] if i
            ])
        )

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
        delegate = self.delegate
        delegate.start_request()
        return RequestContext(delegate, request_id)

    # TODO: doc why we need to override this
    def drop_database(self, name_or_database, callback):
        name = name_or_database
        if isinstance(name, TornadoDatabase):
            name = name.delegate.name

        async_method = asynchronize('drop_database', False, True)
        async_method(self, name, callback=callback)

    # HACK!: For unittests that examine this attribute
    _Connection__pool = DelegateProperty()
    close = DelegateProperty()
    disconnect = DelegateProperty()


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


class TornadoReplicaSetConnection(TornadoBase):
    def __init__(self, *args, **kwargs):
        # TODO
        pass


class TornadoDatabase(TornadoBase):
    # list of overridden async operations on a TornadoDatabase instance
    (
        set_profiling_level, reset_error_history, add_user, remove_user,
        authenticate, logout, command
    ) = [Asynchronized(safe=False, cb_required=False)] * 7

    # operations for which a callback is required
    (
        collection_names, current_op, profiling_level, profiling_info,
        error, last_status, previous_error, dereference, eval
    ) = [Asynchronized(safe=False, cb_required=True)] * 9

    def __init__(self, connection, name, *args, **kwargs):
        # *args and **kwargs are not currently supported by pymongo Database,
        # but it doesn't cost us anything to include them and future-proof
        # this method. # TODO: do I need a name or just use delegate.name?
        self.name = name
        assert isinstance(connection, TornadoConnection)
        self.connection = connection
        super(TornadoDatabase, self).__init__(delegate=pymongo.database.Database(
            connection.delegate, name, *args, **kwargs
        ))

    def __getattr__(self, name):
        return TornadoCollection(self, name)

    __getitem__ = __getattr__

    def __repr__(self):
        return 'Tornado' + self.delegate.__repr__()

    # TODO: doc why we need to override this, refactor
    # TODO: test callback-checking in drop_collection
    def drop_collection(self, name_or_collection, callback):
        name = name_or_collection
        if isinstance(name, TornadoCollection):
            name = name.delegate.name

        async_method = asynchronize('drop_collection', False, True)
        async_method(self, name, callback=callback)

    # TODO: doc why we need to override this, refactor
    # TODO: test callback-checking in validate_collection
    def validate_collection(self, name_or_collection, *args, **kwargs):
        callback = kwargs.get('callback')
        check_callable(callback, required=True)

        name = name_or_collection
        if isinstance(name, TornadoCollection):
            name = name.delegate.name

        async_method = asynchronize('validate_collection', False, True)
        async_method(self, name, callback=callback)

    # TODO: test that this raises an error if collection exists in Motor, and
    # test creating capped coll
    def create_collection(self, name, *args, **kwargs):
        # We need to override create_collection specially, rather than simply
        # include it in async_ops, because we have to wrap the Collection it
        # returns in a TornadoCollection.
        callback = kwargs.get('callback')
        check_callable(callback, required=False)
        if 'callback' in kwargs:
            del kwargs['callback']

        async_method = asynchronize('create_collection', False, False)

        def cb(collection, error):
            if isinstance(collection, pymongo.collection.Collection):
                collection = TornadoCollection(self, name)

            callback(collection, error)

        async_method(self, name, *args, callback=cb, **kwargs)


class TornadoCollection(TornadoBase):
    # list of overridden async operations on a TornadoCollection instance
    (
        create_index, drop_indexes, drop_index, drop, ensure_index, reindex,
        rename, find_and_modify 
    ) = [Asynchronized(safe=False, cb_required=False)] * 8
    
    (
        update, insert, remove, save
    ) = [Asynchronized(safe=True, cb_required=False)] * 4
    
    (
        index_information, count, options, group, distinct, inline_map_reduce,
        find_one,
    ) = [Asynchronized(safe=False, cb_required=True)] * 7

    def __init__(self, database, name, *args, **kwargs):
        assert isinstance(database, TornadoDatabase)
        self._tdb = database
        super(TornadoCollection, self).__init__(
            delegate=pymongo.collection.Collection(
                self._tdb.delegate, name
        ))

    def __getattr__(self, name):
        # dotted collection name, like foo.bar
        return TornadoCollection(
            self._tdb,
            self.name + '.' + name
        )

    def find(self, *args, **kwargs):
        """
        Get a TornadoCursor.
        """
        # TODO: better message
        assert 'callback' not in kwargs, (
            "Pass a callback to each() or to_list(), not find()"
        )

        cursor = self.delegate.find(*args, **kwargs)
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

        async_mr = asynchronize('map_reduce', False, True)

        async_mr(self, *args, callback=inner_cb, **kwargs)

    def __repr__(self):
        return 'Tornado' + repr(self.delegate)

    uuid_subtype = DelegateProperty()

    
# TODO: hint(), etc.
class TornadoCursor(TornadoBase):
    (count, distinct) = [Asynchronized(safe=False, cb_required=True)] * 2

    def __init__(self, cursor):
        """
        @param cursor:  Synchronous pymongo Cursor
        """
        super(TornadoCursor, self).__init__(delegate=cursor)
        self.started = False

        # Number of documents buffered in delegate
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
        async_refresh = asynchronize('_refresh', False, True)

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
        assert self.batched_size == len(self.delegate._Cursor__data)

        while self.batched_size > 0:
            try:
                doc = self.delegate.next()
            except StopIteration:
                # We're done
                callback(None, None)
                return
            except Exception, e:
                callback(None, e)
                return

            self.batched_size -= 1

            # TODO: remove so we don't have to use double-underscore hack
            assert self.batched_size == len(self.delegate._Cursor__data)

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
                    tornado.ioloop.IOLoop.instance().add_callback(
                        lambda: callback(None, None)
                    )

            self._get_more(got_more)
        else:
            # Complete
            tornado.ioloop.IOLoop.instance().add_callback(
                lambda: callback(None, None)
            )

    def to_list(self, callback):
        """Get a list of documents. The caller is responsible for making sure
        that there is enough memory to store the results. to_list returns
        immediately, and your callback is executed asynchronously with the list
        of documents.

        @param callback: function taking (documents, error)
        """
        # TODO: error if tailable
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

    # TODO: refactor these methods
    def where(self, code):
        self.delegate.where(code)
        return self

    def sort(self, *args, **kwargs):
        self.delegate.sort(*args, **kwargs)
        return self

    def explain(self, callback):
        async = asynchronize('explain', False, True)
        async(self, callback=callback)

    def close(self):
        """Explicitly close this cursor.
        """
        # TODO: either use asynchronize() or explain why this works
        greenlet.greenlet(self.delegate.close).switch()

    def __del__(self):
        if self.alive:
            self.close()

    alive = DelegateProperty()

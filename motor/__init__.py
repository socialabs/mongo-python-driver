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

"""Motor, an asynchronous driver for MongoDB and Tornado."""

import functools
import socket
import time
import warnings

# So that 'setup.py doc' can import this module without Tornado or greenlet
requirements_satisfied = True
try:
    from tornado import ioloop, iostream, gen, stack_context
except ImportError:
    requirements_satisfied = False
    warnings.warn("Tornado not installed", ImportWarning)

try:
    import greenlet
except ImportError:
    requirements_satisfied = False
    warnings.warn("greenlet module not installed", ImportWarning)


import pymongo
import pymongo.collection
import pymongo.cursor
import pymongo.database
import pymongo.errors
import pymongo.pool
import pymongo.son_manipulator
import gridfs as pymongo_gridfs
from gridfs import grid_file as pymongo_gridfile


__all__ = ['MotorConnection', 'MotorReplicaSetConnection']

# TODO: document that default timeout is None, ensure we're doing
#   timeouts as efficiently as possible, test performance hit with timeouts
#   from registering and cancelling timeouts
# TODO: examine & document what connection and network timeouts mean here
# TODO: document which versions of greenlet and tornado this has been tested
#   against, include those in some file that pip or pypi can understand?
# TODO: is while cursor.alive or while True the right way to iterate with
#   gen.engine and next_object()?
# TODO: document, smugly, that Motor has configurable IOLoops
# TODO: since Tornado uses logging, so can we
# TODO: test cross-host copydb
# TODO: perhaps remove versionchanged Sphinx annotations from proxied methods,
#   unless versionchanged >= 2.3 or so -- whenever Motor joins PyMongo
# TODO: review open_sync(), does it need to disconnect after success to ensure
#   all IOStreams with old IOLoop are gone?


def check_callable(kallable, required=False):
    if required and not kallable:
        raise TypeError("callable is required")
    if kallable is not None and not callable(kallable):
        raise TypeError("callback must be callable")


def motor_sock_method(method):
    """Wrap a MotorSocket method to pause the current greenlet and arrange
       for the greenlet to be resumed when non-blocking I/O has completed.
    """
    @functools.wraps(method)
    def _motor_sock_method(self, *args, **kwargs):
        child_gr = greenlet.getcurrent()
        main = child_gr.parent
        assert main, "Should be on child greenlet"

        timeout_object = None

        if self.timeout:
            def timeout_err():
                # Running on the main greenlet. If a timeout error is thrown,
                # we raise the exception on the child greenlet. Closing the
                # IOStream removes callback() from the IOLoop so it isn't
                # called.
                self.stream.set_close_callback(None)
                self.stream.close()
                child_gr.throw(socket.timeout("timed out"))

            timeout_object = self.stream.io_loop.add_timeout(
                time.time() + self.timeout, timeout_err)

        # This is run by IOLoop on the main greenlet when operation
        # completes; switch back to child to continue processing
        def callback(result=None):
            self.stream.set_close_callback(None)
            if timeout_object:
                self.stream.io_loop.remove_timeout(timeout_object)

            child_gr.switch(result)

        # Run on main greenlet
        def closed():
            if timeout_object:
                self.stream.io_loop.remove_timeout(timeout_object)

            # The child greenlet might have died, e.g.:
            # - An operation raised an error within PyMongo
            # - PyMongo closed the MotorSocket in response
            # - MotorSocket.close() closed the IOStream
            # - IOStream scheduled this closed() function on the loop
            # - PyMongo operation completed (with or without error) and
            #       its greenlet terminated
            # - IOLoop runs this function
            if not child_gr.dead:
                # IOStream.error is a Tornado 2.3 feature
                error = getattr(self.stream, 'error', None)
                child_gr.throw(error or socket.error("error"))

        self.stream.set_close_callback(closed)

        try:
            kwargs['callback'] = callback

            # method is MotorSocket.send(), recv(), etc. method() begins a
            # non-blocking operation on an IOStream and arranges for
            # callback() to be executed on the main greenlet once the
            # operation has completed.
            method(self, *args, **kwargs)

            # Pause child greenlet until resumed by main greenlet, which
            # will pass the result of the socket operation (data for recv,
            # number of bytes written for sendall) to us.
            socket_result = main.switch()
            return socket_result
        except socket.error:
            raise
        except IOError, e:
            # If IOStream raises generic IOError (e.g., if operation
            # attempted on closed IOStream), then substitute socket.error,
            # since socket.error is what PyMongo's built to handle. For
            # example, PyMongo will catch socket.error, close the socket,
            # and raise AutoReconnect.
            raise socket.error(str(e))

    return _motor_sock_method


class MotorSocket(object):
    """Replace socket with a class that yields from the current greenlet, if
    we're on a child greenlet, when making blocking calls, and uses Tornado
    IOLoop to schedule child greenlet for resumption when I/O is ready.

    We only implement those socket methods actually used by pymongo.
    """
    def __init__(self, sock, io_loop, use_ssl=False):
        self.use_ssl = use_ssl
        self.timeout = None
        if self.use_ssl:
           self.stream = iostream.SSLIOStream(sock, io_loop=io_loop)
        else:
           self.stream = iostream.IOStream(sock, io_loop=io_loop)

    def setsockopt(self, *args, **kwargs):
        self.stream.socket.setsockopt(*args, **kwargs)

    def settimeout(self, timeout):
        # IOStream calls socket.setblocking(False), which does settimeout(0.0).
        # We must not allow pymongo to set timeout to some other value (a
        # positive number or None) or the socket will start blocking again.
        # Instead, we simulate timeouts by interrupting ourselves with
        # callbacks.
        self.timeout = timeout

    @motor_sock_method
    def connect(self, pair, callback):
        """
        :Parameters:
         - `pair`: A tuple, (host, port)
        """
        self.stream.connect(pair, callback)

    @motor_sock_method
    def sendall(self, data, callback):
        self.stream.write(data, callback)

    @motor_sock_method
    def recv(self, num_bytes, callback):
        self.stream.read_bytes(num_bytes, callback)

    def close(self):
        # TODO: examine this, decide if it's correct, and if so explain why
        self.stream.set_close_callback(None)
        
        sock = self.stream.socket
        try:
            try:
                self.stream.close()
            except KeyError:
                # Tornado's _impl (epoll, kqueue, ...) has already removed this
                # file descriptor from its dict.
                pass
        finally:
            # Sometimes necessary to avoid ResourceWarnings in Python 3:
            # specifically, if the fd is closed from the OS's view, then
            # stream.close() throws an exception, but the socket still has an
            # fd and so will print a ResourceWarning. In that case, calling
            # sock.close() directly clears the fd and does not raise an error.
            if sock:
                sock.close()

    def fileno(self):
        return self.stream.socket.fileno()


class MotorPool(pymongo.pool.GreenletPool):
    """A simple connection pool of MotorSockets.

    Note this inherits from GreenletPool so that when PyMongo internally calls
    start_request, e.g. in Database.authenticate() or
    Connection.copy_database(), this pool assigns a socket to the current
    greenlet for the duration of the method. Request semantics are not exposed
    to Motor's users.
    """
    def __init__(self, io_loop, *args, **kwargs):
        self.io_loop = io_loop
        pymongo.pool.GreenletPool.__init__(self, *args, **kwargs)

    def create_connection(self, pair):
        """Copy of BasePool.connect()
        """
        # TODO: refactor all this with BasePool, use a new hook to wrap the
        #   socket with MotorSocket before attempting connect().
        assert greenlet.getcurrent().parent, "Should be on child greenlet"

        host, port = pair or self.pair

        # Don't try IPv6 if we don't support it. Also skip it if host
        # is 'localhost' (::1 is fine). Avoids slow connect issues
        # like PYTHON-356.
        family = socket.AF_INET
        if socket.has_ipv6 and host != 'localhost':
            family = socket.AF_UNSPEC

        err = None
        for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
            af, socktype, proto, dummy, sa = res
            motor_sock = None
            try:
                sock = socket.socket(af, socktype, proto)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                motor_sock = MotorSocket(
                    sock, self.io_loop, use_ssl=self.use_ssl)
                motor_sock.settimeout(self.conn_timeout or 20.0)

                # MotorSocket will pause the current greenlet and resume it
                # when connection has completed
                motor_sock.connect(pair or self.pair)
                return motor_sock
            except socket.error, e:
                err = e
                if motor_sock is not None:
                    motor_sock.close()

        if err is not None:
            raise err
        else:
            # This likely means we tried to connect to an IPv6 only
            # host with an OS/kernel or Python interpeter that doesn't
            # support IPv6.
            raise socket.error('getaddrinfo failed')

    def connect(self, pair):
        """Copy of BasePool.connect(), avoiding call to ssl.wrap_socket which
           is inappropriate for Motor.
           TODO: refactor, extra hooks in BasePool
        """
        motor_sock = self.create_connection(pair)
        motor_sock.settimeout(self.net_timeout)
        return pymongo.pool.SocketInfo(motor_sock, self.pool_id)


def asynchronize(io_loop, sync_method, has_safe_arg, callback_required):
    """
    :Parameters:
     - `io_loop`:           A Tornado IOLoop
     - `sync_method`:       Bound method of pymongo Collection, Database,
                            Connection, or Cursor
     - `has_safe_arg`:      Whether the method takes a 'safe' argument
     - `callback_required`: If True, raise TypeError if no callback is passed
    """
    assert isinstance(io_loop, ioloop.IOLoop), (
        "First argument to asynchronize must be IOLoop, not %s" % repr(io_loop))

    # TODO doc
    @functools.wraps(sync_method)
    def method(*args, **kwargs):
        callback = kwargs.get('callback')
        check_callable(callback, required=callback_required)

        # Don't pass callback to sync_method
        kwargs.pop('callback', None)

        # Safe writes if callback is passed and safe=False not passed explicitly
        if 'safe' not in kwargs and has_safe_arg:
            kwargs['safe'] = bool(callback)

        def call_method():
            result, error = None, None
            try:
                result = sync_method(*args, **kwargs)
            except Exception, e:
                error = e

            # Schedule the callback to be run on the main greenlet
            if callback:
                io_loop.add_callback(
                    functools.partial(callback, result, error)
                )
            elif error:
                raise error

        # Start running the operation on a greenlet
        greenlet.greenlet(call_method).switch()

    return method


class DelegateProperty(object):
    def __init__(self):
        self.name = None


class Async(DelegateProperty):
    def __init__(self, has_safe_arg, cb_required):
        """
        A descriptor that wraps a PyMongo method, such as insert or remove, and
        returns an asynchronous version of the method, which takes a callback.

        :Parameters:
         - `has_safe_arg`:    Whether the method takes a 'safe' argument
         - `cb_required`:     Whether callback is required or optional
        """
        DelegateProperty.__init__(self)
        self.has_safe_arg = has_safe_arg
        self.cb_required = cb_required

    def __get__(self, obj, objtype):
        # self.name is set by MotorMeta
        sync_method = getattr(obj.delegate, self.name)
        return asynchronize(
            obj.get_io_loop(),
            sync_method,
            has_safe_arg=self.has_safe_arg,
            callback_required=self.cb_required
        )


class ReadOnlyDelegateProperty(DelegateProperty):
    def __get__(self, obj, objtype):
        # self.name is set by MotorMeta
        return getattr(obj.delegate, self.name)


class ReadWriteDelegateProperty(ReadOnlyDelegateProperty):
    def __set__(self, obj, val):
        # self.name is set by MotorMeta
        setattr(obj.delegate, self.name, val)


class MotorMeta(type):
    def __new__(cls, name, bases, attrs):
        # Create the class.
        new_class = type.__new__(cls, name, bases, attrs)

        # Set DelegateProperties' names
        for name, attr in attrs.items():
            if isinstance(attr, DelegateProperty):
                attr.name = name

        return new_class


class MotorBase(object):
    __metaclass__ = MotorMeta

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.delegate == other.delegate
        return NotImplemented

    get_lasterror_options   = ReadOnlyDelegateProperty()
    set_lasterror_options   = ReadOnlyDelegateProperty()
    unset_lasterror_options = ReadOnlyDelegateProperty()
    name                    = ReadOnlyDelegateProperty()
    document_class          = ReadWriteDelegateProperty()
    slave_okay              = ReadWriteDelegateProperty()
    safe                    = ReadWriteDelegateProperty()
    read_preference         = ReadWriteDelegateProperty()
    tag_sets                = ReadWriteDelegateProperty()
    secondary_acceptable_latency_ms = ReadWriteDelegateProperty()

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, repr(self.delegate))


class MotorConnectionBase(MotorBase):
    """MotorConnection and MotorReplicaSetConnection common functionality.
    """
    database_names = Async(has_safe_arg=False, cb_required=True)
    close_cursor   = Async(has_safe_arg=False, cb_required=False)
    server_info    = Async(has_safe_arg=False, cb_required=False)
    copy_database  = Async(has_safe_arg=False, cb_required=False)
    disconnect     = ReadOnlyDelegateProperty()
    tz_aware       = ReadOnlyDelegateProperty()
    close          = ReadOnlyDelegateProperty()
    is_primary     = ReadOnlyDelegateProperty()
    is_mongos      = ReadOnlyDelegateProperty()
    max_bson_size  = ReadOnlyDelegateProperty()
    max_pool_size  = ReadOnlyDelegateProperty()

    def __init__(self, *args, **kwargs):
        io_loop = kwargs.pop('io_loop', None)
        if io_loop:
            if not isinstance(io_loop, ioloop.IOLoop):
                raise TypeError(
                    "io_loop must be instance of IOLoop, not %s" % (
                        repr(io_loop)))
            self.io_loop = io_loop
        else:
            self.io_loop = ioloop.IOLoop.instance()

        # Store args and kwargs for when open() is called
        self._init_args = args
        self._init_kwargs = kwargs
        self.delegate = None

    def get_io_loop(self):
        return self.io_loop

    def open(self, callback):
        """
        Actually connect, passing self to a callback when connected.

        :Parameters:
         - `callback`: Optional function taking parameters (connection, error)
        """
        self._open(callback)

    def _open(self, callback):
        check_callable(callback)

        if self.connected:
            if callback:
                self.io_loop.add_callback(
                    functools.partial(callback, self, None))
            return

        def _connect():
            # Run on child greenlet
            error = None
            try:
                args, kwargs = self._delegate_init_args()
                self.delegate = self.__delegate_class__(*args, **kwargs)
            except Exception, e:
                error = e

            if callback:
                # Schedule callback to be executed on main greenlet, with
                # (self, None) if no error, else (None, error)
                self.io_loop.add_callback(
                    functools.partial(
                        callback, None if error else self, error))

        # Actually connect on a child greenlet
        gr = greenlet.greenlet(_connect)
        gr.switch()

    def open_sync(self):
        """Synchronous open(), returning self.

        Under the hood, this method creates a new Tornado IOLoop, runs
        :meth:`open` on the loop, and deletes the loop when :meth:`open`
        completes.
        """
        if self.connected:
            return self

        # Run a private IOLoop until connected or error
        private_loop = ioloop.IOLoop()
        standard_loop, self.io_loop = self.io_loop, private_loop
        try:
            outcome = {}
            def callback(connection, error):
                outcome['error'] = error
                self.io_loop.stop()

            self._open(callback)

            # Returns once callback has been executed and loop stopped.
            self.io_loop.start()
        finally:
            # Replace the private IOLoop with the default loop
            self.io_loop = standard_loop
            if self.delegate:
                self.delegate.pool_class = functools.partial(
                    MotorPool, self.io_loop)

                for pool in self._get_pools():
                    pool.io_loop = self.io_loop
                    pool.reset()

            # Clean up file descriptors.
            private_loop.close()

        if outcome['error']:
            raise outcome['error']

        return self

    def sync_connection(self):
        return self.__delegate_class__(
            *self._init_args, **self._init_kwargs)

    def _delegate_init_args(self):
        """Return args, kwargs to create a delegate object"""
        kwargs = self._init_kwargs.copy()
        kwargs['auto_start_request'] = False
        kwargs['_pool_class'] = functools.partial(MotorPool, self.io_loop)
        return self._init_args, kwargs
    
    def __getattr__(self, name):
        if not self.connected:
            msg = ("Can't access attribute '%s' on %s before calling open()"
                  " or open_sync()" % (
                name, self.__class__.__name__))
            raise pymongo.errors.InvalidOperation(msg)

        return MotorDatabase(self, name)

    __getitem__ = __getattr__

    def drop_database(self, name_or_database, callback):
        """Drop a database.

        Raises :class:`TypeError` if `name_or_database` is not an instance of
        :class:`basestring` (:class:`str` in python 3),
        :class:`~pymongo.database.Database`,
        or :class:`MotorDatabase`.

        :Parameters:
          - `name_or_database`: the name of a database to drop, or a
            :class:`~pymongo.database.Database` instance representing the
            database to drop
          - `callback`: Optional function taking parameters (connection, error)
        """
        if isinstance(name_or_database, MotorDatabase):
            name_or_database = name_or_database.delegate.name

        async_method = asynchronize(
            self.get_io_loop(), self.delegate.drop_database, False, True)
        async_method(name_or_database, callback=callback)

    def start_request(self):
        raise NotImplementedError("Motor doesn't implement requests")

    in_request = end_request = start_request

    @property
    def connected(self):
        """True after :meth:`open` or :meth:`open_sync` completes"""
        return self.delegate is not None


class MotorConnection(MotorConnectionBase):
    __delegate_class__ = pymongo.connection.Connection

    kill_cursors = Async(has_safe_arg=False, cb_required=True)
    fsync        = Async(has_safe_arg=False, cb_required=False)
    unlock       = Async(has_safe_arg=False, cb_required=False)
    nodes        = ReadOnlyDelegateProperty()
    host         = ReadOnlyDelegateProperty()
    port         = ReadOnlyDelegateProperty()

    def __init__(self, *args, **kwargs):
        """Create a new connection to a single MongoDB instance at *host:port*.

        :meth:`open` or :meth:`open_sync` must be called before using a new
        MotorConnection. No property access is allowed before the connection
        is opened.

        MotorConnection takes the same constructor arguments as
        :class:`~pymongo.connection.Connection`, as well as:

        :Parameters:
          - `io_loop` (optional): Special :class:`tornado.ioloop.IOLoop`
            instance to use instead of default
        """
        super(MotorConnection, self).__init__(*args, **kwargs)

    # TODO: test directly, document what 'result' means to the callback
    def is_locked(self, callback):
        """Is this server locked? While locked, all write operations
        are blocked, although read operations may still be allowed.
        Use :meth:`unlock` to unlock.

        :Parameters:
         - `callback`:    function taking parameters (result, error)

        .. note:: PyMongo's :attr:`~pymongo.connection.Connection.is_locked` is
           a property that synchronously executes the `currentOp` command on the
           server before returning. In Motor, `is_locked` must take a callback
           and executes asynchronously.
        """
        def is_locked(result, error):
            if error:
                callback(None, error)
            else:
                callback(result.get('fsyncLock', 0), None)

        self.admin.current_op(callback=is_locked)

    def _get_pools(self):
        # TODO: expose the PyMongo pool, or otherwise avoid this
        return [self.delegate._Connection__pool]


class MotorReplicaSetConnection(MotorConnectionBase):
    __delegate_class__ = pymongo.replica_set_connection.ReplicaSetConnection

    primary     = ReadOnlyDelegateProperty()
    secondaries = ReadOnlyDelegateProperty()
    arbiters    = ReadOnlyDelegateProperty()
    hosts       = ReadOnlyDelegateProperty()
    seeds       = ReadOnlyDelegateProperty()
    close       = ReadOnlyDelegateProperty()

    def __init__(self, *args, **kwargs):
        """Create a new connection to a MongoDB replica set.

        :meth:`open` or :meth:`open_sync` must be called before using a new
        MotorReplicaSetConnection. No property access is allowed before the
        connection is opened.

        MotorReplicaSetConnection takes the same constructor arguments as
        :class:`~pymongo.replica_set_connection.ReplicaSetConnection`,
        as well as:

        :Parameters:
          - `io_loop` (optional): Special :class:`tornado.ioloop.IOLoop`
            instance to use instead of default
        """
        # We only override __init__ to replace its docstring
        super(MotorReplicaSetConnection, self).__init__(*args, **kwargs)

    def open_sync(self):
        """Synchronous open(), returning self.

        Under the hood, this method creates a new Tornado IOLoop, runs
        :meth:`open` on the loop, and deletes the loop when :meth:`open`
        completes.
        """
        # TODO: revisit and refactor open, open_sync(), custom-loop handling,
        #   and the monitor
        super(MotorReplicaSetConnection, self).open_sync()

        # We need to wait for open_sync() to complete and restore the
        # original IOLoop before starting the monitor. This is a hack.
        self.delegate._ReplicaSetConnection__monitor.start_motor(self.io_loop)
        return self

    def open(self, callback):
        check_callable(callback)
        def opened(result, error):
            if error:
                callback(None, error)
            else:
                try:
                    monitor = self.delegate._ReplicaSetConnection__monitor
                    monitor.start_motor(self.io_loop)
                except Exception, e:
                    callback(None, e)
                else:
                    # No errors
                    callback(self, None)

        super(MotorReplicaSetConnection, self)._open(callback=opened)

    def _delegate_init_args(self):
        # This _monitor_class will be passed to PyMongo's
        # ReplicaSetConnection when we create it.
        args, kwargs = super(
            MotorReplicaSetConnection, self)._delegate_init_args()
        kwargs['_monitor_class'] = MotorReplicaSetMonitor
        return args, kwargs

    def _get_pools(self):
        # TODO: expose the PyMongo RSC members, or otherwise avoid this
        return [
            member.pool for member in
            self.delegate._ReplicaSetConnection__members.values()]


# PyMongo uses a background thread to regularly inspect the replica set and
# monitor it for changes. In Motor, use a periodic callback on the IOLoop to
# monitor the set.
class MotorReplicaSetMonitor(pymongo.replica_set_connection.Monitor):
    def __init__(self, rsc):
        assert isinstance(
            rsc, pymongo.replica_set_connection.ReplicaSetConnection), (
            "First argument to MotorReplicaSetMonitor must be"
            " ReplicaSetConnection, not %s" % repr(rsc))

        # Fake the event_class: we won't use it
        pymongo.replica_set_connection.Monitor.__init__(
            self, rsc, event_class=object)

        self.timeout_obj = None

    def shutdown(self, dummy):
        if self.timeout_obj:
            self.io_loop.remove_timeout(self.timeout_obj)

    def refresh(self):
        assert greenlet.getcurrent().parent, "Should be on child greenlet"
        try:
            self.rsc.refresh()
        except pymongo.errors.AutoReconnect:
            pass
        # RSC has been collected or there
        # was an unexpected error.
        except:
            return

        self.timeout_obj = self.io_loop.add_timeout(
            time.time() + self._refresh_interval, self.async_refresh)

    def start(self):
        """No-op: PyMongo thinks this starts the monitor, but Motor starts
           the monitor separately to ensure it uses the right IOLoop"""
        pass

    def start_motor(self, io_loop):
        self.io_loop = io_loop
        self.async_refresh = asynchronize(
            self.io_loop,
            self.refresh,
            has_safe_arg=False,
            callback_required=False)
        self.timeout_obj = self.io_loop.add_timeout(
            time.time() + self._refresh_interval, self.async_refresh)

    def schedule_refresh(self):
        if self.io_loop and self.async_refresh:
            if self.timeout_obj:
                self.io_loop.remove_timeout(self.timeout_obj)

            self.io_loop.add_callback(self.async_refresh)

    def join(self, timeout):
        # PyMongo calls join() after shutdown() -- this is not a thread, so
        # shutdown works immediately and join is unnecessary
        pass


class MotorDatabase(MotorBase):
    __delegate_class__ = pymongo.database.Database

    # list of overridden async operations on a MotorDatabase instance
    set_profiling_level = Async(has_safe_arg=False, cb_required=False)
    reset_error_history = Async(has_safe_arg=False, cb_required=False)
    add_user            = Async(has_safe_arg=False, cb_required=False)
    remove_user         = Async(has_safe_arg=False, cb_required=False)
    logout              = Async(has_safe_arg=False, cb_required=False)
    command             = Async(has_safe_arg=False, cb_required=False)
    authenticate        = Async(has_safe_arg=False, cb_required=True)
    collection_names    = Async(has_safe_arg=False, cb_required=True)
    current_op          = Async(has_safe_arg=False, cb_required=True)
    profiling_level     = Async(has_safe_arg=False, cb_required=True)
    profiling_info      = Async(has_safe_arg=False, cb_required=True)
    error               = Async(has_safe_arg=False, cb_required=True)
    last_status         = Async(has_safe_arg=False, cb_required=True)
    previous_error      = Async(has_safe_arg=False, cb_required=True)
    dereference         = Async(has_safe_arg=False, cb_required=True)
    eval                = Async(has_safe_arg=False, cb_required=True)

    incoming_manipulators         = ReadOnlyDelegateProperty()
    incoming_copying_manipulators = ReadOnlyDelegateProperty()
    outgoing_manipulators         = ReadOnlyDelegateProperty()
    outgoing_copying_manipulators = ReadOnlyDelegateProperty()

    def __init__(self, connection, name, *args, **kwargs):
        # *args and **kwargs are not currently supported by pymongo Database,
        # but it doesn't cost us anything to include them and future-proof
        # this method.
        if not isinstance(connection, MotorConnectionBase):
            raise TypeError("First argument to MotorDatabase must be "
                            "MotorConnectionBase, not %s" % repr(connection))

        self.connection = connection
        self.delegate = pymongo.database.Database(
            connection.delegate, name, *args, **kwargs
        )

    def __getattr__(self, name):
        return MotorCollection(self, name)

    __getitem__ = __getattr__

    # TODO: refactor
    def drop_collection(self, name_or_collection, callback):
        """Drop a collection.

        :Parameters:
          - `name_or_collection`: the name of a collection to drop or the
            collection object itself
          - `callback`: Optional function taking parameters (result, error)
        """

        name = name_or_collection
        if isinstance(name, MotorCollection):
            name = name.delegate.name

        sync_method = self.delegate.drop_collection
        async_method = asynchronize(
            self.get_io_loop(), sync_method, False, False)
        async_method(name, callback=callback)

    # TODO: refactor, test
    def validate_collection(self, name_or_collection, *args, **kwargs):
        """Validate a collection.

        Takes same arguments as
        :meth:`~pymongo.database.Database.validate_collection`, plus:

        :Parameters:
          - `callback`: Optional function taking parameters (result, error)
        """
        name = name_or_collection
        if isinstance(name, MotorCollection):
            name = name.delegate.name

        sync_method = self.delegate.validate_collection
        async_method = asynchronize(
            self.get_io_loop(), sync_method, False, True)
        async_method(name, **kwargs)

    # TODO: refactor
    # TODO: test that this raises an error if collection exists in Motor, and
    # test creating capped coll
    def create_collection(self, name, *args, **kwargs):
        """Create a new collection in this database. Takes same arguments as
        :meth:`~pymongo.database.Database.create_collection`, plus callback,
        which receives a MotorCollection.

        :Parameters:
          - `callback`: Optional function taking parameters (collection, error)
        """
        # We override create_collection to wrap the Collection it returns in a
        # MotorCollection.
        callback = kwargs.pop('callback', None)
        check_callable(callback)

        def create_collection_callback(collection, error):
            if isinstance(collection, pymongo.collection.Collection):
                collection = MotorCollection(self, name)

            callback(collection, error)

        kwargs['callback'] = create_collection_callback
        sync_method = self.delegate.create_collection
        async_method = asynchronize(
            self.get_io_loop(), sync_method, False, False)

        async_method(name, *args, **kwargs)

    def add_son_manipulator(self, manipulator):
        """Add a new son manipulator to this database.

        Newly added manipulators will be applied before existing ones.

        :Parameters:
          - `manipulator`: the manipulator to add
        """
        # We override add_son_manipulator to unwrap the AutoReference's
        # database attribute.
        if isinstance(manipulator, pymongo.son_manipulator.AutoReference):
            db = manipulator.database
            if isinstance(db, MotorDatabase):
                manipulator.database = db.delegate

        self.delegate.add_son_manipulator(manipulator)

    def get_io_loop(self):
        return self.connection.get_io_loop()


class MotorCollection(MotorBase):
    __delegate_class__ = pymongo.collection.Collection

    create_index      = Async(has_safe_arg=False, cb_required=False)
    drop_indexes      = Async(has_safe_arg=False, cb_required=False)
    drop_index        = Async(has_safe_arg=False, cb_required=False)
    drop              = Async(has_safe_arg=False, cb_required=False)
    ensure_index      = Async(has_safe_arg=False, cb_required=False)
    reindex           = Async(has_safe_arg=False, cb_required=False)
    rename            = Async(has_safe_arg=False, cb_required=False)
    find_and_modify   = Async(has_safe_arg=False, cb_required=False)
    update            = Async(has_safe_arg=True, cb_required=False)
    insert            = Async(has_safe_arg=True, cb_required=False)
    remove            = Async(has_safe_arg=True, cb_required=False)
    save              = Async(has_safe_arg=True, cb_required=False)
    index_information = Async(has_safe_arg=False, cb_required=True)
    count             = Async(has_safe_arg=False, cb_required=True)
    options           = Async(has_safe_arg=False, cb_required=True)
    group             = Async(has_safe_arg=False, cb_required=True)
    distinct          = Async(has_safe_arg=False, cb_required=True)
    inline_map_reduce = Async(has_safe_arg=False, cb_required=True)
    find_one          = Async(has_safe_arg=False, cb_required=True)
    aggregate         = Async(has_safe_arg=False, cb_required=True)
    uuid_subtype      = ReadWriteDelegateProperty()
    full_name         = ReadOnlyDelegateProperty()

    def __init__(self, database, name, *args, **kwargs):
        if not isinstance(database, MotorDatabase):
            raise TypeError("First argument to MotorCollection must be "
                            "MotorDatabase, not %s" % repr(database))

        self.database = database
        self.delegate = pymongo.collection.Collection(
            self.database.delegate, name)

    def __getattr__(self, name):
        # dotted collection name, like foo.bar
        return MotorCollection(
            self.database,
            self.name + '.' + name
        )

    def find(self, *args, **kwargs):
        """Create a :class:`MotorCursor`. Same parameters as for
        :meth:`~pymongo.collection.Collection.find`.

        Note that :meth:`find` does not take a `callback` parameter -- pass
        a callback to the :class:`MotorCursor`'s methods such as
        :meth:`MotorCursor.find`.
        """
        if 'callback' in kwargs:
            raise pymongo.errors.InvalidOperation(
                "Pass a callback to next_object, each, to_list, count, or tail, not"
                " to find"
            )

        cursor = self.delegate.find(*args, **kwargs)
        return MotorCursor(cursor, self)

    def map_reduce(self, *args, **kwargs):
        """Perform a map/reduce operation on this collection.

        If `full_response` is ``False`` (default) passes a
        :class:`MotorCollection` instance containing
        the results of the operation to ``callback``.
        Otherwise, returns the full
        response from the server to the `map reduce command`_.

        Takes same arguments as
        :meth:`~pymongo.collection.Collection.map_reduce`,
        as well as:

        :Parameters:
         - `callback`: function taking parameters (result, error)

        .. _map reduce command: http://www.mongodb.org/display/DOCS/MapReduce
        """
        callback = kwargs.pop('callback', None)

        def map_reduce_callback(result, error):
            if isinstance(result, pymongo.collection.Collection):
                result = self.database[result.name]
            callback(result, error)

        kwargs['callback'] = map_reduce_callback
        sync_method = self.delegate.map_reduce
        async_mr = asynchronize(self.get_io_loop(), sync_method, False, True)
        async_mr(*args, **kwargs)

    def get_io_loop(self):
        return self.database.get_io_loop()


class MotorCursorChainingMethod(DelegateProperty):
    def __get__(self, obj, objtype):
        # self.name is set by MotorMeta
        method = getattr(obj.delegate, self.name)

        @functools.wraps(method)
        def return_clone(*args, **kwargs):
            method(*args, **kwargs)
            return obj

        return return_clone


class MotorCursor(MotorBase):
    __delegate_class__ = pymongo.cursor.Cursor
    # TODO: test all these in test_motor_cursor.py
    count         = Async(has_safe_arg=False, cb_required=True)
    distinct      = Async(has_safe_arg=False, cb_required=True)
    explain       = Async(has_safe_arg=False, cb_required=True)
    _refresh      = Async(has_safe_arg=False, cb_required=True)
    close         = Async(has_safe_arg=False, cb_required=False)
    alive         = ReadOnlyDelegateProperty()
    cursor_id     = ReadOnlyDelegateProperty()
    batch_size    = MotorCursorChainingMethod()
    add_option    = MotorCursorChainingMethod()
    remove_option = MotorCursorChainingMethod()
    limit         = MotorCursorChainingMethod()
    skip          = MotorCursorChainingMethod()
    max_scan      = MotorCursorChainingMethod()
    sort          = MotorCursorChainingMethod()
    hint          = MotorCursorChainingMethod()
    where         = MotorCursorChainingMethod()

    def __init__(self, cursor, collection):
        """You will not usually construct a MotorCursor yourself, but acquire
        one from :meth:`MotorCollection.find`.

        :Parameters:
         - `cursor`:      PyMongo :class:`~pymongo.cursor.Cursor`
         - `collection`:  :class:`MotorCollection`
        """
        if not isinstance(cursor, pymongo.cursor.Cursor):
            raise TypeError(
                "cursor must be instance of pymongo.cursor.Cursor, not %s" % (
                repr(cursor)))

        if not isinstance(collection, MotorCollection):
            raise TypeError(
                "collection must be instance of MotorCollection, not %s" % (
                repr(collection)))

        self.delegate = cursor
        self.collection = collection
        self.started = False

    def _get_more(self, callback):
        """
        Get a batch of data asynchronously, either performing an initial query
        or getting more data from an existing cursor.
        :Parameters:
         - `callback`:    function taking parameters (batch_size, error)
        """
        if self.started and not self.alive:
            raise pymongo.errors.InvalidOperation(
                "Can't call get_more() on a MotorCursor that has been"
                " exhausted or killed."
            )

        self.started = True
        self._refresh(callback=callback)

    def next_object(self, callback):
        """Asynchronously retrieve the next document in the result set,
        fetching a batch of results from the server if necessary.

        .. testsetup::

          import sys

          from pymongo.connection import Connection
          Connection().test.test_collection.remove()

          from motor import MotorConnection
          from tornado.ioloop import IOLoop
          connection = MotorConnection().open_sync()
          collection = connection.test.test_collection

        .. doctest::

          >>> cursor = None
          >>> def inserted(result, error):
          ...     global cursor
          ...     if error:
          ...         raise error
          ...     cursor = collection.find().sort([('_id', 1)])
          ...     cursor.next_object(callback=on_next)
          ...
          >>> def on_next(result, error):
          ...     if error:
          ...         raise error
          ...     elif result:
          ...         sys.stdout.write(str(result['_id']) + ', ')
          ...         cursor.next_object(callback=on_next)
          ...     else:
          ...         # Iteration complete
          ...         IOLoop.instance().stop()
          ...         print 'done'
          ...
          >>> collection.insert(
          ...     [{'_id': i} for i in range(5)], callback=inserted)
          >>> IOLoop.instance().start()
          0, 1, 2, 3, 4, done

        In the example above there is no need to call :meth:`close`,
        because the cursor is iterated to completion. If you cancel iteration
        before exhausting the cursor, call :meth:`close` to immediately free
        server resources. Otherwise, the garbage-collector will eventually
        close the cursor when deleting it.

        :Parameters:
         - `callback`: function taking (document, error)
        """
        # TODO: prevent concurrent uses of this cursor, as IOStream does, and
        #   document that and how to avoid it.
        check_callable(callback, required=True)
        add_callback = self.get_io_loop().add_callback

        # TODO: simplify, review, comment
        if self.buffer_size > 0:
            try:
                doc = self.delegate.next()
            except StopIteration:
                # Special case: limit is 0.
                doc = None
                self.close()

            # Schedule callback on IOLoop. Calling this callback directly
            # seems to lead to circular references to MotorCursor.
            add_callback(functools.partial(callback, doc, None))
        elif self.alive and (self.cursor_id or not self.started):
            def got_more(batch_size, error):
                if error:
                    callback(None, error)
                else:
                    self.next_object(callback)

            self._get_more(got_more)
        else:
            # Complete
            add_callback(functools.partial(callback, None, None))

    def each(self, callback):
        """Iterates over all the documents for this cursor.

        `each` returns immediately, and `callback` is executed asynchronously
        for each document. `callback` is passed ``(None, None)`` when iteration
        is complete.

        Return ``False`` from the callback to stop iteration. If you stop
        iteration you should call :meth:`close` to immediately free server
        resources for the cursor. It is unnecessary to close a cursor after
        iterating it completely.

        .. testsetup::

          import sys

          from pymongo.connection import Connection
          Connection().test.test_collection.remove()

          from motor import MotorConnection
          from tornado.ioloop import IOLoop
          connection = MotorConnection().open_sync()
          collection = connection.test.test_collection

        .. doctest::

          >>> cursor = None
          >>> def inserted(result, error):
          ...     global cursor
          ...     if error:
          ...         raise error
          ...     cursor = collection.find().sort([('_id', 1)])
          ...     cursor.each(callback=each)
          ...
          >>> def each(result, error):
          ...     if error:
          ...         raise error
          ...     elif result:
          ...         sys.stdout.write(str(result['_id']) + ', ')
          ...     else:
          ...         # Iteration complete
          ...         IOLoop.instance().stop()
          ...         print 'done'
          ...
          >>> collection.insert(
          ...     [{'_id': i} for i in range(5)], callback=inserted)
          >>> IOLoop.instance().start()
          0, 1, 2, 3, 4, done

        :Parameters:
         - `callback`: function taking (document, error)
        """
        check_callable(callback, required=True)
        self._each_got_more(callback, None, None)

    def _each_got_more(self, callback, batch_size, error):
        add_callback = self.get_io_loop().add_callback
        if error:
            callback(None, error)
            return

        while self.buffer_size > 0:
            try:
                doc = self.delegate.next() # decrements self.buffer_size
            except StopIteration:
                # Special case: limit of 0
                add_callback(functools.partial(callback, None, None))
                self.close()
                return

            # Quit if callback returns exactly False (not None). Note we
            # don't close the cursor: user may want to resume iteration.
            if callback(doc, None) is False:
                return

        if self.alive and (self.cursor_id or not self.started):
            self._get_more(functools.partial(self._each_got_more, callback))
        else:
            # Complete
            add_callback(functools.partial(callback, None, None))


    def to_list(self, callback):
        """Get a list of documents.

        The caller is responsible for making sure that there is enough memory
        to store the results -- it is strongly recommended you use a limit like:

        >>> collection.find().limit(some_number).to_list(callback)

        `to_list` returns immediately, and `callback` is executed
        asynchronously with the list of documents.

        :Parameters:
         - `callback`: function taking (documents, error)
        """
        if self.delegate._Cursor__tailable:
            raise pymongo.errors.InvalidOperation(
                "Can't call to_list on tailable cursor")

        check_callable(callback, required=True)
        the_list = []

        # Special case
        if self.delegate._Cursor__empty:
            callback([], None)
            return

        self._to_list_got_more(callback, the_list, None, None)

    def _to_list_got_more(self, callback, the_list, batch_size, error):
        if error:
            callback(None, error)
            return

        if self.buffer_size > 0:
            the_list.extend(self.delegate._Cursor__data)
            self.delegate._Cursor__data.clear()

        if self.alive and (self.cursor_id or not self.started):
            self._get_more(callback=functools.partial(
                self._to_list_got_more, callback, the_list))
        else:
            callback(the_list, None)

    def clone(self):
        return MotorCursor(self.delegate.clone(), self.collection)

    def rewind(self):
        # TODO: test, doc -- this seems a little extra weird w/ Motor
        self.delegate.rewind()
        self.started = False
        return self

    def tail(self, callback):
        # TODO: doc, prominently. await_data is always true.
        # TODO: doc that tailing an empty collection is expensive,
        #   consider a failsafe, e.g. timing the interval between getmore
        #   and return, and if it's short and no doc, pause before next
        #   getmore
        # TODO: doc that cursor is closed if iteration cancelled
        check_callable(callback, True)

        cursor = self.clone()

        cursor.delegate._Cursor__tailable = True
        cursor.delegate._Cursor__await_data = True

        # Start tailing
        cursor.each(functools.partial(self._tail_got_more, cursor, callback))

    def _tail_got_more(self, cursor, callback, result, error):
        if error:
            cursor.close()
            callback(None, error)
        elif result is not None:
            if callback(result, None) is False:
                # Callee cancelled tailing
                cursor.close()
                return False

        if not cursor.alive:
            # cursor died, start over soon
            self.get_io_loop().add_timeout(
                time.time() + 0.5,
                functools.partial(cursor.tail, callback))

    def get_io_loop(self):
        return self.collection.get_io_loop()

    @property
    def buffer_size(self):
        # TODO: expose so we don't have to use double-underscore hack
        return len(self.delegate._Cursor__data)

    def __getitem__(self, index):
        """Get a slice of documents from this cursor.

        Raises :class:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used.

        To get a single document use an integral index, e.g.::

          >>> def callback(result, error):
          ...     print result
          ...
          >>> db.test.find()[50].each(callback)

        An :class:`~pymongo.errors.IndexErrorIndexError` will be raised if
        the index is negative or greater than the amount of documents in
        this cursor. Any limit previously applied to this cursor will be
        ignored.

        To get a slice of documents use a slice index, e.g.::

          >>> db.test.find()[20:25].each(callback)

        This will return a copy of this cursor with a limit of ``5`` and
        skip of ``20`` applied.  Using a slice index will override any prior
        limits or skips applied to this cursor (including those
        applied through previous calls to this method). Raises
        :class:`~pymongo.errors.IndexError` when the slice has a step,
        a negative start value, or a stop value less than or equal to
        the start value.

        :Parameters:
          - `index`: An integer or slice index to be applied to this cursor
        """
        # TODO test that this raises IndexError if index < 0
        # TODO: doctest
        # TODO: test this is an error if tailable
        if self.started:
            raise pymongo.errors.InvalidOperation("MotorCursor already started")

        if isinstance(index, slice):
             return MotorCursor(self.delegate[index], self.collection)
        else:
            if not isinstance(index, (int, long)):
                raise TypeError("index %r cannot be applied to MotorCursor "
                                "instances" % index)
            # Get one document, force hard limit of 1 so server closes cursor
            # immediately
            return self[self.delegate._Cursor__skip+index:].limit(-1)

    def __del__(self):
        if self.alive and self.cursor_id:
            self.close()


# TODO: doc, explain
def create_gridfs(database, collection="fs", callback=None, io_loop=None):
    if not isinstance(database, MotorDatabase):
        raise TypeError("database must be instance of MotorDatabase, not %s" %
            repr(database))

    # TODO: test custom IOLoop w/ MotorGridFS
    if not io_loop:
        io_loop = ioloop.IOLoop.instance()

    # Run on child greenlet.
    def _create_gridfs():
        delegate = MotorGridFS.__delegate_class__(database.delegate, collection)
        return MotorGridFS(delegate, io_loop)

    async_create_gridfs = asynchronize(
        io_loop=io_loop,
        sync_method=_create_gridfs,
        has_safe_arg=False,
        callback_required=True)

    async_create_gridfs(callback=callback)


class MotorGridFS(object):
    __metaclass__ = MotorMeta
    __delegate_class__ = pymongo_gridfs.GridFS

    # TODO: consider a more protected / untempting method
    def __init__(self, delegate, io_loop):
        self.delegate = delegate
        self.io_loop = io_loop

    def get_io_loop(self):
        return self.io_loop

    def new_file(self, *args, **kwargs):
        callback = kwargs.get('callback')
        check_callable(callback, True)
        
        def new_file_callback(result, error):
            if error:
                callback(None, error)
            elif isinstance(result, pymongo_gridfile.GridIn):
                callback(GridIn(result, self.get_io_loop()), None)
            else:
                callback(result, None)
                
        kwargs['callback'] = new_file_callback
        async_new_file = asynchronize(
            io_loop=self.get_io_loop(),
            sync_method=self.delegate.new_file,
            has_safe_arg=False,
            callback_required=True)
        
        async_new_file(*args, **kwargs)

    put = Async(has_safe_arg=False, cb_required=False)

    # TODO: refactor
    def get(self, *args, **kwargs):
        callback = kwargs.get('callback')
        check_callable(callback, True)
        def get_callback(result, error):
            if error:
                callback(None, error)
            elif isinstance(result, pymongo_gridfile.GridOut):
                callback(GridOut(result, self.get_io_loop()), None)
            else:
                callback(result, None)

        kwargs['callback'] = get_callback
        async_get = asynchronize(
            io_loop=self.get_io_loop(),
            sync_method=self.delegate.get,
            has_safe_arg=False,
            callback_required=True)

        async_get(*args, **kwargs)
        
    # TODO: refactor
    def get_version(self, *args, **kwargs):
        callback = kwargs.get('callback')
        check_callable(callback, True)
        def get_version_callback(result, error):
            if error:
                callback(None, error)
            elif isinstance(result, pymongo_gridfile.GridOut):
                callback(GridOut(result, self.get_io_loop()), None)
            else:
                callback(result, None)

        kwargs['callback'] = get_version_callback
        async_get_version = asynchronize(
            io_loop=self.get_io_loop(),
            sync_method=self.delegate.get_version,
            has_safe_arg=False,
            callback_required=True)

        async_get_version(*args, **kwargs)
                
    # TODO: refactor
    def get_last_version(self, *args, **kwargs):
        callback = kwargs.get('callback')
        check_callable(callback, True)
        def get_last_version_callback(result, error):
            if error:
                callback(None, error)
            elif isinstance(result, pymongo_gridfile.GridOut):
                callback(GridOut(result, self.get_io_loop()), None)
            else:
                callback(result, None)

        kwargs['callback'] = get_last_version_callback
        async_get_last_version = asynchronize(
            io_loop=self.get_io_loop(),
            sync_method=self.delegate.get_last_version,
            has_safe_arg=False,
            callback_required=True)

        async_get_last_version(*args, **kwargs)
        
    delete = Async(has_safe_arg=False, cb_required=False)
    list   = Async(has_safe_arg=False, cb_required=True)
    exists = Async(has_safe_arg=False, cb_required=True)


# TODO: refactor
# TODO: doc no context-mgr protocol, __setattr__
class GridIn(object):
    __metaclass__ = MotorMeta
    __delegate_class__ = pymongo_gridfs.GridIn

    def __init__(self, delegate, io_loop):
        self.delegate = delegate
        self.io_loop = io_loop

    def get_io_loop(self):
        return self.io_loop

    closed = ReadOnlyDelegateProperty()
    __getattr__ = ReadOnlyDelegateProperty()
    close = Async(has_safe_arg=False, cb_required=False)
    write = Async(has_safe_arg=False, cb_required=False)
    writelines = Async(has_safe_arg=False, cb_required=False)

    def set(self, name, value, callback=None):
        async_set = asynchronize(
            io_loop=self.get_io_loop(),
            sync_method=self.delegate.__setattr__,
            has_safe_arg=False,
            callback_required=False)
        async_set(name, value, callback=callback)


# TODO: refactor
class GridOut(object):
    __metaclass__ = MotorMeta
    __delegate_class__ = pymongo_gridfs.GridOut

    def __init__(self, delegate, io_loop):
        self.delegate = delegate
        self.io_loop = io_loop

    def get_io_loop(self):
        return self.io_loop

    __getattr__ = ReadOnlyDelegateProperty()
    # TODO: doc that we can't set these props as in PyMongo
    _id = ReadOnlyDelegateProperty()
    name = ReadOnlyDelegateProperty()
    content_type = ReadOnlyDelegateProperty()
    length = ReadOnlyDelegateProperty()
    chunk_size = ReadOnlyDelegateProperty()
    upload_date = ReadOnlyDelegateProperty()
    aliases = ReadOnlyDelegateProperty()
    metadata = ReadOnlyDelegateProperty()
    md5 = ReadOnlyDelegateProperty()
    tell = ReadOnlyDelegateProperty()
    seek = ReadOnlyDelegateProperty()
    read = Async(has_safe_arg=False, cb_required=True)
    readline = Async(has_safe_arg=False, cb_required=True)
    # TODO: doc that we don't support __iter__, close(), or context-mgr protocol


if requirements_satisfied:
    class Op(gen.Task):
        def __init__(self, func, *args, **kwargs):
            check_callable(func, True)
            super(Op, self).__init__(func, *args, **kwargs)

        def get_result(self):
            (result, error), _ = super(Op, self).get_result()
            if error:
                raise error
            return result


    class WaitOp(gen.Wait):
        def get_result(self):
            (result, error), _ = super(WaitOp, self).get_result()
            if error:
                raise error

            return result


    class WaitAllOps(gen.WaitAll):
        def get_result(self):
            super_results = super(WaitAllOps, self).get_result()

            results = []
            for (result, error), _ in super_results:
                if error:
                    raise error
                else:
                    results.append(result)

            return results

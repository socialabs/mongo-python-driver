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
import logging
import socket
import time
import warnings
import weakref

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
    warnings.warn("Greenlet not installed", ImportWarning)

import pymongo
import pymongo.collection
import pymongo.cursor
import pymongo.database
import pymongo.errors
import pymongo.master_slave_connection
import pymongo.pool
import pymongo.son_manipulator


__all__ = ['MotorConnection', 'MotorReplicaSetConnection',
           'MotorMasterSlaveConnection']

# TODO: note you can't use from multithreaded app, can't fork, consider special
#   checks to prevent it?
# TODO: document that default timeout is None, ensure we're doing
#   timeouts as efficiently as possible, test performance hit with timeouts
#   from registering and cancelling timeouts
# TODO: examine & document what connection and network timeouts mean here
# TODO: document use of MotorConnection.delegate, MotorDatabase.delegate, etc.
# TODO: document that with a callback passed in, Motor's default is
#   to do SAFE writes, unlike PyMongo.
# TODO: what about PyMongo BaseObject's underlying safeness, as well
#   as w, wtimeout, and j? how do they affect control? test that.
# TODO: check handling of safe and get_last_error_options() and kwargs,
#   make sure we respect them
# TODO: SSL, IPv6
# TODO: document which versions of greenlet and tornado this has been tested
#   against, include those in some file that pip or pypi can understand?
# TODO: document this supports same Python versions as Tornado (currently
#   CPython 2.5-2.7, and 3.2
# TODO: document that Motor doesn't do requests at all, use callbacks to
#   ensure consistency
# TODO: document that Motor doesn't do auto_start_request
# TODO: is while cursor.alive or while True the right way to iterate with
#   gen.engine and next()?
# TODO: document, smugly, that Motor has configurable IOLoops
# TODO: document that Motor can do unsafe writes, AsyncMongo can't
# TODO: since Tornado uses logging, so can we
# TODO: test cross-host copydb
# TODO: test that ensure_index calls the callback even if the index
#   is already created and in the index cache - might be a special-case
#   optimization
# TODO: perhaps remove versionchanged Sphinx annotations from proxied methods,
#   unless versionchanged >= 2.3 or so -- whenever Motor joins PyMongo
# TODO: review open_sync(), does it need to disconnect after success to ensure
#   all IOStreams with old IOLoop are gone?
# TODO: note state of gridfs

def check_callable(kallable, required=False):
    if required and not kallable:
        raise TypeError("callable is required")
    if kallable is not None and not callable(kallable):
        raise TypeError("callback must be callable")


def motor_sock_method(check_closed=False):
    def wrap(method):
        @functools.wraps(method)
        def _motor_sock_method(self, *args, **kwargs):
            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main, "Should be on child greenlet"

            # We need to alter this value in inner functions, hence the list
            timeout = [None]
            self_timeout = self.timeout
            if self_timeout:
                def timeout_err():
                    timeout[0] = None
                    self.stream.set_close_callback(None)
                    self.stream.close()
                    child_gr.throw(socket.timeout("timed out"))

                timeout[0] = self.stream.io_loop.add_timeout(
                    time.time() + self_timeout, timeout_err
                )

            # This is run by IOLoop on the main greenlet when socket has
            # connected; switch back to child to continue processing
            def callback(result=None, error=None):
                self.stream.set_close_callback(None)
                if timeout[0] or not self_timeout:
                    # We didn't time out - clear the timeout if any, and resume
                    # processing on child greenlet
                    if timeout[0]:
                        self.stream.io_loop.remove_timeout(timeout[0])

                    if error:
                        child_gr.throw(error)
                    else:
                        child_gr.switch(result)

            if check_closed:
                def closed():
                    # Run on main greenlet
                    # TODO: test failed connection w/ timeout
                    if timeout[0]:
                        self.stream.io_loop.remove_timeout(timeout[0])

                    # IOStream.error is a Tornado 2.3 feature
                    error = getattr(self.stream, 'error', None)
                    child_gr.throw(error or socket.error("error"))

                self.stream.set_close_callback(closed)

            try:
                kwargs_cp = kwargs.copy()
                kwargs_cp['callback'] = callback
                method(self, *args, **kwargs_cp)
                return main.switch()
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
    return wrap


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

    @motor_sock_method(check_closed=True)
    def connect(self, pair, callback):
        """
        :Parameters:
         - `pair`: A tuple, (host, port)
        """
        self.stream.connect(pair, callback)

    @motor_sock_method()
    def sendall(self, data, callback):
        self.stream.write(data, callback)

    @motor_sock_method()
    def recv(self, num_bytes, callback):
        self.stream.read_bytes(num_bytes, callback)

    def close(self):
        if self.stream:
            try:
                self.stream.close()
            except KeyError:
                # Tornado's _impl (epoll, kqueue, ...) has already removed this
                # file descriptor from its dict.
                pass

    def fileno(self):
        return self.stream.socket.fileno()

    def __del__(self):
        self.close()


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
        super(MotorPool, self).__init__(*args, **kwargs)

    def create_connection(self, pair):
        assert greenlet.getcurrent().parent, "Should be on child greenlet"

        # Don't try IPv6 if we don't support it.
        family = socket.AF_INET
        if socket.has_ipv6:
            family = socket.AF_UNSPEC

        if not (pair or self.pair):
            raise pymongo.errors.OperationFailure(
                "(host, port) pair not configured")

        host, port = pair or self.pair
        err = None
        for res in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
            af, socktype, proto, dummy, sa = res

            # TODO: support IPV6; somehow we're not properly catching the error
            # right now and trying IPV4 as we intend in this loop, see
            # MotorSocket.connect()
            if af == socket.AF_INET6:
                continue
            try:
                sock = socket.socket(af, socktype, proto)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                motor_sock = MotorSocket(sock, self.io_loop, use_ssl=self.use_ssl)
                motor_sock.settimeout(self.conn_timeout)

                # MotorSocket will pause the current greenlet and resume it
                # when connection has completed
                motor_sock.connect(pair or self.pair)
                motor_sock.settimeout(self.net_timeout) # TODO: necessary? BasePool.connect() handles this
                return motor_sock

            except socket.error, e:
                err = e

        if err is not None:
            raise err
        else:
            # This likely means we tried to connect to an IPv6 only
            # host with an OS/kernel or Python interpeter that doesn't
            # support IPv6. The test case is Jython2.5.1 which doesn't
            # support IPv6 at all.
            raise socket.error('getaddrinfo failed')


def asynchronize(io_loop, sync_method, has_safe_arg, callback_required):
    """
    :Parameters:
     - `io_loop`:           A Tornado IOLoop
     - `sync_method`:       Bound method of pymongo Collection, Database,
                            Connection, or Cursor
     - `has_safe_arg`:      Whether the method takes a 'safe' argument
     - `callback_required`: If True, raise TypeError if no callback is passed
    """
    assert isinstance(io_loop, ioloop.IOLoop)

    # TODO doc
    # TODO: staticmethod of base class for Motor objects, add some custom
    #   stuff, like Connection can't do anything before open()
    @functools.wraps(sync_method)
    def method(*args, **kwargs):
        callback = kwargs.get('callback')
        check_callable(callback, required=callback_required)

        if 'callback' in kwargs:
            # Don't pass callback to sync_method
            kwargs = kwargs.copy()
            del kwargs['callback']

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
    pass


class Async(DelegateProperty):
    def __init__(self, has_safe_arg, cb_required):
        """
        A descriptor that wraps a PyMongo method, such as insert or remove, and
        returns an asynchronous version of the method, which takes a callback.

        :Parameters:
         - `has_safe_arg`:    Whether the method takes a 'safe' argument
         - `cb_required`:     Whether callback is required or optional
        """
        self.has_safe_arg = has_safe_arg
        self.cb_required = cb_required
        self.name = None

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
    def __cmp__(self, other):
        if isinstance(other, self.__class__):
            return cmp(self.delegate, other.delegate)
        return NotImplemented

    document_class              = ReadWriteDelegateProperty()
    slave_okay                  = ReadWriteDelegateProperty()
    safe                        = ReadWriteDelegateProperty()
    get_lasterror_options       = ReadWriteDelegateProperty()
    set_lasterror_options       = ReadWriteDelegateProperty()
    unset_lasterror_options     = ReadWriteDelegateProperty()
    read_preference             = ReadWriteDelegateProperty()
    name                        = ReadOnlyDelegateProperty()

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, repr(self.delegate))


class MotorConnectionBase(MotorBase):
    database_names              = Async(has_safe_arg=False, cb_required=True)
    close_cursor                = Async(has_safe_arg=False, cb_required=False)
    disconnect                  = ReadOnlyDelegateProperty()
    tz_aware                    = ReadOnlyDelegateProperty()

    def __init__(self, io_loop=None):
        if io_loop:
            if not isinstance(io_loop, ioloop.IOLoop):
                raise TypeError(
                    "io_loop must be instance of IOLoop, not %s" % (
                        repr(io_loop)))
            self.io_loop = io_loop
        else:
            self.io_loop = ioloop.IOLoop.instance()

    def get_io_loop(self):
        return self.io_loop

    def __getattr__(self, name):
        if not self.connected:
            msg = "Can't access database on %s before calling open()" \
                  " or open_sync()" % (
                self.__class__.__name__
            )
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
            self.io_loop, self.delegate.drop_database, False, True)
        async_method(name_or_database, callback=callback)

    def start_request(self):
        raise NotImplementedError("Motor doesn't implement requests")

    in_request = end_request = start_request

    @property
    def connected(self):
        """True after :meth:`open` or :meth:`open_sync` completes"""
        return self.delegate is not None


class MotorConnectionBasePlus(MotorConnectionBase):
    """MotorConnection and MotorReplicaSetConnection common functionality.
    (MotorMasterSlaveConnection is more primitive, so it inherits from
    MotorConnectionBase.)
    """
    server_info                 = Async(has_safe_arg=False, cb_required=False)
    copy_database               = Async(has_safe_arg=False, cb_required=False)
    close                       = ReadOnlyDelegateProperty()
    max_bson_size               = ReadOnlyDelegateProperty()
    max_pool_size               = ReadOnlyDelegateProperty()
    _cache_credentials          = ReadOnlyDelegateProperty()

    def __init__(self, *args, **kwargs):
        if 'auto_start_request' in kwargs:
            raise pymongo.errors.ConfigurationError(
                "Motor doesn't support auto_start_request")

        if 'io_loop' in kwargs:
            kwargs = kwargs.copy()
            io_loop = kwargs.pop('io_loop')
        else:
            io_loop = None

        super(MotorConnectionBasePlus, self).__init__(io_loop)

        # Store args and kwargs for when open() is called
        self._init_args = args
        self._init_kwargs = kwargs
        self.delegate = None

    def open(self, callback):
        """
        Actually connect, passing self to a callback when connected.

        :Parameters:
         - `callback`: Optional function taking parameters (connection, error)
        """
        check_callable(callback)

        if self.connected:
            # TODO: test this branch, with and without callback
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

            self.open(callback)

            # Returns once callback has been executed and loop stopped.
            self.io_loop.start()
        finally:
            # Replace the private IOLoop with the default loop
            self.io_loop = standard_loop
            if self.delegate:
                self.delegate.pool_class = functools.partial(
                    MotorPool, self.io_loop)

                # TODO: get proper arguments for Pool
                self.delegate.pool = self.delegate.pool_class(None, 17, None, None, False)

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


class MotorConnection(MotorConnectionBasePlus):
    __delegate_class__ = pymongo.connection.Connection

    kill_cursors                = Async(has_safe_arg=False, cb_required=True)
    fsync                       = Async(has_safe_arg=False, cb_required=False)
    unlock                      = Async(has_safe_arg=False, cb_required=False)
    nodes                       = ReadOnlyDelegateProperty()
    host                        = ReadOnlyDelegateProperty()
    port                        = ReadOnlyDelegateProperty()

    def __init__(self, *args, **kwargs):
        """Create a new connection to a single MongoDB instance at *host:port*.

        :meth:`open` or :meth:`open_sync` must be called before using a new
        MotorConnection.

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
           and execute asynchronously.
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


class MotorReplicaSetConnection(MotorConnectionBasePlus):
    __delegate_class__ = pymongo.replica_set_connection.ReplicaSetConnection

    primary                       = ReadOnlyDelegateProperty()
    secondaries                   = ReadOnlyDelegateProperty()
    arbiters                      = ReadOnlyDelegateProperty()
    hosts                         = ReadOnlyDelegateProperty()
    seeds                         = ReadOnlyDelegateProperty()

    def __init__(self, *args, **kwargs):
        """Create a new connection to a MongoDB replica set.

        :meth:`open` or :meth:`open_sync` must be called before using a new
        MotorReplicaSetConnection.

        MotorReplicaSetConnection takes the same constructor arguments as
        :class:`~pymongo.replica_set_connection.ReplicaSetConnection`,
        as well as:

        :Parameters:
          - `io_loop` (optional): Special :class:`tornado.ioloop.IOLoop`
            instance to use instead of default
        """
        super(MotorReplicaSetConnection, self).__init__(*args, **kwargs)

    def _delegate_init_args(self):
        # This _monitor_class will be passed to PyMongo's
        # ReplicaSetConnection when we create it.
        args, kwargs = \
            super(MotorReplicaSetConnection, self)._delegate_init_args()
        kwargs['_monitor_class'] = functools.partial(
            MotorReplicaSetMonitor, self.io_loop)
        return args, kwargs

    def _get_pools(self):
        # TODO: expose the PyMongo pools, or otherwise avoid this
        pools = []
        for mongo in self.delegate._ReplicaSetConnection__pools.values():
            if 'pool' in mongo:
                pools.append(mongo['pool'])

        return pools


# PyMongo uses a background thread to regularly inspect the replica set and
# monitor it for changes. In Motor, use a periodic callback on the IOLoop to
# monitor the set.
class MotorReplicaSetMonitor(object):
    def __init__(self, io_loop, obj, interval=5):
        assert isinstance(
            obj, pymongo.replica_set_connection.ReplicaSetConnection)
        assert isinstance(io_loop, ioloop.IOLoop)

        self.ref = weakref.ref(obj)
        self.io_loop = io_loop
        self.interval = interval
        self.async_refresh = asynchronize(
            self.io_loop,
            self.refresh,
            has_safe_arg=False,
            callback_required=False)

    def refresh(self):
        # Dereference the weakref to a ReplicaSetConnection. If it's no longer
        # valid, quit.
        rsc = self.ref()
        if rsc:
            try:
                rsc.refresh()
            except Exception:
                logging.exception("Refreshing replica set configuration")

            self.io_loop.add_timeout(
                time.time() + self.interval, self.async_refresh)

    def start(self):
        """Refresh loop to notice changes in replica set configuration
        """
        self.io_loop.add_callback(self.async_refresh)


class MotorMasterSlaveConnection(MotorConnectionBase):
    __delegate_class__ = pymongo.master_slave_connection.MasterSlaveConnection

    def __init__(self, master, slaves, *args, **kwargs):
        """Create a new connection to a MongoDB master-slave set.
        Takes same arguments as
        :class:`~pymongo.master_slave_connection.MasterSlaveConnection`.

        The master and slaves must be connected :class:`MotorConnection`
        instances.

          >>> master = MotorConnection()
          >>> master.open_sync()
          MotorConnection(Connection('localhost', 27017))
          >>> slaves = [Connection(port=27018)]
          >>> slaves[0].open_sync()
          MotorConnection(Connection('localhost', 27018))
          >>> msc = MotorMasterSlaveConnection(master, slaves)
        """
        # NOTE: master and slaves must be MotorConnections mainly to ensure that
        # their pool_class has been set to MotorPool.
        if 'io_loop' in kwargs:
            kwargs = kwargs.copy()
            io_loop = kwargs.pop('io_loop')
        elif isinstance(master, MotorConnection):
            io_loop = master.io_loop
        else:
            io_loop = None

        # Sets self.io_loop
        super(MotorMasterSlaveConnection, self).__init__(io_loop)

        # Unwrap connections before passing to PyMongo MasterSlaveConnection
        if not isinstance(master, MotorConnection):
            raise TypeError(
                "master must be a MotorConnection")
        elif not master.connected:
            raise pymongo.errors.InvalidOperation(
                "master must already be connected")
        elif self.io_loop and master.io_loop != self.io_loop:
            raise pymongo.errors.ConfigurationError(
                "master connection must have same IOLoop as "
                "MasterSlaveConnection"
            )

        master = master.delegate

        slave_delegates = []
        for slave in slaves:
            if not isinstance(slave, MotorConnection):
                raise TypeError(
                    "slave must be a MotorConnection")
            elif not slave.connected:
                raise pymongo.errors.InvalidOperation(
                    "slave must already be connected")
            elif slave.io_loop != self.io_loop:
                raise pymongo.errors.ConfigurationError(
                    "slave connection must have same IOLoop as "
                    "MasterSlaveConnection"
                )

            slave_delegates.append(slave.delegate)

        self.delegate = pymongo.master_slave_connection.MasterSlaveConnection(
            master, slave_delegates, *args, **kwargs)

    @property
    def master(self):
        """A :class:`MotorConnection`"""
        motor_master = MotorConnection()
        motor_master.delegate = self.delegate.master
        motor_master.io_loop = self.io_loop
        return motor_master

    @property
    def slaves(self):
        """A list of :class:`MotorConnection` instances"""
        motor_slaves = []
        for slave in self.delegate.slaves:
            motor_slave = MotorConnection()
            motor_slave.delegate = slave
            motor_slave.io_loop = self.io_loop
            motor_slaves.append(motor_slave)

        return motor_slaves

    def _get_pools(self):
        # TODO: expose the PyMongo pool, or otherwise avoid this
        return [self.master._Connection__pool]


class MotorDatabase(MotorBase):
    __delegate_class__ = pymongo.database.Database

    # list of overridden async operations on a MotorDatabase instance
    set_profiling_level           = Async(has_safe_arg=False, cb_required=False)
    reset_error_history           = Async(has_safe_arg=False, cb_required=False)
    add_user                      = Async(has_safe_arg=False, cb_required=False)
    remove_user                   = Async(has_safe_arg=False, cb_required=False)
    logout                        = Async(has_safe_arg=False, cb_required=False)
    command                       = Async(has_safe_arg=False, cb_required=False)

    authenticate                  = Async(has_safe_arg=False, cb_required=True)
    collection_names              = Async(has_safe_arg=False, cb_required=True)
    current_op                    = Async(has_safe_arg=False, cb_required=True)
    profiling_level               = Async(has_safe_arg=False, cb_required=True)
    profiling_info                = Async(has_safe_arg=False, cb_required=True)
    error                         = Async(has_safe_arg=False, cb_required=True)
    last_status                   = Async(has_safe_arg=False, cb_required=True)
    previous_error                = Async(has_safe_arg=False, cb_required=True)
    dereference                   = Async(has_safe_arg=False, cb_required=True)
    eval                          = Async(has_safe_arg=False, cb_required=True)

    # TODO: remove system_js?
    system_js                     = ReadOnlyDelegateProperty()
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
        :meth:`~pymongo.database.Database.create_collection`, plus:

        :Parameters:
          - `callback`: Optional function taking parameters (result, error)
        """
        # We override create_collection to wrap the Collection it returns in a
        # MotorCollection.
        sync_method = self.delegate.create_collection
        async_method = asynchronize(
            self.get_io_loop(), sync_method, False, False)

        def cb(collection, error):
            if isinstance(collection, pymongo.collection.Collection):
                collection = MotorCollection(self, name)

            callback(collection, error)

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

    create_index            = Async(has_safe_arg=False, cb_required=False)
    drop_indexes            = Async(has_safe_arg=False, cb_required=False)
    drop_index              = Async(has_safe_arg=False, cb_required=False)
    drop                    = Async(has_safe_arg=False, cb_required=False)
    ensure_index            = Async(has_safe_arg=False, cb_required=False)
    reindex                 = Async(has_safe_arg=False, cb_required=False)
    rename                  = Async(has_safe_arg=False, cb_required=False)
    find_and_modify         = Async(has_safe_arg=False, cb_required=False)

    update                  = Async(has_safe_arg=True, cb_required=False)
    insert                  = Async(has_safe_arg=True, cb_required=False)
    remove                  = Async(has_safe_arg=True, cb_required=False)
    save                    = Async(has_safe_arg=True, cb_required=False)

    index_information       = Async(has_safe_arg=False, cb_required=True)
    count                   = Async(has_safe_arg=False, cb_required=True)
    options                 = Async(has_safe_arg=False, cb_required=True)
    group                   = Async(has_safe_arg=False, cb_required=True)
    distinct                = Async(has_safe_arg=False, cb_required=True)
    inline_map_reduce       = Async(has_safe_arg=False, cb_required=True)
    find_one                = Async(has_safe_arg=False, cb_required=True)

    uuid_subtype            = ReadWriteDelegateProperty()

    full_name               = ReadOnlyDelegateProperty()

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
                "Pass a callback to next, each, to_list, count, or tail, not"
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
        kwargs_cp = kwargs.copy()
        callback = kwargs_cp.pop('callback', None)

        def map_reduce_callback(result, error):
            if isinstance(result, pymongo.collection.Collection):
                result = self.database[result.name]
            callback(result, error)

        kwargs_cp['callback'] = map_reduce_callback
        sync_method = self.delegate.map_reduce
        async_mr = asynchronize(self.get_io_loop(), sync_method, False, True)
        async_mr(*args, **kwargs_cp)

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
    count                       = Async(has_safe_arg=False, cb_required=True)
    distinct                    = Async(has_safe_arg=False, cb_required=True)
    explain                     = Async(has_safe_arg=False, cb_required=True)
    close                       = Async(has_safe_arg=False, cb_required=False)

    slave_okay                  = ReadOnlyDelegateProperty()
    alive                       = ReadOnlyDelegateProperty()
    cursor_id                   = ReadOnlyDelegateProperty()

    batch_size                  = MotorCursorChainingMethod()
    add_option                  = MotorCursorChainingMethod()
    remove_option               = MotorCursorChainingMethod()
    limit                       = MotorCursorChainingMethod()
    skip                        = MotorCursorChainingMethod()
    max_scan                    = MotorCursorChainingMethod()
    sort                        = MotorCursorChainingMethod()
    hint                        = MotorCursorChainingMethod()
    where                       = MotorCursorChainingMethod()

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
        async_refresh = asynchronize(
            self.get_io_loop(), self.delegate._refresh, False, True)
        async_refresh(callback=callback)

    def next(self, callback):
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
          ...     cursor.next(callback=on_next)
          ...
          >>> def on_next(result, error):
          ...     if error:
          ...         raise error
          ...     elif result:
          ...         sys.stdout.write(str(result['_id']) + ', ')
          ...         cursor.next(callback=on_next)
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
                add_callback(functools.partial(callback, None, None))
                self.close()
                return

            callback(doc, None)
        elif self.alive and (self.cursor_id or not self.started):
            def got_more(batch_size, error):
                if error:
                    callback(None, error)
                else:
                    self.next(callback)

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
        add_callback = self.get_io_loop().add_callback

        def got_more(batch_size, error):
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

                # Quit if callback returns exactly False (not None)
                if callback(doc, None) is False:
                    self.close()
                    return

            if self.alive and (self.cursor_id or not self.started):
                self._get_more(got_more)
            else:
                # Complete
                add_callback(functools.partial(callback, None, None))

        got_more(None, None)

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
        # TODO: error if tailable
        check_callable(callback, required=True)
        the_list = []

        # Special case
        if self.delegate._Cursor__empty:
            callback([], None)
            return

        def got_more(batch_size, error):
            if error:
                callback(None, error)
                return

            if self.buffer_size > 0:
                the_list.extend(self.delegate._Cursor__data)
                self.delegate._Cursor__data[:] = []

            if self.alive and (self.cursor_id or not self.started):
                self._get_more(got_more)
            else:
                callback(the_list, None)

        got_more(None, None)

    def clone(self):
        return MotorCursor(self.delegate.clone(), self.collection)

    def rewind(self):
        # TODO: test, doc -- this seems a little extra weird w/ Motor
        self.delegate.rewind()
        self.started = False
        return self

    def tail(self, callback, await_data=None):
        # TODO: doc, prominently =)
        # TODO: doc that tailing an empty collection is expensive
        # TODO: test dropping a collection while tailing it
        # TODO: test tailing collection that isn't empty at first
        check_callable(callback, True)

        loop = self.get_io_loop()
        add_callback = loop.add_callback
        add_timeout = loop.add_timeout

        cursor = self.clone()

        # This is a list so we can modify it from the inner callback
        started = [False]

        # TODO: HACK!
        cursor.delegate._Cursor__tailable = True

        # If await_data parameter is set, then override whatever await_data
        # value was passed to find() (default False)
        # TODO: reconsider or at least test this crazy logic, doc
        if await_data is not None:
            # TODO: HACK!
            cursor.delegate._Cursor__await_data = await_data

        def inner_callback(result, error):
            if error:
                cursor.close()
                callback(None, error)
            elif result is not None:
                started[0] = True
                if callback(result, None) is False:
                    cursor.close()
                    return False
            elif cursor.alive:
                # result and error are both none, meaning no new data in
                # this batch; keep on truckin'
                add_callback(
                    functools.partial(cursor.each, inner_callback)
                )
            else:
                # cursor died, start over soon, but only if it's because this
                # collection was empty when we began.
                if not started[0]:
                    add_timeout(
                        time.time() + 0.5,
                        functools.partial(cursor.tail, callback, await_data)
                    )
                else:
                    # TODO: why exactly would this happen?
                    exc = pymongo.errors.OperationFailure("cursor died")
                    add_callback(functools.partial(callback, None, exc))

        # Start tailing
        cursor.each(inner_callback)

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
        # TODO doc that this does not raise IndexError if index > len results
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


# TODO: move to 'motorgen' submodule, test, doc all these three gen tasks, and
#   consider if there are additional convenience methods possible. Lots of
#   examples. Link to tornado gen docs.
# TODO: some way to generate docs even without Tornado installed?
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

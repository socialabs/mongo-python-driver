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

"""Test the Tornado asynchronous Python driver for MongoDB."""

import functools
import os
import time
import unittest
import datetime
import types

from nose.plugins.skip import SkipTest

have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False

# Don't know why this is necessary for importing pymongo when running in the
# PyCharm debugger
import sys
sys.path.insert(
    0,
    os.path.normpath(os.path.join(os.path.dirname(__file__), '../../..'))
)

import pymongo
import pymongo.pool
from pymongo.objectid import ObjectId
from pymongo.tornado_async import async
from pymongo.son_manipulator import AutoReference, NamespaceInjector
from pymongo.errors import InvalidOperation, ConfigurationError, \
    DuplicateKeyError

from test.utils import delay

import tornado.ioloop, tornado.stack_context
from tornado import gen

# Tornado testing tools
from pymongo.tornado_async.test_tornado_async import eventually, puritanical


# TODO: move to test tools, cleanup, make as many tests in here use @async_test_engine
# as possible.
def async_test_engine(timeout_sec=5):
    if not isinstance(timeout_sec, int) and not isinstance(timeout_sec, float):
        raise TypeError(
            "Expected int or float, got %s\n"
            "Use async_test_engine like:\n\t@async_test_engine()\n"
            "or:\n\t@async_test_engine(5)" % (
                repr(timeout_sec)
            )
        )

    timeout_sec = max(timeout_sec, float(os.environ.get('TIMEOUT_SEC', 0)))
    def decorator(func):
        class AsyncTestRunner(gen.Runner):
            def __init__(self, gen, timeout):
                super(AsyncTestRunner, self).__init__(gen)
                self.timeout = timeout

            def run(self):
                loop = tornado.ioloop.IOLoop.instance()
                try:
                    super(AsyncTestRunner, self).run()
                except Exception:
                    loop.remove_timeout(self.timeout)
                    loop.stop()
                    raise

                if self.finished:
                    loop.remove_timeout(self.timeout)
                    loop.stop()

        @functools.wraps(func)
        def _async_test(self):
            loop = tornado.ioloop.IOLoop.instance()

            def on_timeout():
                loop.stop()
                raise AssertionError("%s timed out" % func)

            timeout = loop.add_timeout(time.time() + timeout_sec, on_timeout)

            gen = func(self)
            assert isinstance(gen, types.GeneratorType)
            AsyncTestRunner(gen, timeout).run()

            loop.start()
        return _async_test
    return decorator


class AssertRaises(tornado.gen.Task):
    def __init__(self, exc_type, func, *args, **kwargs):
        super(AssertRaises, self).__init__(func, *args, **kwargs)
        if not isinstance(exc_type, type):
            raise TypeError("%s is not a class" % repr(exc_type))

        if not issubclass(exc_type, Exception):
            raise TypeError(
                "%s is not a subclass of Exception" % repr(exc_type))
        self.exc_type = exc_type

    def get_result(self):
        (result, error), _ = self.runner.pop_result(self.key)
        if not isinstance(error, self.exc_type):
            raise AssertionError("%s not raised" % self.exc_type.__name__)
        return result

# TODO: sphinx-compat?
# TODO: test map_reduce and inline_map_reduce
# TODO: test that a database called sync_connection, a collection called
#   sync_database, a collection called foo.sync_collection can be accessed via
#   [ ]


host = os.environ.get("DB_IP", "localhost")
port = int(os.environ.get("DB_PORT", 27017))


class TornadoTest(
    puritanical.PuritanicalTest,
    eventually.AssertEventuallyTest
):
    longMessage = True

    def setUp(self):
        super(TornadoTest, self).setUp()

        # Store a regular synchronous pymongo Connection for convenience while
        # testing. Low timeouts so we don't hang a test because, say, Mongo
        # isn't up or is hung by a long-running $where clause.
        self.sync_cx = pymongo.Connection(
            host, port, connectTimeoutMS=200, socketTimeoutMS=200
        )
        self.sync_db = self.sync_cx.test
        self.sync_coll = self.sync_db.test_collection
        self.sync_coll.drop()

        # Make some test data
        self.sync_coll.ensure_index([('s', pymongo.ASCENDING)], unique=True)
        self.sync_coll.insert(
            [{'_id': i, 's': hex(i)} for i in range(200)],
            safe=True
        )

        self.open_cursors = self.get_open_cursors()

    def get_open_cursors(self):
        output = self.sync_cx.admin.command('serverStatus')
        return output.get('cursors', {}).get('totalOpen')

    def async_connection(self, *args, **kwargs):
        cx = async.TornadoConnection(host, port, *args, **kwargs)
        loop = tornado.ioloop.IOLoop.instance()

        def connected(connection, error):
            loop.stop() # So we can exit async_connection()
            if error:
                raise error

        cx.open(connected)
        loop.start()
        assert cx.connected, "Couldn't connect to MongoDB"
        return cx

    def wait_for_cursors(self):
        """
        Useful if you need to ensure some operation completes, e.g. an each(),
        so that all cursors are closed.
        """
        if self.get_open_cursors() > self.open_cursors:
            loop = tornado.ioloop.IOLoop.instance()

            def timeout_err():
                loop.stop()

            timeout_sec = float(os.environ.get('TIMEOUT_SEC', 5))
            timeout = loop.add_timeout(time.time() + timeout_sec, timeout_err)

            def check():
                if self.get_open_cursors() <= self.open_cursors:
                    loop.remove_timeout(timeout)
                    loop.stop()

            period_ms = 500
            tornado.ioloop.PeriodicCallback(check, period_ms).start()
            loop.start()

    def check_callback_handling(self, fn, required=False):
        """
        Take a function and verify that it accepts a 'callback' parameter
        and properly type-checks it. If 'required', check that fn requires
        a callback.
        """
        self.assertRaises(TypeError, fn, callback='foo')
        self.assertRaises(TypeError, fn, callback=1)

        if required:
            self.assertRaises(TypeError, fn)
            self.assertRaises(TypeError, fn, None)
        else:
            # Should not raise
            fn(callback=None)

        # Should not raise
        fn(callback=lambda result, error: None)

    def tearDown(self):
        actual_open_cursors = self.get_open_cursors()

        if actual_open_cursors != self.open_cursors:
            # Run the loop for a little bit: An unfortunately convoluted means of
            # letting all cursors close themselves before we finish the test, so
            # tearDown() doesn't complain about cursors left open.
            loop = tornado.ioloop.IOLoop.instance()
            loop.add_timeout(time.time() + 0.25, loop.stop)
            loop.start()

            actual_open_cursors = self.get_open_cursors()

        self.assertEqual(
            self.open_cursors,
            actual_open_cursors,
            "%d open cursors at start of test, %d at end, should be equal" % (
                self.open_cursors, actual_open_cursors
            )
        )

        self.sync_coll.drop()
        super(TornadoTest, self).tearDown()


class TornadoTestBasic(TornadoTest):
    def test_repr(self):
        cx = self.async_connection()
        self.assert_(repr(cx).startswith('TornadoConnection'))
        db = cx.test
        self.assert_(repr(db).startswith('TornadoDatabase'))
        coll = db.test
        self.assert_(repr(coll).startswith('TornadoCollection'))

    @async_test_engine()
    def test_connection(self):
        cx = async.TornadoConnection(host, port)

        # Can't access databases before connecting
        self.assertRaises(
            pymongo.errors.InvalidOperation,
            lambda: cx.some_database_name
        )

        self.assertRaises(
            pymongo.errors.InvalidOperation,
            lambda: cx['some_database_name']
        )

        result = yield async.Op(cx.open)
        self.assertEqual(result, cx)
        self.assertTrue(cx.connected)

    def test_connection_callback(self):
        cx = async.TornadoConnection(host, port)
        self.check_callback_handling(cx.open)

    @async_test_engine()
    def test_dotted_collection_name(self):
        # Ensure that remove, insert, and find work on collections with dots
        # in their names.
        cx = self.async_connection()
        for coll in (
            cx.test.foo,
            cx.test.foo.bar,
            cx.test.foo.bar.baz.quux
        ):
            result = yield async.Op(coll.insert, {'_id':'xyzzy'})
            self.assertEqual(result, 'xyzzy')
            result = yield async.Op(coll.find_one, {'_id':'xyzzy'})
            self.assertEqual(result['_id'], 'xyzzy')
            yield async.Op(coll.remove)
            result = yield async.Op(coll.find_one, {'_id':'xyzzy'})
            self.assertEqual(result, None)
            yield async.Op(coll.remove)

    def test_cursor(self):
        cx = self.async_connection()
        coll = cx.test.foo
        # We're not actually running the find(), so null callback is ok
        cursor = coll.find()
        self.assert_(isinstance(cursor, async.TornadoCursor))
        self.assertFalse(cursor.started, "Cursor shouldn't start immediately")

    def test_find(self):
        # 1. Open a connection.
        #
        # 2. test_collection has docs inserted in setUp(). Query for documents
        # with _id 0 through 13, in batches of 5: 0-4, 5-9, 10-13.
        #
        # 3. For each document, check if the cursor has been closed. I expect
        # it to remain open until we've retrieved doc with _id 10. Oddly, Mongo
        # doesn't close the cursor and return cursor_id 0 if the final batch
        # exactly contains the last document -- the last batch size has to go
        # one *past* the final document in order to close the cursor.
        connection = yield async.Op(async.TornadoConnection(host, port).open)

        cursor = connection.test.test_collection.find(
            {'_id': {'$lt':14}},
            {'s': False}, # exclude 's' field
            sort=[('_id', 1)],
        ).batch_size(5)

        results = []
        while True:
            doc = yield async.Op(cursor.each)

            if doc:
                results.append(doc['_id'])

            if doc and doc['_id'] < 10:
                self.assertEqual(
                    1 + self.open_cursors,
                    self.get_open_cursors()
                )
            else:
                self.assertEqual(
                    self.open_cursors,
                    self.get_open_cursors()
                )

            if not doc:
                # Done iterating
                break

        self.assertEqual(range(14), results)

    @async_test_engine()
    def test_find_where(self):
        # Check that $where clauses work
        coll = self.async_connection().test.test_collection
        res = yield async.Op(coll.find().to_list)
        self.assertEqual(200,len(res))

        # Get the one doc with _id of 8
        where = 'this._id == 2 * 4'
        res0 = yield async.Op(coll.find({'$where': where}).to_list)
        self.assertEqual(1, len(res0))
        self.assertEqual(8, res0[0]['_id'])

        res1 = yield async.Op(coll.find().where(where).to_list)
        self.assertEqual(res0, res1)

    def test_find_callback(self):
        cx = self.async_connection()
        cursor = cx.test.test_collection.find()
        self.check_callback_handling(cursor.each, required=True)

        # Ensure tearDown doesn't complain about open cursors
        self.wait_for_cursors()

        self.check_callback_handling(cursor.to_list, required=True)
        self.wait_for_cursors()

    def test_find_is_async(self):
        """
        Confirm find() is async by launching three operations which will finish
        out of order. Also test that TornadoConnection doesn't reuse sockets
        incorrectly.
        """
        # TODO: this often fails, due to race conditions in the test
        # Make a big unindexed collection that will take a long time to query
        self.sync_db.drop_collection('big_coll')
        self.sync_db.big_coll.insert([
            {'s': hex(s)} for s in range(1000)
        ])

        cx = self.async_connection()

        results = []

        def callback(doc, error):
            if error:
                raise error
            if doc:
                results.append(doc)

        # Launch 3 find operations for _id's 1, 2, and 3, which will finish in
        # order 2, 3, then 1.
        loop = tornado.ioloop.IOLoop.instance()

        now = time.time()

        # This find() takes 10 seconds
        loop.add_timeout(
            now + 0.1,
            lambda: cx.test.test_collection.find(
                {'_id': 1, '$where': delay(10)},
                fields={'s': True, '_id': False},
            ).each(callback)
        )

        # Very fast lookup
        loop.add_timeout(
            now + 0.5,
            lambda: cx.test.test_collection.find(
                {'_id': 2},
                fields={'s': True, '_id': False},
            ).each(callback)
        )

        # Find {'i': 3} in big_coll -- even though there's only one such record,
        # MongoDB will have to scan the whole table to know that. We expect this
        # to be faster than 10 seconds (the $where clause above) and slower than
        # the indexed lookup above.
        loop.add_timeout(
            now + 1,
            lambda: cx.test.big_coll.find(
                {'s': hex(3)},
                fields={'s': True, '_id': False},
            ).each(callback)
        )

        # Results were appended in order 2, 3, 1
        self.assertEventuallyEqual(
            [{'s': hex(s)} for s in (2, 3, 1)],
            lambda: results,
            timeout_sec=11
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_find_and_cancel(self):
        cx = self.async_connection()
        results = []

        def callback(doc, error):
            if error:
                raise error

            results.append(doc)

            if len(results) == 2:
                # cancel iteration
                return False

        cx.test.test_collection.find(sort=[('s', 1)]).each(callback)
        self.assertEventuallyEqual(
            [
                {'_id': 0, 's': hex(0)},
                {'_id': 1, 's': hex(1)},
            ],
            lambda: results,
        )

        tornado.ioloop.IOLoop.instance().start()

        # There are 200 docs, but we canceled after 2
        self.assertEqual(2, len(results))

    def test_find_to_list(self):
        cx = self.async_connection()
        results = []

        def callback(docs, error):
            if error:
                raise error

            results.append([doc['_id'] for doc in docs])

            if len(results) == 2:
                # cancel iteration
                return False

        cx.test.test_collection.find(
            sort=[('_id', 1)], fields=['_id']
        ).to_list(callback)

        self.assertEventuallyEqual([range(200)], lambda: results)
        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    @async_test_engine()
    def test_find_one(self):
        result = yield async.Op(
            self.async_connection().test.test_collection.find_one,
            {'_id': 1}
        )

        self.assertEqual({'_id': 1, 's': hex(1)}, result)

    def test_find_one_callback(self):
        cx = self.async_connection()
        self.check_callback_handling(
            cx.test.test_collection.find_one,
            required=True
        )

    def test_find_one_is_async(self):
        """
        Confirm find_one() is async by launching two operations which will
        finish out of order.
        """
        results = []

        def callback(result, error):
            if error:
                raise error
            results.append(result)

        # Launch 2 find_one operations for _id's 1 and 2, which will finish in
        # order 2 then 1.
        loop = tornado.ioloop.IOLoop.instance()

        cx = self.async_connection()

        # This find_one() takes half a second
        loop.add_timeout(
            time.time() + 0.1,
            lambda: cx.test.test_collection.find_one(
                {'_id': 1, '$where': delay(1)},
                fields={'s': True, '_id': False},
                callback=callback
            )
        )

        # Very fast lookup
        loop.add_timeout(
            time.time() + 0.2,
            lambda: cx.test.test_collection.find_one(
                {'_id': 2},
                fields={'s': True, '_id': False},
                callback=callback
            )
        )

        # Results were appended in order 2, 1
        self.assertEventuallyEqual(
            [{'s': hex(s)} for s in (2, 1)],
            lambda: results
        )

        tornado.ioloop.IOLoop.instance().start()

    @async_test_engine()
    def test_count(self):
        coll = self.async_connection().test.test_collection
        result = yield async.Op(coll.find().count)
        self.assertEqual(200, result)
        result = yield async.Op(coll.find({'_id': {'$gt': 99}}).count)
        self.assertEqual(100, result)
        where = 'this._id % 2 == 0 && this._id >= 50'
        result = yield async.Op(coll.find({'$where': where}).count)
        self.assertEqual(75, result)
        result = yield async.Op(coll.find().where(where).count)
        self.assertEqual(75, result)
        result = yield async.Op(
            coll.find({'_id': {'$lt': 100}}).where(where).count)
        self.assertEqual(25, result)
        result = yield async.Op(coll.find(
            {'_id': {'$lt': 100}, '$where': where}).count)
        self.assertEqual(result, 25)

    def test_cursor_close(self):
        """
        The flow here is complex; we're testing that a cursor can be explicitly
        closed.
        1. Create a cursor on the server by running find()
        2. In the find() callback, start closing the cursor
        3. Wait a little to make sure the cursor closes
        4. Stop the IOLoop so we can exit test_cursor_close()
        5. In TornadoTest.tearDown(), we'll assert all cursors have closed.
        """
        cx = self.async_connection()
        loop = tornado.ioloop.IOLoop.instance()

        def found(result, error):
            if error:
                raise error

            cursor.close()
            loop.add_timeout(time.time() + .1, loop.stop)

            # Cancel iteration, so the cursor isn't exhausted
            return False

        cursor = cx.test.test_collection.find()
        cursor.each(callback=found)

        # Start the find(), the callback will close the cursor
        loop.start()

    @async_test_engine()
    def test_update(self):
        cx = self.async_connection()
        result = yield async.Op(cx.test.test_collection.update,
            {'_id': 5},
            {'$set': {'foo': 'bar'}},
        )

        self.assertEqual(1, result['ok'])
        self.assertEqual(True, result['updatedExisting'])
        self.assertEqual(1, result['n'])
        self.assertEqual(None, result['err'])

    @async_test_engine()
    def test_update_bad(self):
        """
        Violate a unique index, make sure we handle error well
        """
        results = []

        def callback(result, error):
            self.assert_(isinstance(error, DuplicateKeyError))
            self.assertEqual(None, result)
            results.append(result)

        cx = self.async_connection()

        try:
            # There's already a document with s: hex(4)
            yield async.Op(
                cx.test.test_collection.update,
                {'_id': 5},
                {'$set': {'s': hex(4)}},
            )
        except DuplicateKeyError:
            pass
        else:
            self.fail("DuplicateKeyError not raised")

    def test_update_callback(self):
        cx = self.async_connection()
        self.check_callback_handling(
            functools.partial(cx.test.test_collection.update, {}, {})
        )

    @async_test_engine()
    def test_insert(self):
        result = yield async.Op(
            self.async_connection().test.test_collection.insert,
            {'_id': 201}
        )

        # insert() returns new _id
        self.assertEqual(201, result)

    @async_test_engine()
    def test_insert_many(self):
        result = yield async.Op(
            self.async_connection().test.test_collection.insert,
            [{'_id': i, 's': hex(i)} for i in range(201, 211)]
        )

        self.assertEqual(range(201, 211), result)

    @async_test_engine()
    def test_insert_bad(self):
        """
        Violate a unique index, make sure we handle error well
        """
        yield AssertRaises(
            DuplicateKeyError,
            self.async_connection().test.test_collection.insert,
            {'s': hex(4)} # There's already a document with s: hex(4)
        )

    def test_insert_many_one_bad(self):
        """
        Violate a unique index in one of many updates, make sure we handle error
        well
        """
        result = yield AssertRaises(
            DuplicateKeyError,
            self.async_connection().test.test_collection.insert,
            [
                {'_id': 201, 's': hex(201)},
                {'_id': 202, 's': hex(4)}, # Already exists
                {'_id': 203, 's': hex(203)},
            ]
        )


        # In async, even though first insert succeeded, result is None
        self.assertEqual(None, result)

        # First insert should've succeeded
        result = yield async.Op(
            self.sync_db.test_collection.find({'_id': 201}).to_list
        )

        self.assertEqual([{'_id': 201, 's': hex(201)}], result)

        # Final insert didn't execute, since second failed
        result = yield async.Op(
            self.sync_db.test_collection.find({'_id': 203}).to_list
        )

        self.assertEqual([], result)

    def test_save_callback(self):
        cx = self.async_connection()
        self.check_callback_handling(
            functools.partial(cx.test.test_collection.save, {})
        )

    @async_test_engine()
    def test_save_with_id(self):
        result = yield async.Op(
            self.async_connection().test.test_collection.save,
            {'_id': 5}
        )

        # save() returns the _id, in this case 5
        self.assertEqual(5, result)

    @async_test_engine()
    def test_save_without_id(self):
        result = yield async.Op(
            self.async_connection().test.test_collection.save,
            {'fiddle': 'faddle'}
        )

        # save() returns the new _id
        self.assertTrue(isinstance(result, ObjectId))

    @async_test_engine()
    def test_save_bad(self):
        """
        Violate a unique index, make sure we handle error well
        """
        result = yield AssertRaises(
            DuplicateKeyError,
            self.async_connection().test.test_collection.save,
            {'_id': 5, 's': hex(4)} # There's already a document with s: hex(4)
        )

        self.assertEqual(None, result)

    @async_test_engine()
    def test_save_multiple(self):
        """
        TODO: what are we testing here really?
        """
        cx = self.async_connection()

        for i in range(10):
            cx.test.test_collection.save(
                {'_id': i + 500, 's': hex(i + 500)},
                callback=(yield tornado.gen.Callback(key=i))
            )

        results = yield async.WaitAllOps(range(10))

        # TODO: doc that result, error are in result.args for all gen.engine
        # yields

        # Once all saves complete, results will be a list of _id's, from 500 to
        # 509, but not necessarily in that order since we're async.
        self.assertEqual(range(500, 510), sorted(results))

    @async_test_engine()
    def test_remove(self):
        """
        Remove a document twice, check that we get a success response first time
        and an error the second time.
        """
        cx = self.async_connection()
        result = yield async.Op(cx.test.test_collection.remove, {'_id': 1})

        # First time we remove, n = 1
        self.assertEventuallyEqual(1, lambda: result['n'])
        self.assertEventuallyEqual(1, lambda: result['ok'])
        self.assertEventuallyEqual(None, lambda: result['err'])

        result = yield async.Op(cx.test.test_collection.remove, {'_id': 1})

        # Second time, document is already gone, n = 0
        self.assertEventuallyEqual(0, lambda: result['n'])
        self.assertEventuallyEqual(1, lambda: result['ok'])
        self.assertEventuallyEqual(None, lambda: result['err'])

    def test_remove_callback(self):
        cx = self.async_connection()
        self.check_callback_handling(
            functools.partial(cx.test.test_collection.remove, {})
        )

    @async_test_engine()
    def test_unsafe_remove(self):
        """
        Test that unsafe removes with no callback still work
        """
        self.assertEqual( 1, self.sync_coll.find({'_id': 117}).count(),
            msg="Test setup should have a document with _id 117")

        coll = self.async_connection().test.test_collection
        yield async.Op(coll.remove, {'_id': 117})
        self.assertEqual(
            0, second=(yield async.Op(coll.find({'_id': 117}).count))
        )

    def test_unsafe_insert(self):
        """
        Test that unsafe inserts with no callback still work
        """
        # id 201 not present
        self.assertEqual(0, self.sync_coll.find({'_id': 201}).count())

        # insert id 201 without a callback or safe=True
        self.async_connection().test.test_collection.insert({'_id': 201})

        # the insert is eventually executed
        self.assertEventuallyEqual(
            1,
            lambda: len(list(self.sync_db.test_collection.find({'_id': 201})))
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_unsafe_save(self):
        """
        Test that unsafe saves with no callback still work
        """
        self.async_connection().test.test_collection.save({'_id': 201})

        self.assertEventuallyEqual(
            1,
            lambda: len(list(self.sync_db.test_collection.find({'_id': 201})))
        )

        tornado.ioloop.IOLoop.instance().start()

    @async_test_engine()
    def test_command(self):
        cx = self.async_connection()
        result = yield async.Op(cx.admin.command, "buildinfo")
        self.assertEqual(int, type(result['bits']))

    def test_command_callback(self):
        cx = self.async_connection()
        self.check_callback_handling(
            functools.partial(cx.admin.command, 'buildinfo', check=False)
        )

    def test_nested_callbacks(self):
        cx = self.async_connection()
        results = [0]

        def callback(result, error):
            if error:
                raise error

            if not result:
                # Done iterating
                return

            results[0] += 1
            if results[0] < 1000:
                cx.test.test_collection.find(
                    {'_id': 1},
                    {'s': False},
                ).each(callback)

        cx.test.test_collection.find(
            {'_id': 1},
            {'s': False},
        ).each(callback)

        self.assertEventuallyEqual(
            [1000],
            lambda: results,
            timeout_sec=30
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_nested_callbacks_2(self):
        cx = async.TornadoConnection(host, port)
        results = []

        def connected(cx, error):
            if error:
                raise error

            cx.pymongo_ssl_test.test.insert({'_id': 201}, callback=inserted)

        def inserted(result, error):
            if error:
                raise error

            cx.pymongo_ssl_test.test.find_one({'_id': 201}, callback=found)

        def found(result, error):
            if error:
                raise error

            cx.pymongo_ssl_test.test.remove({'_id': 201}, callback=removed)

        def removed(result, error):
            results.append('done')

        cx.open(connected)

        self.assertEventuallyEqual(
            ['done'],
            lambda: results
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_get_last_error(self):
        """
        Create a unique index on 'x', insert the same value for x twice,
        assert DuplicateKeyError is passed to callback for second insert.
        Try again in a request, with an unsafe insert followed by an explicit
        call to database.error(), which raises InvalidOperation because we
        can't make two concurrent operations in a request. Finally, insert
        unsafely and call error() again, check that we get the getLastError
        result correctly, which checks that we're using a single socket in the
        request as expected.
        """
        # TODO: test that ensure_index calls the callback even if the index
        # is already created and in the index cache - might be a special-case
        # optimization

        # Use a special collection for this test
        sync_coll = self.sync_db.test_get_last_error
        sync_coll.drop()
        cx = self.async_connection()
        coll = cx.test.test_get_last_error

        results = []

        def ensured_index(result, error):
            if error:
                raise error

            results.append(result)
            coll.insert({'x':1}, callback=inserted1)

        def inserted1(result, error):
            if error:
                raise error

            results.append(result)
            coll.insert({'x':1}, callback=inserted2)

        def inserted2(result, error):
            self.assert_(isinstance(error, DuplicateKeyError))
            results.append(result)

            with cx.start_request():
                coll.insert(
                    {'x':1},
                    safe=False,
                    callback=inserted3
                )

        def inserted3(result, error):
            # No error, since we passed safe=False to insert()
            self.assertEqual(None, error)
            results.append(result)

            # We're still in the request begun in inserted2
            cx.test.error(callback=on_get_last_error)

        def on_get_last_error(result, error):
            if error:
                # This is unexpected -- Motor raised an exception trying to
                # execute getLastError on the server
                raise error

            results.append(result)

        # start the sequence of callbacks
        cx.test.test_get_last_error.ensure_index(
            [('x', 1)], unique=True, callback=ensured_index
        )

        # index name
        self.assertEventuallyEqual('x_1', lambda: results[0])

        # result of first insert
        self.assertEventuallyEqual(
            True,
            lambda: isinstance(results[1], ObjectId)
        )

        # result of second insert - failed with DuplicateKeyError
        self.assertEventuallyEqual(None, lambda: results[2])

        # result of third insert - failed, but safe=False
        self.assertEventuallyEqual(
            True,
            lambda: isinstance(results[3], ObjectId)
        )

        # result of error()
        self.assertEventuallyEqual(
            11000,
            lambda: results[4]['code']
        )

        tornado.ioloop.IOLoop.instance().start()
        self.sync_db.test_get_last_error.drop()

    @async_test_engine()
    def test_get_last_error_gen(self):
        # Same as test_get_last_error, but using tornado.gen
        cx = self.async_connection()
        coll = cx.text.test_get_last_error
        yield async.Op(coll.drop)

        result = yield async.Op(
            coll.ensure_index, [('x', 1)], unique=True
        )
        self.assertEqual('x_1', result)

        result = yield async.Op(coll.insert, {'x':1})
        self.assertTrue(isinstance(result, ObjectId))

        yield AssertRaises(DuplicateKeyError, coll.insert, {'x':1})

        with cx.start_request():
            result = yield async.Op(
                coll.insert,
                    {'x':1},
                safe=False
            )

            # insert failed, but safe=False so it returned the
            # driver-generated _id
            self.assertTrue(isinstance(result, ObjectId))

            # We're still in the request, so getLastError will work
            result = yield async.Op(cx.test.error)
            self.assertEqual(11000, result['code'])

        yield async.Op(coll.drop)

    def test_no_concurrent_ops_in_request(self):
        # Check that an attempt to do two things at once in a request raises
        # InvalidOperation
        results = []
        cx = self.async_connection()

        def inserted(result, error):
            results.append({
                'result': result,
                'error': error,
            })

        with cx.start_request():
            cx.test.test_collection.insert({})
            cx.test.test_collection.insert({}, callback=inserted)

        self.assertEventuallyEqual(
            None,
            lambda: results[0]['result']
        )

        self.assertEventuallyEqual(
            True,
            lambda: isinstance(results[0]['error'], InvalidOperation)
        )

        tornado.ioloop.IOLoop.instance().start()

    def _test_request(self, chain0_in_request, chain1_in_request):
        # Sequence:
        # We have two chains of callbacks, chain0 and chain1. A chain is a
        # sequence of callbacks, each spawned by the previous callback on the
        # chain. We test the following sequence:
        #
        # 0.00 sec: chain0 makes a bad insert
        # 0.25 sec: chain1 makes a good insert
        # 0.50 sec: chain0 checks getLastError
        # 0.75 sec: chain1 checks getLastError
        # 1.00 sec: IOLoop stops
        #
        # If start_request() works, then chain 0 gets the DuplicateKeyError
        # when it runs in a request, and neither chain gets the error when
        # they run with no request.
        gap_seconds = 0.25
        cx = self.async_connection()
        loop = tornado.ioloop.IOLoop.instance()

        # Results for chain 0 and chain 1
        results = {
            0: [],
            1: [],
        }

        def insert(chain_num, use_request, doc):
            request = None
            if use_request:
                request = cx.start_request()
                request.__enter__()

            # Perhaps causes DuplicateKeyError, depending on doc
            cx.test.test_collection.insert(doc)
            loop.add_timeout(
                datetime.timedelta(seconds=2*gap_seconds),
                lambda: inserted(chain_num)
            )

            if use_request:
                request.__exit__(None, None, None)

        def inserted(chain_num):
            cb = lambda result, error: got_error(chain_num, result, error)
            cx.test.error(callback=cb)

        def got_error(chain_num, result, error):
            if error:
                raise error

            results[chain_num].append(result)

        # Start chain 0. Causes DuplicateKeyError.
        insert(chain_num=0, use_request=chain0_in_request, doc={'s': hex(4)})

        # Start chain 1, 0.25 seconds from now. Succeeds: no error on insert.
        loop.add_timeout(
            datetime.timedelta(seconds=gap_seconds),
            lambda: insert(
                chain_num=1, use_request=chain1_in_request, doc={'s': hex(201)}
            )
        )

        loop.add_timeout(datetime.timedelta(seconds=4*gap_seconds), loop.stop)
        loop.start()
        return results

    def test_start_request(self):
        # getLastError works correctly only chain 0 is in a request
        results = self._test_request(True, False)
        self.assertEqual(11000, results[0][0]['code'])
        self.assertEqual([None], results[1])

    def test_start_request2(self):
        # getLastError works correctly when *both* chains are in requests
        results = self._test_request(True, True)
        self.assertEqual(11000, results[0][0]['code'])
        self.assertEqual([None], results[1])

    def test_no_start_request(self):
        # getLastError didn't get the error: chain0 and chain1 used the
        # same socket, so chain0's getLastError was checking on chain1's
        # insert, which had no error.
        results = self._test_request(False, False)
        self.assertEqual([None], results[0])
        self.assertEqual([None], results[1])

    def test_no_start_request2(self):
        # getLastError didn't get the error: chain0 and chain1 used the
        # same socket, so chain0's getLastError was checking on chain1's
        # insert, which had no error.
        results = self._test_request(False, True)
        self.assertEqual([None], results[0])
        self.assertEqual([None], results[1])

    def test_timeout(self):
        # Launch two slow find_ones. The one with a timeout should get an error
        loop = tornado.ioloop.IOLoop.instance()
        no_timeout = self.async_connection()
        timeout = self.async_connection(socketTimeoutMS=100)

        results = []
        query = {
            '$where': delay(0.5),
            '_id': 1,
        }

        def callback(result, error):
            results.append({'result': result, 'error': error})

        no_timeout.test.test_collection.find_one(query, callback=callback)
        timeout.test.test_collection.find_one(query, callback=callback)

        self.assertEventuallyEqual(
            True,
            lambda: isinstance(
                results[0]['error'],
                pymongo.errors.AutoReconnect
            )
        )

        self.assertEventuallyEqual(
            {'_id':1, 's':hex(1)},
            lambda: results[1]['result']
        )

        loop.start()

        # Make sure the delay completes before we call tearDown() and try to
        # drop the collection
        time.sleep(0.5)

    @async_test_engine()
    def test_auto_ref_and_deref(self):
        # Test same functionality as in PyMongo's test_database.py; the impl
        # for async is a little complex so we should test that it works here,
        # and not just rely on run_synchronous_tests to cover it.
        cx = self.async_connection()
        db = cx.test

        # We test a special hack where add_son_manipulator corrects our mistake
        # if we pass a TornadoDatabase, instead of Database, to AutoReference.
        db.add_son_manipulator(AutoReference(db))
        db.add_son_manipulator(NamespaceInjector())

        a = {"hello": u"world"}
        b = {"test": a}
        c = {"another test": b}

        yield async.Op(db.a.remove, {})
        yield async.Op(db.b.remove, {})
        yield async.Op(db.c.remove, {})
        yield async.Op(db.a.save, a)
        yield async.Op(db.b.save, b)
        yield async.Op(db.c.save, c)
        a["hello"] = "mike"
        yield async.Op(db.a.save, a)
        result_a = yield async.Op(db.a.find_one)
        result_b = yield async.Op(db.b.find_one)
        result_c = yield async.Op(db.c.find_one)

        self.assertEqual(a, result_a)
        self.assertEqual(a, result_b["test"])
        self.assertEqual(a, result_c["another test"]["test"])
        self.assertEqual(b, result_b)
        self.assertEqual(b, result_c["another test"])
        self.assertEqual(c, result_c)

    @async_test_engine()
    def test_max_pool_size_validation(self):
        cx = async.TornadoConnection(host=host, port=port, max_pool_size=-1)
        yield AssertRaises(ConfigurationError, cx.open)

        cx = async.TornadoConnection(host=host, port=port, max_pool_size='foo')
        yield AssertRaises(ConfigurationError, cx.open)

        c = async.TornadoConnection(host=host, port=port, max_pool_size=100)
        yield async.Op(c.open)
        self.assertEqual(c.max_pool_size, 100)

    def test_pool_request(self):
        # 1. Create a connection
        # 2. Get two sockets while keeping refs to both, check they're different
        # 3. Dereference both sockets, check they're reclaimed by pool
        # 4. Get a socket in a request
        # 5. Get a socket not in a request, check different
        # 6. Get another socket in request, check we get InvalidOperation (no
        #   concurrent ops in request)
        # 7. Dereference request socket, check it's reclaimed by pool
        # 8. Get two sockets, once in request and once not, check different
        # 9. Check that second socket in request is same as first
        
        # 1.
        async.socket_uuid = True
        cx = self.async_connection(max_pool_size=17)
        cx_pool = cx.delegate._Connection__pool
        loop = tornado.ioloop.IOLoop.instance()

        # Connection has needed one socket so far to call isMaster
        self.assertEqual(1, len(cx_pool.sockets))
        self.assertFalse(cx_pool.in_request())

        def get_socket():
            # Weirdness in PyMongo, which I hope will be fixed soon: Connection
            # does pool.get_socket(pair), while ReplicaSetConnection initializes
            # pool with pair and just does pool.get_socket(). We're using
            # Connection so we have to emulate its call.
            return cx_pool.get_socket((host, port))

        get_socket = async.asynchronize(get_socket, False, True)

        def socket_ids():
            return [sock_info.sock.uuid for sock_info in cx_pool.sockets]

        # We need a place to keep refs to sockets so they're not reclaimed
        # before we're ready
        socks = {}
        results = set()

        # 2.
        def got_sock0(sock, error):
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            socks[0] = sock
            if 1 in socks:
                # got_sock1 has also run; let this callback finish so its refs
                # are deleted
                loop.add_callback(check_socks_different_and_reclaimed0)

        def got_sock1(sock, error):
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            socks[1] = sock
            if 0 in socks:
                # got_sock0 has also run; let this callback finish so its refs
                # are deleted
                loop.add_callback(check_socks_different_and_reclaimed0)

        # 3.
        def check_socks_different_and_reclaimed0():
            self.assertNotEqual(socks[0], socks[1])
            id_0, id_1 = socks[0].sock.uuid, socks[1].sock.uuid
            del socks[0]
            del socks[1]
            self.assertTrue(id_0 in socket_ids())
            self.assertTrue(id_1 in socket_ids())

            results.add('step3')
            
            # 4.
            with cx.start_request():
                get_socket(callback=got_sock2)

            # 5.
            get_socket(callback=got_sock3)

        sock2_id = [None]
        def got_sock2(sock, error):
            # Get request socket
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            self.assertTrue(cx_pool.in_request())
            socks[2] = sock
            sock2_id[0] = sock.sock.uuid
            if 3 in socks:
                # got_sock3 has also run
                loop.add_callback(check_socks_different_and_reclaimed1)

            # We're in a request in this function, so test step 6, after
            # check_socks_different_and_reclaimed1 has run.
            loop.add_timeout(
                time.time() + 0.25,
                lambda: get_socket(callback=check_invalid_op))

        def got_sock3(sock, error):
            # Get NON-request socket
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            self.assertFalse(cx_pool.in_request())
            socks[3] = sock
            if 2 in socks:
                # got_sock2 has also run
                loop.add_callback(check_socks_different_and_reclaimed1)

        def check_socks_different_and_reclaimed1():
            self.assertNotEqual(socks[2], socks[3])
            id_2, id_3 = socks[2].sock.uuid, socks[3].sock.uuid
            del socks[2]
            del socks[3]

            # sock 2 is the request socket, it hasn't been reclaimed yet
            # because we still have check_invalid_op() pending in that request
            self.assertFalse(id_2 in socket_ids())

            # sock 3 is done and it's been reclaimed
            self.assertTrue(id_3 in socket_ids())
            results.add('step5')

        # 6.
        def check_invalid_op(result, error):
            self.assertEqual(None, result)
            self.assertTrue(isinstance(error, InvalidOperation))
            self.assertTrue(cx_pool.in_request())
            results.add('step6')

            # 7.
            self.assertFalse(sock2_id[0] in socket_ids())

            # Schedule a callback *not* in a request
            with tornado.stack_context.NullContext():
                loop.add_callback(check_request_sock_reclaimed)

        def check_request_sock_reclaimed():
            self.assertFalse(cx_pool.in_request())

            # TODO: I can't figure out how to force GC reliably here
#            self.assertTrue(
#                sock2_id[0] in socket_ids(),
#                "Request socket not reclaimed by pool")

            results.add('step7')
            
            # 8.
            get_socket(callback=got_sock4)
            
            with cx.start_request():
                get_socket(callback=got_sock5)

        def got_sock4(sock, error):
            self.assertFalse(cx_pool.in_request())
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            socks[4] = sock
            if 5 in socks:
                # got_sock5 has also run
                check_socks_different()

        sock5_id = [None]
        def got_sock5(sock, error):
            self.assertTrue(cx_pool.in_request())
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))

            socks[5] = sock
            sock5_id[0] = sock.sock.uuid

            if 4 in socks:
                # got_sock4 has also run
                check_socks_different()

            # 9.
            get_socket(callback=got_sock6)

        def check_socks_different():
            self.assertNotEqual(socks[4], socks[5])

            # sock 5 is the request socket
            id_4 = socks[4].sock.uuid
            sock5_id[0] = socks[5].sock.uuid

            del socks[4]
            del socks[5]

            # sock 4 is done and it's been reclaimed
            self.assertTrue(id_4 in socket_ids())

            # sock 5 still in request
            self.assertFalse(sock5_id[0] in socket_ids())
            results.add('step8')

        def got_sock6(sock, error):
            self.assertTrue(cx_pool.in_request())
            self.assertTrue(isinstance(sock, pymongo.pool.SocketInfo))
            self.assertEqual(sock5_id[0], sock.sock.uuid)
            results.add('step9')

        # Knock over the first domino.
        get_socket(callback=got_sock0)
        get_socket(callback=got_sock1)

        for step in (3, 5, 6, 7, 8, 9):
            self.assertEventuallyEqual(
                True, lambda: 'step%s' % step in results)

        loop.start()


class TornadoSSLTest(TornadoTest):
    def test_no_ssl(self):
        if have_ssl:
            raise SkipTest(
                "We have SSL compiled into Python, can't test what happens "
                "without SSL"
            )

        self.assertRaises(ConfigurationError,
            async.TornadoConnection, host, port, ssl=True)
        # TODO: test same thing with async ReplicaSetConnection
#        self.assertRaises(ConfigurationError,
#            ReplicaSetConnection, ssl=True)

    def test_simple_ops(self):
        """
        TODO: this is duplicative of test_nested_callbacks_2
        """
        if not have_ssl:
            raise SkipTest()

        loop = tornado.ioloop.IOLoop.instance()
        cx = async.TornadoConnection(host, port, connectTimeoutMS=100, ssl=True)

        def connected(cx, error):
            if error:
                raise error

            cx.pymongo_ssl_test.test.insert({'ssl': True}, callback=inserted)

        def inserted(result, error):
            if error:
                raise error

            cx.pymongo_ssl_test.test.find_one(callback=found)

        def found(result, error):
            if error:
                raise error

            loop.stop()
            cx.drop_database('pymongo_ssl_test')

        cx.open(connected)
        loop.start()

# TODO: test that save and insert are async somehow? MongoDB's global
#     write lock makes this hard. Maybe with two mongod instances.
# TODO: test master/slave, shard, replica set
# TODO: replicate asyncmongo's whole test suite?
# TODO: apply pymongo's whole suite to async
# TODO: don't use the 'test' database, use something that will play nice w/
#     Jenkins environment
# TODO: test SSL, I don't think my call to ssl.wrap_socket() in TornadoSocket is
#     right
# TODO: check that sockets are returned to pool, or closed, or something
# TODO: test unsafe remove
# TODO: test deeply-nested callbacks
# TODO: separate these into some class-specific modules a la PyMongo

if __name__ == '__main__':
    unittest.main()

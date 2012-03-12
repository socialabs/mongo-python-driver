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
from pymongo.objectid import ObjectId
from pymongo.tornado_async import async
from pymongo.son_manipulator import AutoReference, NamespaceInjector
from pymongo.errors import InvalidOperation, ConfigurationError, \
    DuplicateKeyError

from test.utils import delay

import tornado.ioloop

# Tornado testing tools
from pymongo.tornado_async.test_tornado_async import eventually, puritanical

# TODO: sphinx-compat?
# TODO: test map_reduce and inline_map_reduce


host = os.environ.get("DB_IP", "localhost")
port = int(os.environ.get("DB_PORT", 27017))


class TornadoTest(
    puritanical.PuritanicalTest,
    eventually.AssertEventuallyTest
):
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

        called = {'callback': False}

        def callback(connection, error):
            if error:
                raise error

            called['callback'] = True

        self.assertEventuallyEqual(
            True,
            lambda: cx.connected
        )

        self.assertEventuallyEqual(
            True,
            lambda: called['callback']
        )

        cx.open(callback)
        tornado.ioloop.IOLoop.instance().start()

    def test_connection_callback(self):
        cx = async.TornadoConnection(host, port)
        self.check_callback_handling(cx.open)
        
    def test_dotted_collection_name(self):
        # Ensure that remove, insert, and find work on collections with dots
        # in their names.
        cx = self.async_connection()
        for coll in (
            cx.test.foo,
            cx.test.foo.bar,
            cx.test.foo.bar.baz.quux
        ):
            results = []

            def removed1(result, error):
                if error:
                    raise error

                results.append('removed1')
                coll.insert({'_id':'xyzzy'}, callback=inserted)

            def inserted(result, error):
                if error:
                    raise error

                results.append(result)
                coll.find_one({'_id':'xyzzy'}, callback=found1)

            def found1(result, error):
                if error:
                    raise error

                results.append(result)
                coll.remove(callback=removed2)

            def removed2(result, error):
                if error:
                    raise error

                results.append('removed2')
                coll.find_one({'_id':'xyzzy'}, callback=found2)

            def found2(result, error):
                if error:
                    raise error

                results.append(result)

            self.assertEventuallyEqual(
                ['removed1', 'xyzzy', {'_id':'xyzzy'}, 'removed2', None],
                lambda: results
            )

            coll.remove(callback=removed1)

            tornado.ioloop.IOLoop.instance().start()

    def test_cursor(self):
        cx = self.async_connection()
        coll = cx.test.foo
        # We're not actually running the find(), so null callback is ok
        cursor = coll.find()
        self.assert_(isinstance(cursor, async.TornadoCursor))
        self.assertFalse(cursor.started, "Cursor shouldn't start immediately")

    def test_find(self):
        results = []

        # Although in most tests I'm using the synchronized
        # self.async_connection() for convenience, here I'll really test
        # passing a callback to open() that does a find(), just to make sure
        # that works in the Christian Kvalheim Node.js-driver style.
        def connected(connection, error):
            if error:
                raise error

            def each(doc, error):
                if error:
                    raise error

                if doc:
                    results.append(doc['_id'])

                    # 'cursor' should still be open
                    self.assertEqual(
                        1 + self.open_cursors,
                        self.get_open_cursors()
                    )

            def got_more(result, error):
                if error:
                    raise error

                results.append(result)
                if len(results) >= 10:
                    # cursor should be closed by now
                    expected_cursors = self.open_cursors
                else:
                    expected_cursors = self.open_cursors + 1

                actual_open_cursors = self.get_open_cursors()

                self.assertEqual(
                    expected_cursors,
                    actual_open_cursors,
                    "Expected %d open cursors when there are %d "
                    "results, found %d" % (
                        expected_cursors, len(results), actual_open_cursors
                    )
                )

                # Get next batch
                if cursor.alive:
                    cursor.get_more(got_more)
                else:
                    self.assertRaises(
                        InvalidOperation,
                        lambda: cursor.get_more(got_more)
                    )

            # test_collection has 200 docs with _id from 0 to 199, from setUp().
            # Get those with _id from 0 to 14 in batches 0-4, 5-9, 10-14.
            connection.test.test_collection.find(
                {'_id': {'$lt':15}},
                {'s': False}, # exclude 's' field
                sort=[('_id', 1)],
                batch_size=5
            ).each(each)

        async.TornadoConnection(host, port).open(callback=connected)

        self.assertEventuallyEqual(
            range(15),
            lambda: results
        )

        tornado.ioloop.IOLoop.instance().start()

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
        # Make a big unindexed collection that will take a long time to query
        self.sync_db.drop_collection('big_coll')
        self.sync_db.big_coll.insert([
            {'s': hex(s)} for s in range(10000)
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

        # This find() takes 1 second
        loop.add_timeout(
            now + 0.1,
            lambda: cx.test.test_collection.find(
                {'_id': 1, '$where': delay(1)},
                fields={'s': True, '_id': False},
            ).each(callback)
        )

        # Very fast lookup
        loop.add_timeout(
            now + 0.2,
            lambda: cx.test.test_collection.find(
                {'_id': 2},
                fields={'s': True, '_id': False},
            ).each(callback)
        )

        # Find {'i': 3} in big_coll -- even though there's only one such record,
        # MongoDB will have to scan the whole table to know that. We expect this
        # to be faster than 1 second (the $where clause above) and slower than
        # the indexed lookup above.
        loop.add_timeout(
            now + 0.3,
            lambda: cx.test.big_coll.find(
                {'s': hex(3)},
                fields={'s': True, '_id': False},
            ).each(callback)
        )

        # Results were appended in order 2, 3, 1
        self.assertEventuallyEqual(
            [{'s': hex(s)} for s in (2, 3, 1)],
            lambda: results,
            timeout_sec=1.3
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

    def test_find_one(self):
        results = []
        def callback(result, error):
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.find_one(
            {'_id': 1},
            callback=callback
        )

        self.assertEventuallyEqual(
            # Not a list of docs, find_one() returns the doc itself
            {'_id': 1, 's': hex(1)},
            lambda: results[0]
        )

        tornado.ioloop.IOLoop.instance().start()

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

    def test_update(self):
        results = []

        def callback(result, error):
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.update(
            {'_id': 5},
            {'$set': {'foo': 'bar'}},
            callback=callback,
        )

        self.assertEventuallyEqual(1, lambda: results[0]['ok'])
        self.assertEventuallyEqual(True, lambda: results[0]['updatedExisting'])
        self.assertEventuallyEqual(1, lambda: results[0]['n'])
        self.assertEventuallyEqual(None, lambda: results[0]['err'])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_update_bad(self):
        """
        Violate a unique index, make sure we handle error well
        """
        results = []

        def callback(result, error):
            self.assert_(isinstance(error, DuplicateKeyError))
            self.assertEqual(None, result)
            results.append(result)

        self.async_connection().test.test_collection.update(
            {'_id': 5},
            {'$set': {'s': hex(4)}}, # There's already a document with s: hex(4)
            callback=callback,
        )

        self.assertEventuallyEqual(None, lambda: results[0])
        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_update_callback(self):
        cx = self.async_connection()
        self.check_callback_handling(
            functools.partial(cx.test.test_collection.update, {}, {})
        )

    def test_insert(self):
        results = []

        def callback(result, error):
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.insert(
            {'_id': 201},
            callback=callback,
        )

        # insert() returns new _id
        self.assertEventuallyEqual(201, lambda: results[0])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_insert_many(self):
        results = []

        def callback(result, error):
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.insert(
            [{'_id': i, 's': hex(i)} for i in range(201, 211)],
            callback=callback,
        )

        self.assertEventuallyEqual(range(201, 211), lambda: results[0])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_insert_bad(self):
        """
        Violate a unique index, make sure we handle error well
        """
        results = []

        def callback(result, error):
            self.assert_(isinstance(error, DuplicateKeyError))
            self.assertEqual(None, result)
            results.append(result)

        self.async_connection().test.test_collection.insert(
            {'s': hex(4)}, # There's already a document with s: hex(4)
            callback=callback,
        )

        self.assertEventuallyEqual(None, lambda: results[0])
        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_insert_many_one_bad(self):
        """
        Violate a unique index in one of many updates, make sure we handle error
        well
        """
        results = []

        def callback(result, error):
            self.assert_(isinstance(error, DuplicateKeyError))
            results.append(result)

        self.async_connection().test.test_collection.insert(
            [
                {'_id': 201, 's': hex(201)},
                {'_id': 202, 's': hex(4)}, # Already exists
                {'_id': 203, 's': hex(203)},
            ],
            callback=callback,
        )


        # In async, even though first insert succeeded, result is None
        self.assertEventuallyEqual(None, lambda: results[0])

        tornado.ioloop.IOLoop.instance().start()

        self.assertEqual(1, len(results))

        # First insert should've succeeded
        self.assertEqual(
            [{'_id': 201, 's': hex(201)}],
            list(self.sync_db.test_collection.find({'_id': 201}))
        )

        # Final insert didn't execute, since second failed
        self.assertEqual(
            [],
            list(self.sync_db.test_collection.find({'_id': 203}))
        )

    def test_save_callback(self):
        cx = self.async_connection()
        self.check_callback_handling(
            functools.partial(cx.test.test_collection.save, {})
        )

    def test_save_with_id(self):
        results = []

        def callback(result, error):
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.save(
            {'_id': 5},
            callback=callback,
        )

        # save() returns the _id, in this case 5
        self.assertEventuallyEqual(5, lambda: results[0])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_save_without_id(self):
        results = []

        def callback(result, error):
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.save(
            {'fiddle': 'faddle'},
            callback=callback,
        )

        # save() returns the new _id
        self.assertEventuallyEqual(
            True,
            lambda: isinstance(results[0], ObjectId)
        )

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_save_bad(self):
        """
        Violate a unique index, make sure we handle error well
        """
        results = []

        def callback(result, error):
            self.assert_(isinstance(error, DuplicateKeyError))
            self.assertEqual(None, result)
            results.append(result)

        self.async_connection().test.test_collection.save(
            {'_id': 5, 's': hex(4)}, # There's already a document with s: hex(4)
            callback=callback,
        )

        self.assertEventuallyEqual(None, lambda: results[0])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_save_multiple(self):
        """
        TODO: what are we testing here really?
        """
        results = []

        def callback(result, error):
            if error:
                raise error
            results.append(result)

        cx = self.async_connection()

        for i in range(10):
            cx.test.test_collection.save(
                {'_id': i + 500, 's': hex(i + 500)},
                callback=callback
            )

        # Once all saves complete, results will be a list of _id's, from 500 to
        # 509, but not necessarily in that order since we're async.
        self.assertEventuallyEqual(
            range(500, 510),
            lambda: sorted(results)
        )

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(10, len(results))

    def test_remove(self):
        """
        Remove a document twice, check that we get a success response first time
        and an error the second time.
        """
        results = []
        def callback(result, error):
            if error:
                raise error
            results.append(result)

        cx = self.async_connection()
        cx.test.test_collection.remove(
            {'_id': 1},
            callback=callback
        )

        tornado.ioloop.IOLoop.instance().add_timeout(
            0.1,
            lambda: cx.test.test_collection.remove(
                {'_id': 1},
                callback=callback
            )
        )

        # First time we remove, n = 1
        self.assertEventuallyEqual(1, lambda: results[0]['n'])
        self.assertEventuallyEqual(1, lambda: results[0]['ok'])
        self.assertEventuallyEqual(None, lambda: results[0]['err'])

        # Second time, document is already gone, n = 0
        self.assertEventuallyEqual(0, lambda: results[1]['n'])
        self.assertEventuallyEqual(1, lambda: results[1]['ok'])
        self.assertEventuallyEqual(None, lambda: results[1]['err'])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(2, len(results))

    def test_remove_callback(self):
        cx = self.async_connection()
        self.check_callback_handling(
            functools.partial(cx.test.test_collection.remove, {})
        )

    def test_unsafe_remove(self):
        """
        Test that unsafe removes with no callback still work
        """
        self.assertEqual(
            1,
            self.sync_coll.find({'_id': 117}).count(),
            "Test setup should have a document with _id 117"
        )

        self.async_connection().test.test_collection.remove({'_id': 117})

        self.assertEventuallyEqual(
            0,
            lambda: len(list(self.sync_db.test_collection.find({'_id': 117})))
        )

        tornado.ioloop.IOLoop.instance().start()
        
    def test_unsafe_insert(self):
        """
        Test that unsafe inserts with no callback still work
        """
        # id 201 not present
        self.assertEqual(0, self.sync_coll.find({'_id': 201}).count())

        # insert id 201 without a callback or safe=True
        self.async_connection().test.test_collection.insert({'_id': 201})

        # the insert is executed
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

    def test_command(self):
        cx = self.async_connection()
        results = []

        def callback(result, error):
            if error:
                raise error
            results.append(result)

        cx.admin.command("buildinfo", callback=callback)

        self.assertEventuallyEqual(
            int,
            lambda: type(results[0]['bits'])
        )

        tornado.ioloop.IOLoop.instance().start()

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
        # Use a special collection for this test
        self.sync_db.test_get_last_error.drop()
        cx = self.async_connection()
        results = []

        def ensured_index(result, error):
            if error:
                raise error

            results.append(result)
            cx.test.test_get_last_error.insert({'x':1}, callback=inserted1)

        def inserted1(result, error):
            if error:
                raise error

            results.append(result)
            cx.test.test_get_last_error.insert({'x':1}, callback=inserted2)

        def inserted2(result, error):
            self.assert_(isinstance(error, DuplicateKeyError))
            results.append(result)

            with cx.start_request():
                cx.test.test_get_last_error.insert(
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
        # TODO: test that ensure_index calls the callback even if the index
        # is already created and in the index cache - might be a special-case
        # optimization
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

    def test_auto_ref_and_deref(self):
        # Test same functionality as in PyMongo's test_database.py; the impl
        # for async is a little complex so we should test that it works here,
        # and not just rely on run_synchronous_tests to cover it.
        cx = self.async_connection()
        db = cx.test
        db.add_son_manipulator(AutoReference(db))
        db.add_son_manipulator(NamespaceInjector())

        a = {"hello": u"world"}
        b = {"test": a}
        c = {"another test": b}

        def step0():
            db.a.remove({}, callback=step1)

        def step1(result, error):
            if error:
                raise error
            db.b.remove({}, callback=step2)

        def step2(result, error):
            if error:
                raise error
            db.c.remove({}, callback=step3)

        def step3(result, error):
            if error:
                raise error
            db.a.save(a, callback=step4)

        def step4(result, error):
            if error:
                raise error
            db.b.save(b, callback=step5)

        def step5(result, error):
            if error:
                raise error
            db.c.save(c, callback=step6)

        def step6(result, error):
            if error:
                raise error
            a["hello"] = "mike"
            db.a.save(a, callback=step7)

        results = {}

        def step7(result, error):
            if error:
                raise error
            db.a.find_one(callback=step8)

        def step8(result, error):
            if error:
                raise error
            results['a'] = result
            db.b.find_one(callback=step9)

        def step9(result, error):
            if error:
                raise error
            results['b'] = result

        self.assertEventuallyEqual(a, lambda: results['a'])
        self.assertEventuallyEqual(a, lambda: results['b']["test"])
        self.assertEventuallyEqual(a, lambda: self.sync_db.c.find_one()["another test"]["test"])
        self.assertEventuallyEqual(b, lambda: self.sync_db.b.find_one())
        self.assertEventuallyEqual(b, lambda: self.sync_db.c.find_one()["another test"])
        self.assertEventuallyEqual(c, lambda: self.sync_db.c.find_one())

        step0()
        tornado.ioloop.IOLoop.instance().start()

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

if __name__ == '__main__':
    unittest.main()

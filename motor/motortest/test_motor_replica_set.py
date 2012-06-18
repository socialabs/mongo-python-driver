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

"""Test Motor, an asynchronous driver for MongoDB and Tornado."""

import functools
import socket
import unittest

import motor
if not motor.requirements_satisfied:
    from nose.plugins.skip import SkipTest
    raise SkipTest("Tornado or greenlet not installed")

from tornado import ioloop, iostream

from motor.motortest import (
    MotorTest, async_test_engine, AssertRaises, host, port)
import pymongo.errors
import pymongo.replica_set_connection
from test.test_replica_set_connection import TestConnectionReplicaSetBase


class MotorReplicaSetTest(MotorTest, TestConnectionReplicaSetBase):
    def setUp(self):
        # TODO: Make TestConnectionReplicaSetBase cooperative
        TestConnectionReplicaSetBase.setUp(self)
        MotorTest.setUp(self)

    @async_test_engine()
    def test_sync_connection(self):
        class DictSubclass(dict):
            pass

        args = ['localhost:27017']
        kwargs = dict(
            connectTimeoutMS=1000, socketTimeoutMS=1500, max_pool_size=23,
            document_class=DictSubclass, tz_aware=True, replicaSet='repl0')

        cx = yield motor.Op(motor.MotorReplicaSetConnection(
            *args, **kwargs).open)
        sync_cx = cx.sync_connection()
        self.assertTrue(isinstance(
            sync_cx, pymongo.replica_set_connection.ReplicaSetConnection))
        self.assertEqual(1000, sync_cx._ReplicaSetConnection__conn_timeout * 1000.0)
        self.assertEqual(1500, sync_cx._ReplicaSetConnection__net_timeout * 1000.0)
        self.assertEqual(23, sync_cx.max_pool_size)
        self.assertEqual(True, sync_cx._ReplicaSetConnection__tz_aware)
        self.assertEqual(DictSubclass, sync_cx._ReplicaSetConnection__document_class)

        # Make sure sync connection works
        self.assertEqual(
                {'_id': 5, 's': hex(5)},
            sync_cx.test.test_collection.find_one({'_id': 5}))

    @async_test_engine()
    def test_auto_reconnect_exception_when_read_preference_is_secondary(self):
        cx = motor.MotorReplicaSetConnection(
            'localhost:27017', replicaSet='repl0')

        yield motor.Op(cx.open)
        db = cx.pymongo_test

        def raise_socket_error(self, data, callback):
            ioloop.IOLoop.instance().add_callback(
                functools.partial(callback, None, socket.error('foo')))

        old_write = iostream.IOStream.write
        iostream.IOStream.write = raise_socket_error

        try:
            cursor = db.test.find(
                read_preference=pymongo.ReadPreference.SECONDARY)

            yield AssertRaises(pymongo.errors.AutoReconnect, cursor.each)
        finally:
            iostream.IOStream.write = old_write


if __name__ == '__main__':
    unittest.main()

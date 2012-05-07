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
import collections

import unittest

from tornado import ioloop
from nose.plugins.skip import SkipTest

import motor
import pymongo

from motor.motortest import (
    MotorTest, async_test_engine, host, port, host2, port2, host3, port3,
    AssertEqual, puritanical)
from pymongo.errors import (
    InvalidOperation, ConfigurationError, ConnectionFailure)


MSC = motor.MotorMasterSlaveConnection


class MotorMasterSlaveTest(MotorTest):
    def setUp(self):
        super(MotorMasterSlaveTest, self).setUp()

        self.sync_master = pymongo.connection.Connection(host, port)

        self.sync_slaves = []
        for slave_host, slave_port in [
            (host2, port2),
            (host3, port3),
        ]:
            try:
                slave = pymongo.connection.Connection(slave_host, slave_port,
                    read_preference=pymongo.ReadPreference.SECONDARY)
                self.sync_slaves.append(slave)
            except ConnectionFailure:
                pass

        if not self.sync_slaves:
            raise SkipTest("No slaves")

        self.sync_master.drop_database('pymongo_test')

    @async_test_engine()
    def test_master_slave(self):
        master = motor.MotorConnection(host, port)
        slaves = [
            motor.MotorConnection(slave.host, slave.port)
            for slave in self.sync_slaves
        ]

        # Must open connections before creating MSC
        self.assertRaises(pymongo.errors.InvalidOperation, MSC, master, slaves)

        yield motor.Op(master.open)
        yield motor.Op(slaves[0].open)
        cx = MSC(master, slaves)
        collection = cx.pymongo_test.test
        self.assertTrue(isinstance(collection, motor.MotorCollection))

        doc = {'asdf': 'barbazquux'}
        yield motor.Op(collection.insert, doc, w=1 + len(self.sync_slaves))
        yield AssertEqual(doc, collection.find_one)

    @async_test_engine()
    def test_pymongo_connections(self):
        cx = MSC(self.sync_master, self.sync_slaves)
        self.assertTrue(cx.connected)
        collection = cx.pymongo_test.test
        self.assertTrue(isinstance(collection, motor.MotorCollection))

        doc = {'asdf': 'barbazquux'}
        yield motor.Op(collection.insert, doc, w=1 + len(self.sync_slaves))
        yield AssertEqual(doc, collection.find_one)

    def test_custom_io_loop(self):
        loop = puritanical.PuritanicalIOLoop()

        @async_test_engine(io_loop=loop)
        def test(self):
            # Make sure we can do async things with the custom loop
            master = motor.MotorConnection(host, port, io_loop=loop)
            yield motor.Op(master.open)
            self.assertTrue(master.connected)

            slaves = [
                motor.MotorConnection(slave.host, slave.port, io_loop=loop)
                for slave in self.sync_slaves
            ]

            for slave in slaves:
                yield motor.Op(slave.open)
                self.assertTrue(slave.connected)

            # Test various combos of IOLoops
            std_loop_master = motor.MotorConnection(host, port)
            std_loop_master.open_sync()
            std_loop_slave = motor.MotorConnection(host2, port2)
            std_loop_slave.open_sync()

            self.assertRaises(ConfigurationError, MSC, std_loop_master, slaves)
            self.assertRaises(ConfigurationError, MSC, master, [std_loop_slave])
            self.assertRaises(ConfigurationError,
                MSC, master, slaves, io_loop=ioloop.IOLoop())

            # No error
            MSC(master, slaves, io_loop=loop)
            cx = MSC(master, slaves)

            # Try with the master-slave connection
            db = cx.pymongo_test
            self.assertTrue(isinstance(db, motor.MotorDatabase))
            self.assertEqual(loop, db.get_io_loop())

            collection = db.test
            self.assertTrue(isinstance(collection, motor.MotorCollection))
            self.assertEqual(loop, collection.get_io_loop())

            doc = {'asdf': 'barbazquux'}
            yield motor.Op(collection.insert, doc, w=1 + len(self.sync_slaves))
            yield AssertEqual(doc, collection.find_one)

            # Try with 'master' and 'slaves' properties
            self.assertEqual(loop, cx.master.io_loop)
            doc2 = {'fdsafdsa': 'q924hfa9s8h'}
            yield motor.Op(cx.master.pymongo_test.test.insert, doc2,
                w=1 + len(self.sync_slaves))

            self.assertEqual(len(self.sync_slaves), len(cx.slaves))
            for slave in cx.slaves:
                self.assertEqual(loop, slave.io_loop)
                yield AssertEqual(doc, slave.pymongo_test.test.find_one)

        test(self)

    def test_requests(self):
        cx = self.motor_connection(host, port)
        for method in 'start_request', 'in_request', 'end_request':
            self.assertRaises(NotImplementedError, getattr(cx, method))

    def test_types(self):
        self.assertRaises(TypeError, MSC, 1)
        self.assertRaises(TypeError, MSC, self.sync_master, 1)
        self.assertRaises(TypeError, MSC, self.sync_master, [1])

    def test_disconnect(self):
        disconnects = collections.defaultdict(lambda: 0)
        class DisconnectTracker(pymongo.connection.Connection):
            def disconnect(self):
                disconnects[self] += 1
            
        master = DisconnectTracker()
        slaves = [DisconnectTracker(), DisconnectTracker()]
        cx = MSC(master, slaves)
        disconnects.clear()
        cx.disconnect()
        self.assertEqual(1, disconnects[cx.master.delegate])
        self.assertEqual(1, disconnects[cx.slaves[0].delegate])
        self.assertEqual(1, disconnects[cx.slaves[1].delegate])

        
if __name__ == '__main__':
    unittest.main()

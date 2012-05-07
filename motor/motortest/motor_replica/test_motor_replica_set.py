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

"""Test replica set operations and failures."""

import time
import unittest
from tornado import gen

from tornado.ioloop import IOLoop

# TODO: this replset_tools is just a copy of those in PyMongo's test/replica/,
# since I'm not sure yet whether I can share directories with PyMongo or add
# an __init__.py to test/replica/; find a way to share.
import replset_tools

from pymongo import (ReplicaSetConnection,
                     ReadPreference)
from pymongo.connection import Connection, _partition_node
from pymongo.errors import AutoReconnect, ConnectionFailure

import motor
from motor.motortest import (
    MotorTest, async_test_engine, AssertRaises, AssertEqual)

from motor.motortest import eventually, puritanical


class MotorTestReadPreference(
    puritanical.PuritanicalTest,
    eventually.AssertEventuallyTest
):
    def setUp(self):
        super(MotorTestReadPreference, self).setUp()
        members = [{}, {}, {'arbiterOnly': True}]
        res = replset_tools.start_replica_set(members)
        self.seed, self.name = res

    @async_test_engine(timeout_sec=60)
    def test_read_preference(self):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetConnection(self.seed, replicaSet=self.name)
        c.open_sync()
        self.assertTrue(bool(len(c.secondaries)))
        db = c.pymongo_test
        yield motor.Op(db.test.remove, w=len(c.secondaries))

        # Force replication...
        w = len(c.secondaries) + 1
        yield motor.Op(db.test.insert, {'foo': 'bar'}, w=w)

        # Test direct connection to a secondary
        host, port = replset_tools.get_secondaries()[0].split(':')
        port = int(port)
        conn = motor.MotorConnection(host, port, slave_okay=True).open_sync()
        self.assertEqual(host, conn.host)
        self.assertEqual(port, conn.port)

        result = yield motor.Op(conn.pymongo_test.test.find_one)
        self.assertTrue(result)
        conn = motor.MotorConnection(host, port,
            read_preference=ReadPreference.SECONDARY).open_sync()
        self.assertEqual(host, conn.host)
        self.assertEqual(port, conn.port)
        result = yield motor.Op(conn.pymongo_test.test.find_one)
        self.assertTrue(result)

        # Test direct connection to an arbiter
        host = replset_tools.get_arbiters()[0]
        arbiter_connection = motor.MotorConnection(host)
        yield AssertRaises(ConnectionFailure, arbiter_connection.open)

        # Test PRIMARY
        for _ in xrange(10):
            cursor = db.test.find()
            yield motor.Op(cursor.next)
            self.assertEqual(cursor.delegate._Cursor__connection_id, c.primary)

        # Test SECONDARY with a secondary
        db.read_preference = ReadPreference.SECONDARY
        for _ in xrange(10):
            cursor = db.test.find()
            yield motor.Op(cursor.next)
            self.assertTrue(
                cursor.delegate._Cursor__connection_id in c.secondaries)

        # Test SECONDARY_ONLY with a secondary
        db.read_preference = ReadPreference.SECONDARY_ONLY
        for _ in xrange(10):
            cursor = db.test.find()
            yield motor.Op(cursor.next)
            self.assertTrue(cursor.delegate._Cursor__connection_id in c.secondaries)

        # Test SECONDARY with no secondary
        killed = replset_tools.kill_all_secondaries()
        yield gen.Task(loop.add_timeout, time.time() + 2)

        self.assertTrue(bool(len(killed)))
        db.read_preference = ReadPreference.SECONDARY
        for _ in xrange(10):
            cursor = db.test.find()
            result = yield motor.Op(cursor.next)
            self.assertEqual(cursor.delegate._Cursor__connection_id, c.primary)

        # Test SECONDARY_ONLY with no secondary
        db.read_preference = ReadPreference.SECONDARY_ONLY
        for _ in xrange(10):
            cursor = db.test.find()
            yield AssertRaises(AutoReconnect, cursor.next)

        replset_tools.restart_members(killed)

        # Test PRIMARY with no primary (should raise an exception)
        db.read_preference = ReadPreference.PRIMARY
        cursor = db.test.find()
        yield motor.Op(cursor.next)
        self.assertEqual(cursor.delegate._Cursor__connection_id, c.primary)
        killed = replset_tools.kill_primary()
        self.assertTrue(bool(len(killed)))
        yield gen.Task(loop.add_timeout, time.time() + 2)
        yield AssertRaises(AutoReconnect, db.test.find_one)

    def tearDown(self):
        super(MotorTestReadPreference, self).tearDown()
        replset_tools.kill_all_members()


class MotorTestPassiveAndHidden(
    puritanical.PuritanicalTest,
    eventually.AssertEventuallyTest
):
    def setUp(self):
        super(MotorTestPassiveAndHidden, self).setUp()
        members = [{}, {'priority': 0}, {'arbiterOnly': True},
                {'priority': 0, 'hidden': True}, {'priority': 0, 'slaveDelay': 5}]
        res = replset_tools.start_replica_set(members)
        self.seed, self.name = res

    @async_test_engine(timeout_sec=60)
    def test_passive_and_hidden(self):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetConnection(self.seed, replicaSet=self.name)
        c.open_sync()
        db = c.pymongo_test
        w = len(c.secondaries) + 1
        yield motor.Op(db.test.remove, w=w)
        yield motor.Op(db.test.insert, {'foo': 'bar'}, w=w)
        db.read_preference = ReadPreference.SECONDARY

        passives = replset_tools.get_passives()
        passives = [_partition_node(member) for member in passives]
        hidden = replset_tools.get_hidden_members()
        hidden = [_partition_node(member) for member in hidden]
        self.assertEqual(c.secondaries, set(passives))

        for _ in xrange(10):
            cursor = db.test.find()
            yield motor.Op(cursor.next)
            self.assertTrue(cursor.delegate._Cursor__connection_id not in hidden)

        replset_tools.kill_members(replset_tools.get_passives(), 2)
        yield gen.Task(loop.add_timeout, time.time() + 2)

        for _ in xrange(10):
            cursor = db.test.find()
            yield motor.Op(cursor.next)
            self.assertEqual(cursor.delegate._Cursor__connection_id, c.primary)

    def tearDown(self):
        super(MotorTestPassiveAndHidden, self).setUp()
        replset_tools.kill_all_members()

class MotorTestHealthMonitor(
    puritanical.PuritanicalTest,
    eventually.AssertEventuallyTest
):
    def setUp(self):
        super(MotorTestHealthMonitor, self).setUp()
        res = replset_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    @async_test_engine(timeout_sec=60)
    def test_primary_failure(self):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetConnection(self.seed, replicaSet=self.name)
        c.open_sync()
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries
        killed = replset_tools.kill_primary()
        self.assertTrue(bool(len(killed)))
        yield gen.Task(loop.add_timeout, time.time() + 1)

        # Wait for new primary to step up, and for MotorReplicaSetConnection
        # to detect it.
        for _ in xrange(30):
            if c.primary != primary:
                break
            yield gen.Task(loop.add_timeout, time.time() + 1)
        else:
            self.fail("New primary not detected")

        self.assertTrue(secondaries != c.secondaries)

    @async_test_engine(timeout_sec=60)
    def test_secondary_failure(self):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetConnection(self.seed, replicaSet=self.name)
        c.open_sync()
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries

        killed = replset_tools.kill_secondary()
        self.assertTrue(bool(len(killed)))
        self.assertEqual(primary, c.primary)

        # Wait for secondary to die, and for MotorReplicaSetConnection
        # to detect it.
        for _ in xrange(30):
            if c.secondaries != secondaries:
                break
            yield gen.Task(loop.add_timeout, time.time() + 1)
        else:
            self.fail("Dead secondary not detected")

        secondaries = c.secondaries

        replset_tools.restart_members(killed)
        self.assertEqual(primary, c.primary)

        # Wait for secondary to join, and for MotorReplicaSetConnection
        # to detect it.
        for _ in xrange(30):
            if c.secondaries != secondaries:
                break
            yield gen.Task(loop.add_timeout, time.time() + 1)
        else:
            self.fail("Dead secondary not detected")

    @async_test_engine(timeout_sec=60)
    def test_primary_stepdown(self):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetConnection(self.seed, replicaSet=self.name)
        c.open_sync()
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries
        replset_tools.stepdown_primary()

        # Wait for primary to step down, and for MotorReplicaSetConnection
        # to detect it.
        for _ in xrange(30):
            if c.primary != primary:
                break
            yield gen.Task(loop.add_timeout, time.time() + 1)
        else:
            self.fail("New primary not detected")

        self.assertTrue(secondaries != c.secondaries)

    def tearDown(self):
        super(MotorTestHealthMonitor, self).tearDown()
        replset_tools.kill_all_members()


class MotorTestWritesWithFailover(
    puritanical.PuritanicalTest,
    eventually.AssertEventuallyTest
):
    def setUp(self):
        super(MotorTestWritesWithFailover, self).setUp()
        res = replset_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    @async_test_engine(timeout_sec=60)
    def test_writes_with_failover(self):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetConnection(self.seed, replicaSet=self.name)
        c.open_sync()
        primary = c.primary
        db = c.pymongo_test
        w = len(c.secondaries) + 1
        yield motor.Op(db.test.remove, w=w)
        yield motor.Op(db.test.insert, {'foo': 'bar'}, w=w)
        result = yield motor.Op(db.test.find_one)
        self.assertEqual('bar', result['foo'])

        killed = replset_tools.kill_primary(9)
        self.assertTrue(bool(len(killed)))
        yield gen.Task(loop.add_timeout, time.time() + 2)

        for _ in xrange(30):
            try:
                yield motor.Op(db.test.insert, {'bar': 'baz'})

                # Success
                break
            except AutoReconnect:
                yield gen.Task(loop.add_timeout, time.time() + 1)
        else:
            self.fail("Couldn't insert after primary killed")

        self.assertTrue(primary != c.primary)
        result = yield motor.Op(db.test.find_one, {'bar': 'baz'})
        self.assertEqual('baz', result['bar'])

    def tearDown(self):
        replset_tools.kill_all_members()


class MotorTestReadWithFailover(
    puritanical.PuritanicalTest,
    eventually.AssertEventuallyTest
):
    def setUp(self):
        super(MotorTestReadWithFailover, self).setUp()
        res = replset_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    @async_test_engine(timeout_sec=60)
    def test_read_with_failover(self):
        loop = IOLoop.instance()
        c = motor.MotorReplicaSetConnection(self.seed, replicaSet=self.name)
        c.open_sync()
        self.assertTrue(bool(len(c.secondaries)))

        db = c.pymongo_test
        w = len(c.secondaries) + 1
        db.test.remove({}, safe=True, w=w)
        # Force replication
        yield motor.Op(db.test.insert, [{'foo': i} for i in xrange(10)], w=w)
        yield AssertEqual(10, db.test.count)

        db.read_preference = ReadPreference.SECONDARY
        cursor = db.test.find().batch_size(5)
        yield motor.Op(cursor.next)
        self.assertEqual(5, cursor.delegate._Cursor__retrieved)
        replset_tools.kill_primary()
        yield gen.Task(loop.add_timeout, time.time() + 2)

        # Primary failure shouldn't interrupt the cursor
        while True:
            result = yield motor.Op(cursor.next)
            if not result:
                # Complete
                break

        self.assertEqual(10, cursor.delegate._Cursor__retrieved)

    def tearDown(self):
        replset_tools.kill_all_members()

if __name__ == '__main__':
    unittest.main()

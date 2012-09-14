# -*- coding: utf-8 -*-
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

"""Test GridFS with Motor, an asynchronous driver for MongoDB and Tornado."""

import unittest
from gridfs.errors import FileExists, NoFile

import motor
if not motor.requirements_satisfied:
    from nose.plugins.skip import SkipTest
    raise SkipTest("Tornado or greenlet not installed")

from tornado import gen

from test.motor import (
    MotorTest, async_test_engine, host, port, AssertEqual, AssertRaises)

from pymongo.errors import AutoReconnect
from pymongo.read_preferences import ReadPreference
from test.test_replica_set_connection import TestConnectionReplicaSetBase

import datetime
from bson.py3compat import b, StringIO


# TODO: add tests of callback handling

class MotorGridfsTest(MotorTest):
    def _reset(self):
        self.sync_db.drop_collection("fs.files")
        self.sync_db.drop_collection("fs.chunks")
        self.sync_db.drop_collection("alt.files")
        self.sync_db.drop_collection("alt.chunks")

    def setUp(self):
        super(MotorGridfsTest, self).setUp()
        self._reset()
        self.db = self.motor_connection(host, port).open_sync().test

    def tearDown(self):
        self._reset()
        super(MotorGridfsTest, self).tearDown()

    def test_gridfs(self):
        self.assertRaises(TypeError, motor.create_gridfs, "foo")
        self.assertRaises(TypeError, motor.create_gridfs, self.db, 5)

    @async_test_engine()
    def test_basic(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        oid = yield motor.Op(fs.put, b("hello world"))
        out = yield motor.Op(fs.get, oid)
        yield AssertEqual(b("hello world"), out.read)
        yield AssertEqual(1, self.db.fs.files.count)
        yield AssertEqual(1, self.db.fs.chunks.count)

        yield motor.Op(fs.delete, oid)
        yield AssertRaises(NoFile, fs.get, oid)
        yield AssertEqual(0, self.db.fs.files.count)
        yield AssertEqual(0, self.db.fs.chunks.count)

        yield AssertRaises(NoFile, fs.get, "foo")
        yield AssertEqual("foo", fs.put, b("hello world"), _id="foo")
        gridout = yield motor.Op(fs.get, "foo")
        yield AssertEqual(b("hello world"), gridout.read)

    @async_test_engine()
    def test_list(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        self.assertEqual([], (yield motor.Op(fs.list)))
        yield motor.Op(fs.put, b("hello world"))
        self.assertEqual([], (yield motor.Op(fs.list)))

        yield motor.Op(fs.put, b(""), filename="mike")
        yield motor.Op(fs.put, b("foo"), filename="test")
        yield motor.Op(fs.put, b(""), filename="hello world")

        self.assertEqual(set(["mike", "test", "hello world"]),
                         set((yield motor.Op(fs.list))))

    @async_test_engine()
    def test_empty_file(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        oid = yield motor.Op(fs.put, b(""))
        gridout = yield motor.Op(fs.get, oid)
        self.assertEqual(b(""), (yield motor.Op(gridout.read)))
        self.assertEqual(1, (yield motor.Op(self.db.fs.files.count)))
        self.assertEqual(0, (yield motor.Op(self.db.fs.chunks.count)))

        raw = yield motor.Op(self.db.fs.files.find_one)
        self.assertEqual(0, raw["length"])
        self.assertEqual(oid, raw["_id"])
        self.assertTrue(isinstance(raw["uploadDate"], datetime.datetime))
        self.assertEqual(256 * 1024, raw["chunkSize"])
        self.assertTrue(isinstance(raw["md5"], basestring))

    @async_test_engine()
    def test_alt_collection(self):
        alt = yield motor.Op(motor.create_gridfs, self.db, 'alt')
        oid = yield motor.Op(alt.put, b("hello world"))
        gridout = yield motor.Op(alt.get, oid)
        self.assertEqual(b("hello world"), (yield motor.Op(gridout.read)))
        self.assertEqual(1, (yield motor.Op(self.db.alt.files.count)))
        self.assertEqual(1, (yield motor.Op(self.db.alt.chunks.count)))

        alt.delete(oid)
        yield AssertRaises(NoFile, alt.get, oid)
        self.assertEqual(0, (yield motor.Op(self.db.alt.files.count)))
        self.assertEqual(0, (yield motor.Op(self.db.alt.chunks.count)))

        yield AssertRaises(NoFile, alt.get, "foo")
        oid = yield motor.Op(alt.put, b("hello world"), _id="foo")
        self.assertEqual("foo", oid)
        gridout = yield motor.Op(alt.get, "foo")
        self.assertEqual(b("hello world"), (yield motor.Op(gridout.read)))

        yield motor.Op(alt.put, b(""), filename="mike")
        yield motor.Op(alt.put, b("foo"), filename="test")
        yield motor.Op(alt.put, b(""), filename="hello world")

        self.assertEqual(set(["mike", "test", "hello world"]),
                         set((yield motor.Op(alt.list))))

    @async_test_engine()
    def test_threaded_reads(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        yield motor.Op(fs.put, b("hello"), _id="test")
        n = 10
        results = []

        @gen.engine
        def read(callback):
            for _ in range(n):
                file = yield motor.Op(fs.get, "test")
                results.append((yield motor.Op(file.read)))
            callback()

        for i in range(10):
            read(callback=(yield gen.Callback(i))) # spawn task

        yield gen.WaitAll(range(10))

        self.assertEqual(
            n * 10 * [b('hello')],
            results)

    @async_test_engine()
    def test_threaded_writes(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        n = 10

        @gen.engine
        def write(callback):
            for _ in range(n):
                file = yield motor.Op(fs.new_file, filename="test")
                yield motor.Op(file.write, b("hello"))
                yield motor.Op(file.close)
            callback()

        for i in range(10):
            write(callback=(yield gen.Callback(i))) # spawn task

        yield gen.WaitAll(range(10))

        f = yield motor.Op(fs.get_last_version, "test")
        self.assertEqual((yield motor.Op(f.read)), b("hello"))

        # Should have created 100 versions of 'test' file
        self.assertEqual(
            100,
            (yield motor.Op(self.db.fs.files.find({'filename':'test'}).count)))

    @async_test_engine()
    def test_get_last_version(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        one = yield motor.Op(fs.put, b("foo"), filename="test")
        two = yield motor.Op(fs.new_file, filename="test")
        yield motor.Op(two.write, b("bar"))
        yield motor.Op(two.close)
        two = two._id
        three = yield motor.Op(fs.put, b("baz"), filename="test")

        # TODO refactor
        @gen.engine
        def glv(callback):
            last = yield motor.Op(fs.get_last_version, "test")
            last.read(callback=callback)

        self.assertEqual(b("baz"), (yield motor.Op(glv)))
        yield motor.Op(fs.delete, three)
        self.assertEqual(b("bar"), (yield motor.Op(glv)))
        yield motor.Op(fs.delete, two)
        self.assertEqual(b("foo"), (yield motor.Op(glv)))
        yield motor.Op(fs.delete, one)
        yield AssertRaises(NoFile, fs.get_last_version, "test")

    @async_test_engine()
    def test_get_last_version_with_metadata(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        one = yield motor.Op(fs.put, b("foo"), filename="test", author="author")
        two = yield motor.Op(fs.put, b("bar"), filename="test", author="author")

        # TODO refactor
        @gen.engine
        def version(callback, **kwargs):
            gridout = yield motor.Op(fs.get_version, **kwargs)
            gridout.read(callback=callback)
            
        self.assertEqual(b("bar"), (yield motor.Op(version, author="author")))
        fs.delete(two)
        self.assertEqual(b("foo"), (yield motor.Op(version, author="author")))
        fs.delete(one)

        one = yield motor.Op(fs.put, b("foo"), filename="test", author="author1")
        two = yield motor.Op(fs.put, b("bar"), filename="test", author="author2")

        self.assertEqual(b("foo"), (yield motor.Op(version, author="author1")))
        self.assertEqual(b("bar"), (yield motor.Op(version, author="author2")))
        self.assertEqual(b("bar"), (yield motor.Op(version, filename="test")))

        yield AssertRaises(NoFile, fs.get_last_version, author="author3")
        yield AssertRaises(NoFile, fs.get_last_version, filename="nottest", author="author1")

        fs.delete(one)
        fs.delete(two)

    @async_test_engine()
    def test_get_version(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        yield motor.Op(fs.put, b("foo"), filename="test")
        yield motor.Op(fs.put, b("bar"), filename="test")
        yield motor.Op(fs.put, b("baz"), filename="test")

        # TODO refactor
        @gen.engine
        def version(*args, **kwargs):
            callback = kwargs.pop('callback')
            gridout = yield motor.Op(fs.get_version, *args, **kwargs)
            gridout.read(callback=callback)
            
        self.assertEqual(b("foo"), (yield motor.Op(version, "test", 0)))
        self.assertEqual(b("bar"), (yield motor.Op(version, "test", 1)))
        self.assertEqual(b("baz"), (yield motor.Op(version, "test", 2)))

        self.assertEqual(b("baz"), (yield motor.Op(version, "test", -1)))
        self.assertEqual(b("bar"), (yield motor.Op(version, "test", -2)))
        self.assertEqual(b("foo"), (yield motor.Op(version, "test", -3)))

        yield AssertRaises(NoFile, fs.get_version, "test", 3)
        yield AssertRaises(NoFile, fs.get_version, "test", -4)

    @async_test_engine()
    def test_get_version_with_metadata(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        one = yield motor.Op(fs.put, b("foo"), filename="test", author="author1")
        two = yield motor.Op(fs.put, b("bar"), filename="test", author="author1")
        three = yield motor.Op(fs.put, b("baz"), filename="test", author="author2")

        # TODO refactor
        @gen.engine
        def version(callback, **kwargs):
            gridout = yield motor.Op(fs.get_version, **kwargs)
            gridout.read(callback=callback)

        self.assertEqual(b("foo"), (yield motor.Op(version, filename="test", author="author1", version=-2)))
        self.assertEqual(b("bar"), (yield motor.Op(version, filename="test", author="author1", version=-1)))
        self.assertEqual(b("foo"), (yield motor.Op(version, filename="test", author="author1", version=0)))
        self.assertEqual(b("bar"), (yield motor.Op(version, filename="test", author="author1", version=1)))
        self.assertEqual(b("baz"), (yield motor.Op(version, filename="test", author="author2", version=0)))
        self.assertEqual(b("baz"), (yield motor.Op(version, filename="test", version=-1)))
        self.assertEqual(b("baz"), (yield motor.Op(version, filename="test", version=2)))

        yield AssertRaises(NoFile, fs.get_version, filename="test", author="author3")
        yield AssertRaises(NoFile, fs.get_version, filename="test", author="author1", version=2)

        yield motor.Op(fs.delete, one)
        yield motor.Op(fs.delete, two)
        yield motor.Op(fs.delete, three)

    @async_test_engine()
    def test_put_filelike(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        oid = yield motor.Op(fs.put, StringIO(b("hello world")), chunk_size=1)
        self.assertEqual(11, (yield motor.Op(self.db.fs.chunks.count)))
        gridout = yield motor.Op(fs.get, oid)
        self.assertEqual(b("hello world"), (yield motor.Op(gridout.read)))

    @async_test_engine()
    def test_put_duplicate(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        oid = yield motor.Op(fs.put, b("hello"))
        yield AssertRaises(FileExists, fs.put, "world", _id=oid)

    @async_test_engine()
    def test_exists(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        oid = yield motor.Op(fs.put, b("hello"))
        self.assertTrue((yield motor.Op(fs.exists, oid)))
        self.assertTrue((yield motor.Op(fs.exists, {"_id": oid})))
        self.assertTrue((yield motor.Op(fs.exists, _id=oid)))

        self.assertFalse((yield motor.Op(fs.exists, filename="mike")))
        self.assertFalse((yield motor.Op(fs.exists, "mike")))

        oid = yield motor.Op(fs.put, b("hello"), filename="mike", foo=12)
        self.assertTrue((yield motor.Op(fs.exists, oid)))
        self.assertTrue((yield motor.Op(fs.exists, {"_id": oid})))
        self.assertTrue((yield motor.Op(fs.exists, _id=oid)))
        self.assertTrue((yield motor.Op(fs.exists, filename="mike")))
        self.assertTrue((yield motor.Op(fs.exists, {"filename": "mike"})))
        self.assertTrue((yield motor.Op(fs.exists, foo=12)))
        self.assertTrue((yield motor.Op(fs.exists, {"foo": 12})))
        self.assertTrue((yield motor.Op(fs.exists, foo={"$gt": 11})))
        self.assertTrue((yield motor.Op(fs.exists, {"foo": {"$gt": 11}})))

        self.assertFalse((yield motor.Op(fs.exists, foo=13)))
        self.assertFalse((yield motor.Op(fs.exists, {"foo": 13})))
        self.assertFalse((yield motor.Op(fs.exists, foo={"$gt": 12})))
        self.assertFalse((yield motor.Op(fs.exists, {"foo": {"$gt": 12}})))

    @async_test_engine()
    def test_put_unicode(self):
        fs = yield motor.Op(motor.create_gridfs, self.db)
        yield AssertRaises(TypeError, fs.put, u"hello")

        oid = yield motor.Op(fs.put, u"hello", encoding="utf-8")
        gridout = yield motor.Op(fs.get, oid)
        self.assertEqual(b("hello"), (yield motor.Op(gridout.read)))
        self.assertEqual("utf-8", gridout.encoding)

        oid = yield motor.Op(fs.put, u"aé", encoding="iso-8859-1")
        gridout = yield motor.Op(fs.get, oid)
        self.assertEqual(
            u"aé".encode("iso-8859-1"), (yield motor.Op(gridout.read)))
        self.assertEqual("iso-8859-1", gridout.encoding)


class TestGridfsReplicaSet(MotorTest, TestConnectionReplicaSetBase):
    @async_test_engine()
    def test_gridfs_replica_set(self):
        rsc = motor.MotorReplicaSetConnection(
            host=host, port=port,
            w=self.w, wtimeout=5000,
            read_preference=ReadPreference.SECONDARY,
            replicaSet=self.name
        ).open_sync()

        try:
            fs = yield motor.Op(motor.create_gridfs, rsc.pymongo_test)
            oid = yield motor.Op(fs.put, b('foo'))
            gridout = yield motor.Op(fs.get, oid)
            content = yield motor.Op(gridout.read)
            self.assertEqual(b('foo'), content)
        finally:
            rsc.close()

    @async_test_engine()
    def test_gridfs_secondary(self):
        primary_host, primary_port = self.primary
        primary_connection = motor.MotorConnection(
            primary_host, primary_port).open_sync()

        secondary_host, secondary_port = self.secondaries[0]
        for secondary_connection in [
            motor.MotorConnection(
                secondary_host, secondary_port, slave_okay=True).open_sync(),
            motor.MotorConnection(secondary_host, secondary_port,
                read_preference=ReadPreference.SECONDARY).open_sync(),
        ]:
            yield motor.Op(
                primary_connection.pymongo_test.drop_collection, "fs.files")
            yield motor.Op(
                primary_connection.pymongo_test.drop_collection, "fs.chunks")

            # Should detect it's connected to secondary and not attempt to
            # create index
            fs = yield motor.Op(
                motor.create_gridfs, secondary_connection.pymongo_test)

            # This won't detect secondary, raises error
            yield AssertRaises(AutoReconnect, fs.put, b('foo'))

    def tearDown(self):
        rsc = self._get_connection()
        rsc.pymongo_test.drop_collection('fs.files')
        rsc.pymongo_test.drop_collection('fs.chunks')
        rsc.close()


if __name__ == "__main__":
    unittest.main()

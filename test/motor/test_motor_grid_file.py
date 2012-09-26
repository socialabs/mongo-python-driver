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
from tornado import gen

import motor
if not motor.requirements_satisfied:
    from nose.plugins.skip import SkipTest
    raise SkipTest("Tornado or greenlet not installed")

import datetime
import unittest
from functools import partial

from bson.objectid import ObjectId
from bson.py3compat import b, StringIO
from gridfs.errors import NoFile
from gridfs.grid_file import DEFAULT_CHUNK_SIZE, _SEEK_CUR, _SEEK_END

from test import qcheck
from test.motor import MotorTest, async_test_engine, host, port, AssertRaises


# TODO: test the files from gfs are already open when expected

class MotorGridFileTest(MotorTest):
    def _reset(self):
        self.sync_db.drop_collection("fs.files")
        self.sync_db.drop_collection("fs.chunks")
        self.sync_db.drop_collection("alt.files")
        self.sync_db.drop_collection("alt.chunks")

    def setUp(self):
        super(MotorGridFileTest, self).setUp()
        self._reset()
        self.db = self.motor_connection(host, port).open_sync().test

    def tearDown(self):
        self._reset()
        super(MotorGridFileTest, self).tearDown()

    @async_test_engine()
    def test_grid_in_callback(self):
        f = motor.MotorGridIn(self.db.fs, filename="test")
        self.check_callback_handling(f.open, False)
        f = yield motor.Op(motor.MotorGridIn(self.db.fs, filename="test").open)
        self.check_callback_handling(partial(f.set, 'name', 'value'), False)
        self.check_callback_handling(f.close, False)
        self.check_callback_handling(partial(f.write, 'a'), False)
        self.check_callback_handling(partial(f.writelines, ['a']), False)

    @async_test_engine()
    def test_grid_out_callback(self):
        # Some setup: we need to make an open GridOut
        f = yield motor.Op(motor.MotorGridIn(self.db.fs, filename="test").open)
        yield motor.Op(f.close)

        g = motor.MotorGridOut(self.db.fs, f._id)
        self.check_callback_handling(g.open, False)

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.check_callback_handling(g.read, True)
        self.check_callback_handling(g.readline, True)

    @async_test_engine()
    def test_basic(self):
        f = yield motor.Op(motor.MotorGridIn(self.db.fs, filename="test").open)
        yield motor.Op(f.write, b("hello world"))
        yield motor.Op(f.close)
        self.assertEqual(1, (yield motor.Op(self.db.fs.files.find().count)))
        self.assertEqual(1, (yield motor.Op(self.db.fs.chunks.find().count)))

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(b("hello world"), (yield motor.Op(g.read)))

        # make sure it's still there...
        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(b("hello world"), (yield motor.Op(g.read)))

        f = yield motor.Op(motor.MotorGridIn(self.db.fs, filename="test").open)
        yield motor.Op(f.close)
        self.assertEqual(2, (yield motor.Op(self.db.fs.files.find().count)))
        self.assertEqual(1, (yield motor.Op(self.db.fs.chunks.find().count)))

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(b(""), (yield motor.Op(g.read)))

    @async_test_engine()
    def test_md5(self):
        f = yield motor.Op(motor.MotorGridIn(self.db.fs).open)
        yield motor.Op(f.write, b("hello world\n"))
        yield motor.Op(f.close)
        self.assertEqual("6f5902ac237024bdd0c176cb93063dc4", f.md5)

    @async_test_engine()
    def test_alternate_collection(self):
        yield motor.Op(self.db.alt.files.remove)
        yield motor.Op(self.db.alt.chunks.remove)

        f = yield motor.Op(motor.MotorGridIn(self.db.alt).open)
        yield motor.Op(f.write, b("hello world"))
        yield motor.Op(f.close)

        self.assertEqual(1, (yield motor.Op(self.db.alt.files.find().count)))
        self.assertEqual(1, (yield motor.Op(self.db.alt.chunks.find().count)))

        g = yield motor.Op(motor.MotorGridOut(self.db.alt, f._id).open)
        self.assertEqual(b("hello world"), (yield motor.Op(g.read)))

        # test that md5 still works...
        self.assertEqual("5eb63bbbe01eeed093cb22bb8f5acdc3", g.md5)

    @async_test_engine()
    def test_grid_in_default_opts(self):
        self.assertRaises(TypeError, motor.MotorGridIn, "foo")

        a = yield motor.Op(motor.MotorGridIn(self.db.fs).open)

        self.assertTrue(isinstance(a._id, ObjectId))
        self.assertRaises(AttributeError, setattr, a, "_id", 5)

        self.assertEqual(None, a.filename)

        # This raises AttributeError because you can't directly set properties
        # in Motor, have to use set()
        def setter():
            a.filename = "my_file"
        self.assertRaises(AttributeError, setter)

        # This method of setting attributes works in Motor
        yield motor.Op(a.set, "filename", "my_file")
        self.assertEqual("my_file", a.filename)

        self.assertEqual(None, a.content_type)
        yield motor.Op(a.set, "content_type", "text/html")
        self.assertEqual("text/html", a.content_type)

        self.assertRaises(AttributeError, getattr, a, "length")
        self.assertRaises(AttributeError, setattr, a, "length", 5)

        self.assertEqual(256 * 1024, a.chunk_size)
        self.assertRaises(AttributeError, setattr, a, "chunk_size", 5)

        self.assertRaises(AttributeError, getattr, a, "upload_date")
        self.assertRaises(AttributeError, setattr, a, "upload_date", 5)

        self.assertRaises(AttributeError, getattr, a, "aliases")
        yield motor.Op(a.set, "aliases", ["foo"])
        self.assertEqual(["foo"], a.aliases)

        self.assertRaises(AttributeError, getattr, a, "metadata")
        yield motor.Op(a.set, "metadata", {"foo": 1})
        self.assertEqual({"foo": 1}, a.metadata)

        self.assertRaises(AttributeError, getattr, a, "md5")
        self.assertRaises(AttributeError, setattr, a, "md5", 5)

        yield motor.Op(a.close)

        self.assertTrue(isinstance(a._id, ObjectId))
        self.assertRaises(AttributeError, setattr, a, "_id", 5)

        self.assertEqual("my_file", a.filename)

        self.assertEqual("text/html", a.content_type)

        self.assertEqual(0, a.length)
        self.assertRaises(AttributeError, setattr, a, "length", 5)

        self.assertEqual(256 * 1024, a.chunk_size)
        self.assertRaises(AttributeError, setattr, a, "chunk_size", 5)

        self.assertTrue(isinstance(a.upload_date, datetime.datetime))
        self.assertRaises(AttributeError, setattr, a, "upload_date", 5)

        self.assertEqual(["foo"], a.aliases)

        self.assertEqual({"foo": 1}, a.metadata)

        self.assertEqual("d41d8cd98f00b204e9800998ecf8427e", a.md5)
        self.assertRaises(AttributeError, setattr, a, "md5", 5)

    @async_test_engine()
    def test_grid_in_custom_opts(self):
        self.assertRaises(TypeError, motor.MotorGridIn, "foo")

        a = yield motor.Op(
            motor.MotorGridIn(self.db.fs, _id=5, filename="my_file",
            contentType="text/html", chunkSize=1000, aliases=["foo"],
            metadata={"foo": 1, "bar": 2}, bar=3, baz="hello").open)

        self.assertEqual(5, a._id)
        self.assertEqual("my_file", a.filename)
        self.assertEqual("text/html", a.content_type)
        self.assertEqual(1000, a.chunk_size)
        self.assertEqual(["foo"], a.aliases)
        self.assertEqual({"foo": 1, "bar": 2}, a.metadata)
        self.assertEqual(3, a.bar)
        self.assertEqual("hello", a.baz)
        self.assertRaises(AttributeError, getattr, a, "mike")

        b = yield motor.Op(
            motor.MotorGridIn(self.db.fs,
            content_type="text/html", chunk_size=1000, baz=100).open)
        self.assertEqual("text/html", b.content_type)
        self.assertEqual(1000, b.chunk_size)
        self.assertEqual(100, b.baz)

    @async_test_engine()
    def test_grid_out_default_opts(self):
        self.assertRaises(TypeError, motor.MotorGridOut, "foo")

        gout = motor.MotorGridOut(self.db.fs, 5)
        yield AssertRaises(NoFile, gout.open)

        a = yield motor.Op(motor.MotorGridIn(self.db.fs).open)
        yield motor.Op(a.close)

        b = yield motor.Op(motor.MotorGridOut(self.db.fs, a._id).open)

        self.assertEqual(a._id, b._id)
        self.assertEqual(0, b.length)
        self.assertEqual(None, b.content_type)
        self.assertEqual(256 * 1024, b.chunk_size)
        self.assertTrue(isinstance(b.upload_date, datetime.datetime))
        self.assertEqual(None, b.aliases)
        self.assertEqual(None, b.metadata)
        self.assertEqual("d41d8cd98f00b204e9800998ecf8427e", b.md5)

        for attr in ["_id", "name", "content_type", "length", "chunk_size",
                     "upload_date", "aliases", "metadata", "md5"]:
            self.assertRaises(AttributeError, setattr, b, attr, 5)

    @async_test_engine()
    def test_grid_out_custom_opts(self):
        one = yield motor.Op(
            motor.MotorGridIn(self.db.fs, _id=5, filename="my_file",
            contentType="text/html", chunkSize=1000, aliases=["foo"],
            metadata={"foo": 1, "bar": 2}, bar=3, baz="hello").open)
        yield motor.Op(one.write, b("hello world"))
        yield motor.Op(one.close)

        two = yield motor.Op(motor.MotorGridOut(self.db.fs, 5).open)

        self.assertEqual(5, two._id)
        self.assertEqual(11, two.length)
        self.assertEqual("text/html", two.content_type)
        self.assertEqual(1000, two.chunk_size)
        self.assertTrue(isinstance(two.upload_date, datetime.datetime))
        self.assertEqual(["foo"], two.aliases)
        self.assertEqual({"foo": 1, "bar": 2}, two.metadata)
        self.assertEqual(3, two.bar)
        self.assertEqual("5eb63bbbe01eeed093cb22bb8f5acdc3", two.md5)

        for attr in ["_id", "name", "content_type", "length", "chunk_size",
                     "upload_date", "aliases", "metadata", "md5"]:
            self.assertRaises(AttributeError, setattr, two, attr, 5)

    @async_test_engine()
    def test_grid_out_file_document(self):
        one = yield motor.Op(motor.MotorGridIn(self.db.fs).open)
        yield motor.Op(one.write, b("foo bar"))
        yield motor.Op(one.close)

        two = yield motor.Op(motor.MotorGridOut(
            self.db.fs,
            file_document=(yield motor.Op(self.db.fs.files.find_one))).open)

        self.assertEqual(b("foo bar"), (yield motor.Op(two.read)))

        three = yield motor.Op(motor.MotorGridOut(
            self.db.fs, 5,
            file_document=(yield motor.Op(self.db.fs.files.find_one))).open)

        self.assertEqual(b("foo bar"), (yield motor.Op(three.read)))

        yield AssertRaises(NoFile, motor.MotorGridOut(self.db.fs, file_document={}).open)

    @async_test_engine()
    def test_write_file_like(self):
        one = yield motor.Op(motor.MotorGridIn(self.db.fs).open)
        yield motor.Op(one.write, b("hello world"))
        yield motor.Op(one.close)

        two = yield motor.Op(motor.MotorGridOut(self.db.fs, one._id).open)

        three = yield motor.Op(motor.MotorGridIn(self.db.fs).open)
        yield motor.Op(three.write, two)
        yield motor.Op(three.close)

        four = yield motor.Op(motor.MotorGridOut(self.db.fs, three._id).open)
        self.assertEqual(b("hello world"), (yield motor.Op(four.read)))

        five = yield motor.Op(motor.MotorGridIn(self.db.fs, chunk_size=2).open)
        yield motor.Op(five.write, b("hello"))
        buffer = StringIO(b(" world"))
        yield motor.Op(five.write, buffer)
        yield motor.Op(five.write, b(" and mongodb"))
        yield motor.Op(five.close)
        self.assertEqual(
            b("hello world and mongodb"),
            (yield motor.Op((
                yield motor.Op(motor.MotorGridOut(self.db.fs, five._id).open)
                ).read)))

    @async_test_engine()
    def test_write_lines(self):
        a = yield motor.Op(motor.MotorGridIn(self.db.fs).open)
        yield motor.Op(a.writelines, [b("hello "), b("world")])
        yield motor.Op(a.close)

        gout = yield motor.Op(motor.MotorGridOut(self.db.fs, a._id).open)
        self.assertEqual(b("hello world"), (yield motor.Op(gout.read)))

    @async_test_engine()
    def test_close(self):
        f = yield motor.Op(motor.MotorGridIn(self.db.fs).open)
        yield motor.Op(f.close)
        yield AssertRaises(ValueError, f.write, "test")
        yield motor.Op(f.close)

    @async_test_engine()
    def test_multi_chunk_file(self):
        random_string = qcheck.gen_string(qcheck.lift(300000))()

        f = yield motor.Op(motor.MotorGridIn(self.db.fs).open)
        yield motor.Op(f.write, random_string)
        yield motor.Op(f.close)

        self.assertEqual(1, (yield motor.Op(self.db.fs.files.find().count)))
        self.assertEqual(2, (yield motor.Op(self.db.fs.chunks.find().count)))

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(random_string, (yield motor.Op(g.read)))

    @async_test_engine()
    def test_small_chunks(self):
        data = 'whatever' * 100

        f = yield motor.Op(motor.MotorGridIn(self.db.fs, chunkSize=1).open)
        yield motor.Op(f.write, data)
        yield motor.Op(f.close)

        self.assertEqual(1, (yield motor.Op(self.db.fs.files.find().count)))
        self.assertEqual(
            len(data), (yield motor.Op(self.db.fs.chunks.find().count)))

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(data, (yield motor.Op(g.read)))

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(data,
            (yield motor.Op(g.read, len(data) / 2)) +
            (yield motor.Op(g.read, len(data) / 2)))

    @async_test_engine()
    def test_seek(self):
        f = yield motor.Op(motor.MotorGridIn(self.db.fs, chunkSize=3).open)
        yield motor.Op(f.write, b("hello world"))
        yield motor.Op(f.close)

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(b("hello world"), (yield motor.Op(g.read)))
        g.seek(0)
        self.assertEqual(b("hello world"), (yield motor.Op(g.read)))
        g.seek(1)
        self.assertEqual(b("ello world"), (yield motor.Op(g.read)))
        self.assertRaises(IOError, g.seek, -1)

        g.seek(-3, _SEEK_END)
        self.assertEqual(b("rld"), (yield motor.Op(g.read)))
        g.seek(0, _SEEK_END)
        self.assertEqual(b(""), (yield motor.Op(g.read)))
        self.assertRaises(IOError, g.seek, -100, _SEEK_END)

        g.seek(3)
        g.seek(3, _SEEK_CUR)
        self.assertEqual(b("world"), (yield motor.Op(g.read)))
        self.assertRaises(IOError, g.seek, -100, _SEEK_CUR)

    @async_test_engine()
    def test_tell(self):
        f = yield motor.Op(motor.MotorGridIn(self.db.fs, chunkSize=3).open)
        yield motor.Op(f.write, b("hello world"))
        yield motor.Op(f.close)

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(0, g.tell())
        yield motor.Op(g.read, 0)
        self.assertEqual(0, g.tell())
        yield motor.Op(g.read, 1)
        self.assertEqual(1, g.tell())
        (yield motor.Op(g.read, 2))
        self.assertEqual(3, g.tell())
        (yield motor.Op(g.read))
        self.assertEqual(g.length, g.tell())

    @async_test_engine()
    def test_multiple_reads(self):
        f = yield motor.Op(motor.MotorGridIn(self.db.fs, chunkSize=3).open)
        yield motor.Op(f.write, b("hello world"))
        yield motor.Op(f.close)

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(b("he"), (yield motor.Op(g.read, 2)))
        self.assertEqual(b("ll"), (yield motor.Op(g.read, 2)))
        self.assertEqual(b("o "), (yield motor.Op(g.read, 2)))
        self.assertEqual(b("wo"), (yield motor.Op(g.read, 2)))
        self.assertEqual(b("rl"), (yield motor.Op(g.read, 2)))
        self.assertEqual(b("d"), (yield motor.Op(g.read, 2)))
        self.assertEqual(b(""), (yield motor.Op(g.read, 2)))

    @async_test_engine()
    def test_readline(self):
        f = yield motor.Op(motor.MotorGridIn(self.db.fs, chunkSize=5).open)
        yield motor.Op(f.write, b("""Hello world,
How are you?
Hope all is well.
Bye"""))
        yield motor.Op(f.close)

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(b("H"), (yield motor.Op(g.read, 1)))
        self.assertEqual(b("ello world,\n"), (yield motor.Op(g.readline, )))
        self.assertEqual(b("How a"), (yield motor.Op(g.readline, 5)))
        self.assertEqual(b(""), (yield motor.Op(g.readline, 0)))
        self.assertEqual(b("re you?\n"), (yield motor.Op(g.readline, )))
        self.assertEqual(b("Hope all is well.\n"), (yield motor.Op(g.readline, 1000)))
        self.assertEqual(b("Bye"), (yield motor.Op(g.readline, )))
        self.assertEqual(b(""), (yield motor.Op(g.readline, )))

    @async_test_engine()
    def test_read_chunks_unaligned_buffer_size(self):
        in_data = b("This is a text that doesn't "
                    "quite fit in a single 16-byte chunk.")
        f = yield motor.Op(motor.MotorGridIn(self.db.fs, chunkSize=16).open)
        yield motor.Op(f.write, in_data)
        yield motor.Op(f.close)

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        out_data = b('')
        while 1:
            s = yield motor.Op(g.read, 13)
            if not s:
                break
            out_data += s

        self.assertEqual(in_data, out_data)

    @async_test_engine()
    def test_write_unicode(self):
        f = yield motor.Op(motor.MotorGridIn(self.db.fs).open)
        self.assertRaises(TypeError, f.write, u"foo")

        f = yield motor.Op(motor.MotorGridIn(self.db.fs, encoding="utf-8").open)
        yield motor.Op(f.write, u"foo")
        yield motor.Op(f.close)

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(b("foo"), (yield motor.Op(g.read)))

        f = yield motor.Op(motor.MotorGridIn(self.db.fs, encoding="iso-8859-1").open)
        yield motor.Op(f.write, u"aé")
        yield motor.Op(f.close)

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual(u"aé".encode("iso-8859-1"), (yield motor.Op(g.read)))

    @async_test_engine()
    def test_set_after_close(self):
        f = yield motor.Op(motor.MotorGridIn(self.db.fs, _id="foo", bar="baz").open)

        self.assertEqual("foo", f._id)
        self.assertEqual("baz", f.bar)
        self.assertRaises(AttributeError, getattr, f, "baz")
        self.assertRaises(AttributeError, getattr, f, "uploadDate")

        self.assertRaises(AttributeError, setattr, f, "_id", 5)
        f.bar = "foo"
        f.baz = 5

        self.assertEqual("foo", f._id)
        self.assertEqual("foo", f.bar)
        self.assertEqual(5, f.baz)
        self.assertRaises(AttributeError, getattr, f, "uploadDate")

        yield motor.Op(f.close)

        self.assertEqual("foo", f._id)
        self.assertEqual("foo", f.bar)
        self.assertEqual(5, f.baz)
        self.assertTrue(f.uploadDate)

        self.assertRaises(AttributeError, setattr, f, "_id", 5)
        f.bar = "a"
        f.baz = "b"
        self.assertRaises(AttributeError, setattr, f, "upload_date", 5)

        g = yield motor.Op(motor.MotorGridOut(self.db.fs, f._id).open)
        self.assertEqual("a", g.bar)
        self.assertEqual("b", g.baz)
        # Versions 2.0.1 and older saved a _closed field for some reason.
        self.assertRaises(AttributeError, getattr, g, "_closed")

    @async_test_engine()
    def test_prechunked_string(self):

        @gen.engine
        def write_me(s, chunk_size, callback):
            buf = StringIO(s)
            infile = yield motor.Op(motor.MotorGridIn(self.db.fs).open)
            while True:
                to_write = buf.read(chunk_size)
                if to_write == b(''):
                    break
                yield motor.Op(infile.write, to_write)
            yield motor.Op(infile.close)
            buf.close()

            outfile = yield motor.Op(
                motor.MotorGridOut(self.db.fs, infile._id).open)
            data = (yield motor.Op(outfile.read))
            self.assertEqual(s, data)
            callback()

        s = b('x' * DEFAULT_CHUNK_SIZE * 4)
        # Test with default chunk size
        yield gen.Task(write_me, s, DEFAULT_CHUNK_SIZE)
        # Multiple

        yield gen.Task(write_me, s, DEFAULT_CHUNK_SIZE * 3)

        # Custom
        yield gen.Task(write_me, s, 262300)


if __name__ == "__main__":
    unittest.main()

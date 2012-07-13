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

"""Test the replica_set_connection module."""

import sys
import unittest
sys.path[0:0] = [""]

from pymongo.read_preferences import ReadPreference
from pymongo.errors import ConfigurationError

from test.test_connection import host, port
from test.test_replica_set_connection import TestConnectionReplicaSetBase

pair = '%s:%d' % (host, port)


class TestReadPreferencesBase(TestConnectionReplicaSetBase):
    def setUp(self):
        super(TestReadPreferencesBase, self).setUp()
        # Insert some data so we can use cursors in read_from_which_host
        c = self._get_connection()
        c.pymongo_test.test.drop()
        c.pymongo_test.test.insert([{'_id': i} for i in range(10)], w=self.w)

    def tearDown(self):
        super(TestReadPreferencesBase, self).tearDown()
        c = self._get_connection()
        c.pymongo_test.test.drop()

    def read_from_which_host(self, connection):
        """Do a find() on the connection and return which host was used
        """
        cursor = connection.pymongo_test.test.find()
        cursor.next()
        return cursor._Cursor__connection_id

    def read_from_which_kind(self, connection):
        """Do a find() on the connection and return 'primary' or 'secondary'
           depending on which the connection used.
        """
        connection_id = self.read_from_which_host(connection)
        if connection_id == connection.primary:
            return 'primary'
        elif connection_id in connection.secondaries:
            return 'secondary'
        else:
            self.fail(
                'Cursor used connection id %s, expected either primary '
                '%s or secondaries %s' % (
                    connection_id, connection.primary, connection.secondaries))

    def assertReadsFrom(self, expected, **kwargs):
        c = self._get_connection(**kwargs)
        used = self.read_from_which_kind(c)
        self.assertEqual(expected, used, 'Cursor used %s, expected %s' % (
            expected, used))


class TestReadPreferences(TestReadPreferencesBase):
    def test_primary(self):
        self.assertReadsFrom('primary',
            read_preference=ReadPreference.PRIMARY)

    def test_primary_preferred(self):
        self.assertReadsFrom('primary',
            read_preference=ReadPreference.PRIMARY_PREFERRED)

    def test_secondary(self):
        self.assertReadsFrom('secondary',
            read_preference=ReadPreference.SECONDARY)

    def test_secondary_preferred(self):
        self.assertReadsFrom('secondary',
            read_preference=ReadPreference.SECONDARY_PREFERRED)
        
    def test_nearest(self):
        # With high secondaryAcceptableLatencyMS, expect to read from any
        # member
        c = self._get_connection(
            read_preference=ReadPreference.NEAREST,
            secondaryAcceptableLatencyMS=1000,
            auto_start_request=False)

        used = set()
        for i in range(1000):
            host = self.read_from_which_host(c)
            used.add(host)

        data_members = set(self.hosts).difference(set(self.arbiters))
        not_used = data_members.difference(used)
        self.assertFalse(not_used,
            "Expected to use primary and all secondaries for mode NEAREST,"
            " but didn't use %s" % not_used)


class TestTags(TestReadPreferencesBase):
    def test_primary(self):
        # Tags not allowed with PRIMARY
        self.assertRaises(ConfigurationError,
            self._get_connection, tag_sets=[{'dc': 'ny'}])


class TestCommands(TestReadPreferencesBase):
    def test_commands_and_read_preferences(self):
        assert False, 'TODO: https://wiki.10gen.com/display/10GEN/Driver+Read+Semantics#DriverReadSemantics-Commands'


if __name__ == "__main__":
    unittest.main()

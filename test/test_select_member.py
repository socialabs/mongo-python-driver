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

"""Test the read_preferences.select_member function."""
import sys
import unittest

sys.path[0:0] = [""]

from pymongo.replica_set_connection import Member, MAX_BSON_SIZE
from pymongo.read_preferences import ReadPreference, select_member


def make_member(state, tags=None, ping_time=0, up=True, host=None):
    m = Member(
        host=host,
        ismaster_response={
            'ismaster': (state == 'primary'),
            'maxBsonObjectSize': MAX_BSON_SIZE,
            'tags': tags or {},
        },
        ping_time=ping_time / 1000.0,
        connection_pool=None)
    
    m.up = up
    return m


PRIMARY = ReadPreference.PRIMARY
PRIMARY_PREFERRED = ReadPreference.PRIMARY_PREFERRED
SECONDARY = ReadPreference.SECONDARY
SECONDARY_PREFERRED = ReadPreference.SECONDARY_PREFERRED
NEAREST = ReadPreference.NEAREST


class ModeTest(unittest.TestCase):
    def test_primary(self):
        self.assertTrue(select_member([
            make_member('primary')
        ]).is_primary)

        self.assertTrue(select_member([
            make_member('primary'),
            make_member('secondary')
        ]).is_primary)

        self.assertEqual(None, select_member([
            make_member('secondary')
        ]))

    def test_primary_preferred(self):
        self.assertTrue(select_member([
            make_member('primary')
        ], PRIMARY_PREFERRED).is_primary)

        self.assertTrue(select_member([
            make_member('primary'),
            make_member('secondary')
        ], PRIMARY_PREFERRED).is_primary)

        self.assertFalse(select_member([
            make_member('secondary')
        ], PRIMARY_PREFERRED).is_primary)

    def test_secondary(self):
        self.assertEqual(None, select_member([
            make_member('primary')
        ], SECONDARY))

        self.assertFalse(select_member([
            make_member('primary'),
            make_member('secondary')
        ], SECONDARY).is_primary)

        self.assertFalse(select_member([
            make_member('secondary')
        ], SECONDARY).is_primary)

    def test_secondary_preferred(self):
        self.assertTrue(select_member([
            make_member('primary')
        ], SECONDARY_PREFERRED).is_primary)

        self.assertFalse(select_member([
            make_member('primary'),
            make_member('secondary')
        ], SECONDARY_PREFERRED).is_primary)

        self.assertFalse(select_member([
            make_member('secondary')
        ], SECONDARY_PREFERRED).is_primary)

    def test_nearest(self):
        self.assertTrue(select_member([
            make_member('primary')
        ], NEAREST).is_primary)

        self.assertFalse(select_member([
            make_member('secondary')
        ], NEAREST).is_primary)

        # Test that we distribute reads evenly among primary and secondary
        n0, n1 = 0, 0
        for i in range(10000):
            if 0 == select_member([
                make_member('primary', host=0),
                make_member('secondary', host=1),
            ], NEAREST).host:
                n0 += 1
            else:
                n1 += 1

        self.assertAlmostEqual(n0, n1, delta=400)


class TagsTest(unittest.TestCase):
    def test_multiple_tags(self):
        members = [
            make_member('primary',   tags={'dc': 'ny', 'rack': '1'}, host=0),
            make_member('secondary', tags={'dc': 'ny', 'rack': '2'}, host=1),
            make_member('secondary', tags={'dc': 'sf', 'rack': '1'}, host=2)
        ]

        self.assertEqual(0, select_member(
            members, mode=SECONDARY_PREFERRED,
            tag_sets=[{'dc': 'ny', 'rack': '1'}]).host)

        self.assertEqual(1, select_member(
            members, mode=SECONDARY_PREFERRED,
            tag_sets=[{'dc': 'ny', 'rack': '2'}]).host)


class PingTest(unittest.TestCase):
    def test_ping_time_selection(self):
        members = [
            make_member('primary',   ping_time=10, tags={'dc': 'ny'}, host=0),
            make_member('secondary', ping_time=20, tags={'dc': 'la'}, host=1),
            make_member('secondary', ping_time=20, tags={'dc': 'sf'}, host=2),
            make_member('secondary', ping_time=30, tags={'dc': 'sf'}, host=3),
            make_member('secondary', ping_time=40, tags={'dc': 'sf'}, host=4)
        ]

        # members 3 and 4 are too far from 0, which is closest
        chosen = set(
            select_member(members, NEAREST).host for _ in range(1000))
        self.assertEqual(set([0, 1, 2]), chosen)

        # member 1 is closest secondary, so member 3 is ok but not 4
        chosen = set(select_member(
            members, SECONDARY_PREFERRED
        ).host for _ in range(1000))
        self.assertEqual(set([1, 2, 3]), chosen)

        # member 2 is closest secondary in 'sf', so member 3 is ok but still
        # not 4
        chosen = set(select_member(
            members, NEAREST, tag_sets=[{'dc': 'sf'}]
        ).host for _ in range(1000))
        self.assertEqual(set([2, 3]), chosen)

        # Make member 4 ok
        chosen = set(select_member(
            members, NEAREST, tag_sets=[{'dc': 'sf'}], latency=30
        ).host for _ in range(1000))
        self.assertEqual(set([2, 3, 4]), chosen)

        # Far primary
        members = [
            make_member('primary',   ping_time=50, host=0),
            make_member('secondary', ping_time= 0, host=1)]

        chosen = set(select_member(
            members, PRIMARY).host for _ in range(1000))
        self.assertEqual(set([0]), chosen)

        chosen = set(select_member(
            members, PRIMARY_PREFERRED).host for _ in range(1000))
        self.assertEqual(set([0]), chosen)

        chosen = set(select_member(
            members, SECONDARY_PREFERRED).host for _ in range(1000))
        self.assertEqual(set([1]), chosen)


if __name__ == '__main__':
    unittest.main()

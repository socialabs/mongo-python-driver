# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License",
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

"""Utilities for choosing which member of a replica set to read from."""

import random

from pymongo.errors import ConfigurationError


class ReadPreference:
    """An enum that defines the read preferences supported by PyMongo.

    TODO: update
    +----------------------+--------------------------------------------------+
    |    Connection type   |                 Read Preference                  |
    +======================+================+================+================+
    |                      |`PRIMARY`       |`SECONDARY`     |`SECONDARY_ONLY`|
    +----------------------+----------------+----------------+----------------+
    |Connection to a single|Queries are     |Queries are     |Same as         |
    |host.                 |allowed if the  |allowed if the  |`SECONDARY`     |
    |                      |connection is to|connection is to|                |
    |                      |the replica set |the replica set |                |
    |                      |primary.        |primary or a    |                |
    |                      |                |secondary.      |                |
    +----------------------+----------------+----------------+----------------+
    |Connection to a       |Queries are sent|Queries are     |Same as         |
    |mongos.               |to the primary  |distributed     |`SECONDARY`     |
    |                      |of a shard.     |among shard     |                |
    |                      |                |secondaries.    |                |
    |                      |                |Queries are sent|                |
    |                      |                |to the primary  |                |
    |                      |                |if no           |                |
    |                      |                |secondaries are |                |
    |                      |                |available.      |                |
    |                      |                |                |                |
    +----------------------+----------------+----------------+----------------+
    |ReplicaSetConnection  |Queries are sent|Queries are     |Queries are     |
    |                      |to the primary  |distributed     |never sent to   |
    |                      |of the replica  |among replica   |the replica set |
    |                      |set.            |set secondaries.|primary. An     |
    |                      |                |Queries are sent|exception is    |
    |                      |                |to the primary  |raised if no    |
    |                      |                |if no           |secondary is    |
    |                      |                |secondaries are |available.      |
    |                      |                |available.      |                |
    |                      |                |                |                |
    +----------------------+----------------+----------------+----------------+
    """

    PRIMARY = 0
    PRIMARY_PREFERRED = 1
    SECONDARY = 2
    SECONDARY_ONLY = 2
    SECONDARY_PREFERRED = 3
    NEAREST = 4

# For formatting error messages
modes = {
    ReadPreference.PRIMARY:             'PRIMARY',
    ReadPreference.PRIMARY_PREFERRED:   'PRIMARY_PREFERRED',
    ReadPreference.SECONDARY:           'SECONDARY',
    ReadPreference.SECONDARY_PREFERRED: 'SECONDARY_PREFERRED',
    ReadPreference.NEAREST:             'NEAREST',
}

def select_primary(members):
    for member in members:
        if member.is_primary:
            if member.up:
                return member
            else:
                return None

    return None


def select_member_with_tags(members, tags, secondary_only, latency):
    candidates = []

    for candidate in members:
        if not candidate.up:
            continue

        if secondary_only and candidate.is_primary:
            continue

        if candidate.matches_tags(tags):
            candidates.append(candidate)

    if not candidates:
        return None

    # ping_time is in seconds
    fastest = min([candidate.ping_time for candidate in candidates])
    near_candidates = [
        candidate for candidate in candidates
        if candidate.ping_time - fastest < latency / 1000.]

    return random.choice(near_candidates)


def select_member(
    members,
    mode=ReadPreference.PRIMARY,
    tag_sets=None,
    latency=15
):
    """Return a Member or None.
    """
    if tag_sets is None:
        tag_sets = [{}]

    # For brevity
    PRIMARY             = ReadPreference.PRIMARY
    PRIMARY_PREFERRED   = ReadPreference.PRIMARY_PREFERRED
    SECONDARY           = ReadPreference.SECONDARY
    SECONDARY_PREFERRED = ReadPreference.SECONDARY_PREFERRED
    NEAREST             = ReadPreference.NEAREST
        
    if mode == PRIMARY:
        if tag_sets != [{}]:
            raise ConfigurationError("PRIMARY cannot be combined with tags")
        return select_primary(members)

    elif mode == PRIMARY_PREFERRED:
        candidate_primary = select_member(members, PRIMARY, [{}], latency)
        if candidate_primary:
            return candidate_primary
        else:
            return select_member(members, SECONDARY, tag_sets, latency)

    elif mode == SECONDARY:
        for tags in tag_sets:
            candidate = select_member_with_tags(members, tags, True, latency)
            if candidate:
                return candidate

        return None

    elif mode == SECONDARY_PREFERRED:
        candidate_secondary = select_member(
            members, SECONDARY, tag_sets, latency)
        if candidate_secondary:
            return candidate_secondary
        else:
            return select_member(members, PRIMARY, [{}], latency)

    elif mode == NEAREST:
        for tags in tag_sets:
            candidate = select_member_with_tags(members, tags, False, latency)
            if candidate:
                return candidate

        # Ran out of tags.
        return None

    else:
        raise ConfigurationError("Invalid mode %s" % repr(mode))


"""Commands that may be sent to replica-set secondaries, depending on
   ReadPreference and tags. All other commands are always run on the primary.
"""
secondary_ok_commands = set([
    "group", "aggregate", "collStats", "dbStats", "count", "distinct",
    "geoNear", "geoSearch", "geoWalk", "mapreduce",
])

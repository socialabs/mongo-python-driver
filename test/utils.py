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

"""Utilities for testing pymongo
"""

from pymongo.connection import Connection
from pymongo.master_slave_connection import MasterSlaveConnection
from pymongo.replica_set_connection import ReplicaSetConnection


def delay(sec):
    # Javascript sleep() only available in MongoDB since version ~1.9
    return '''function() {
        var d = new Date((new Date()).getTime() + %s * 1000);
        while (d > (new Date())) { }; return true;
    }''' % sec

def get_command_line(connection):
    command_line = connection.admin.command('getCmdLineOpts')
    assert command_line['ok'] == 1, "getCmdLineOpts() failed"
    return command_line['argv']

def server_started_with_auth(connection):
    argv = get_command_line(connection)
    return '--auth' in argv or '--keyFile' in argv

def server_is_master_with_slave(connection):
    return '--master' in get_command_line(connection)

def drop_collections(db):
    for coll in db.collection_names():
        if not coll.startswith('system'):
            db.drop_collection(coll)

def joinall(threads):
    """Join threads with a 5-minute timeout, assert joins succeeded"""
    for t in threads:
        t.join(300)
        assert not t.isAlive(), "Thread %s hung" % t

def empty(connection):
    """Accelerate cleanup of a connection's sockets, especially any request
       sockets that haven't yet been deleted after their threads died. In
       normal use PyMongo will eventually close these, but for tests it must be
       done immediately, otherwise in Jython on Mac OS X the test suite can
       run out of file descriptors.
    """
    if isinstance(connection, Connection):
        pools = [connection._Connection__pool]
        connection.disconnect()
    elif isinstance(connection, MasterSlaveConnection):
        pools = [connection.master._Connection__pool] + [
            slave._Connection__pool for slave in connection.slaves]
        connection.disconnect()
    elif isinstance(connection, ReplicaSetConnection):
        pools = [
            mongo['pool'] for mongo in connection._ReplicaSetConnection__pools
            if 'pool' in mongo]

        # ReplicaSetConnection.disconnect closes primary, close() closes all
        connection.close()
    else:
        raise TypeError(
            'Expected connection to be a PyMongo connection type, not %s' % (
                repr(connection)))

    for pool in pools:
        for sock_info in pool._tid_to_sock.values():
            # Check that it's not the constants NO_REQUEST or NO_SOCKET_YET
            if hasattr(sock_info, 'close'):
                sock_info.close()
        pool._tid_to_sock.clear()

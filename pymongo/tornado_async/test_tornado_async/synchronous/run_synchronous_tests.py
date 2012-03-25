import sys
import nose
from nose.config import Config
from os import path
import re
import fake_pymongo
from nose.plugins import Plugin
from nose.plugins.manager import PluginManager
from nose.selector import Selector
from pymongo.tornado_async.test_tornado_async.puritanical import PuritanicalIOLoop

# TODO: running this file without a test-module name does nothing; it should run
#   all PyMongo tests except test_gevent
# TODO: test for Motor all the things test_pooling tests

excluded_modules = [
    'test.test_gevent',
    'test.test_threads',
    'test.test_threads_replica_set_connection',
    'test.test_pooling',
    'test.test_paired',

    # TODO:
    'test.test_ssl',
]

excluded_tests = [
    # TODO: For each of these, examine why the synchro test fails and either
    # fix the synchro test or test the same functionality directly in Motor,
    # or document that Motor doesn't support the functionality

    # Tests defined in specific suites that we skip because we test this
    # functionality directly on Motor, rather than via fake_pymongo
    'TestCollection.test_ensure_unique_index_threaded',
    'TestConnection.test_interrupt_signal',
    'TestConnection.test_with_start_request',
    'TestConnection.test_fork',
    'TestConnection.test_copy_db',
    # This is in test_replica_set_connection, not test_connection
    'TestConnection.test_auto_reconnect_exception_when_read_preference_is_secondary',
    'TestMasterSlaveConnection.test_continue_until_slave_works',
    'TestMasterSlaveConnection.test_disconnect',
    'TestMasterSlaveConnection.test_raise_autoreconnect_if_all_slaves_fail',
    'TestDatabase.test_authenticate_and_request',

    # TODO: document these differences b/w Motor and PyMongo (most of them
    # already have TODOs in async.py, just double-check them from this list)

    # Tests defined several places that we'll always skip because we've
    # decided not to support these features in Motor the same as in
    # PyMongo
    'test_multiprocessing',
    'test_repr',

    # Tests defined in specific suites that we skip because we've
    # decided not to support these features in Motor the same as in
    # PyMongo
    'TestConnection.test_contextlib_auto_start_request',
    'TestConnection.test_auto_start_request',
    'TestDatabase.test_system_js',
    'TestDatabase.test_system_js_list',
    'TestCursor.test_properties',
    'TestCursor.test_getitem_index_out_of_range',
    'TestGridfs.test_threaded_writes',
    'TestGridfs.test_threaded_reads',
]

class SynchroNosePlugin(Plugin):
    name = 'synchro'

    def __init__(self, *args, **kwargs):
        # We need a standard Nose selector in order to filter out methods that
        # don't match TestSuite.test_*
        self.selector = Selector(config=None)
        super(SynchroNosePlugin, self).__init__(*args, **kwargs)

    def configure(self, options, conf):
        super(SynchroNosePlugin, self).configure(options, conf)
        # TODO: Figure out less hacky way to enable this plugin
        # programmatically with Nose
        self.enabled = True

    def wantModule(self, module):
        return module.__name__ not in excluded_modules

    def wantMethod(self, method):
        if not self.selector.matches(method.__name__):
            return False

        for excluded_name in excluded_tests:
            if '.' in excluded_name:
                suite_name, method_name = excluded_name.split('.')
                if (method.im_class.__name__ == suite_name
                    and method.__name__ == method_name
                ):
                    return False
            else:
                if method.__name__ == excluded_name:
                    return False

        return True

if __name__ == '__main__':
    PuritanicalIOLoop().install()

    # Monkey-patch all pymongo's unittests so they think our fake pymongo is the
    # real one
    # TODO: try using 'from imp import new_module' instead of this?
    sys.modules['pymongo'] = fake_pymongo

    for submod in [
        'connection',
        'collection',
        'master_slave_connection',
        'replica_set_connection',
        'database',
        'pool',
    ]:
        # So that 'from pymongo.connection import Connection' gets the fake
        # Connection, not the real
        sys.modules['pymongo.%s' % submod] = fake_pymongo

    # Find our directory
    this_dir = path.dirname(__file__)

    # Find test dir
    test_dir = path.normpath(path.join(this_dir, '../../../../test'))
    print 'Running tests in %s' % test_dir

    # Exclude a test that hangs and prevents the run from completing - we should
    # fix the test for async, eventually
    # TODO: fix these, or implement a Motor-specific test that exercises the
    #   same features as each of these
    # TODO: document these variations and omissions b/w PyMongo and the Motor API
    print 'WARNING: excluding some tests -- go in and fix them for async!'

    config = Config(
        plugins=PluginManager(),
    )

    nose.main(
        config=config,
        addplugins=[SynchroNosePlugin()],
        defaultTest=test_dir,
    )

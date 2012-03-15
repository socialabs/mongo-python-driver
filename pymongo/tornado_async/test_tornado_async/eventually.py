import inspect
import os
import time
import unittest

from tornado import ioloop

class AssertEventuallyTest(unittest.TestCase):
    def setUp(self):
        super(AssertEventuallyTest, self).setUp()

        # Callbacks registered with assertEventuallyEqual()
        self.assert_callbacks = set()

    def assertEventuallyEqual(
            self, expected, fn, msg=None, timeout_sec=None
    ):
        frame_info = inspect.stack()[1]
        comment = '%s:%s in %s' % (frame_info[1], frame_info[2], frame_info[3])
        if msg is not None:
            comment += ': ' + msg

        if timeout_sec is None:
            timeout_sec = 5
        timeout_sec = max(timeout_sec, float(os.environ.get('TIMEOUT_SEC', 0)))
        start = time.time()
        loop = ioloop.IOLoop.instance()

        def callback(*args, **kwargs):
            if args or kwargs:
                pass
            try:
                self.assertEqual(expected, fn(), comment)
                # Passed
                self.assert_callbacks.remove(callback)
                if not self.assert_callbacks:
                    # All asserts have passed
                    loop.stop()
            except Exception:
                # Failed -- keep waiting?
                if time.time() - start < timeout_sec:
                    # Try again in about 0.1 seconds
                    loop.add_timeout(time.time() + 0.1, callback)
                else:
                    # Timeout expired without passing test
                    loop.stop()
                    raise

        self.assert_callbacks.add(callback)

        # Run this callback on the next I/O loop iteration
        loop.add_callback(callback)

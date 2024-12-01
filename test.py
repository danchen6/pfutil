#!python3
import unittest
import uuid

from pfutil import HyperLogLog


# Generate with Redis: `r.pfadd('t', 'a', 'b', 'c')` then `r.get('t')`
REDIS_HYPERLOGLOG_ABC = b'HYLL\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80`\xf3\x80P\xb1\x84K\xfb\x80BZ'
REDIS_HYPERLOGLOG_EMPTY = b'HYLL\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x7f\xff'


class TestHyperLogLog(unittest.TestCase):
    def test_init_empty(self):
        h = HyperLogLog()
        self.assertEqual(h.pfcount(), 0)
        self.assertEqual(h.to_bytes(), REDIS_HYPERLOGLOG_EMPTY)

    def test_init_from_bytes(self):
        h = HyperLogLog.from_bytes(REDIS_HYPERLOGLOG_ABC)
        self.assertEqual(h.pfcount(), 3)

    def test_init_from_elements(self):
        h = HyperLogLog.from_elements('x', 'y', 'z')
        self.assertEqual(h.pfcount(), 3)

    def test_pfadd(self):
        h = HyperLogLog()
        h.pfadd('x', 'y', 'z')
        for _ in range(10):
            h.pfadd(str(uuid.uuid4()))
        self.assertEqual(h.pfcount(), 13)

    def test_pfmerge(self):
        a = HyperLogLog.from_elements('a', 'b', 'c')
        b = HyperLogLog.from_elements('x', 'y', 'z')
        b.pfmerge(a)
        self.assertEqual(b.pfcount(), 6)

    def test_error_rate_fast(self):
        h = HyperLogLog()
        for _ in range(10000):
            h.pfadd(str(uuid.uuid4()))
        rate = abs(h.pfcount() - 10000) / 10000
        self.assertLessEqual(rate, 0.015)

    @unittest.skip('Slow test case')
    def test_error_rate_slow(self):
        h = HyperLogLog()
        for _ in range(1000000):
            h.pfadd(str(uuid.uuid4()))
        rate = abs(h.pfcount() - 1000000) / 1000000
        self.assertLessEqual(rate, 0.015)


class TestErrorHandling(unittest.TestCase):
    def test_from_bytes_empty(self):
        with self.assertRaises(ValueError) as e:
            h = HyperLogLog.from_bytes(b'')
            h.pfcount()
        exception_msg = e.exception.args[0]
        self.assertEqual(exception_msg, 'Invalid signature')

    def test_from_bytes_invalid(self):
        with self.assertRaises(ValueError) as e:
            h = HyperLogLog.from_bytes(b'invalid')
            h.pfcount()
        exception_msg = e.exception.args[0]
        self.assertEqual(exception_msg, 'Invalid signature')

    def test_add_integer(self):
        h = HyperLogLog()
        with self.assertRaises(TypeError) as e:
            h.pfadd(42)
        exception_msg = e.exception.args[0]
        self.assertEqual(exception_msg, 'All arguments must be strings')

    def test_from_elements_integer(self):
        with self.assertRaises(TypeError) as e:
            h = HyperLogLog.from_elements(5566)
            h.pfcount()
        exception_msg = e.exception.args[0]
        self.assertEqual(exception_msg, 'All arguments must be strings')

    def test_merge_string(self):
        h = HyperLogLog()
        with self.assertRaises(TypeError) as e:
            h.pfmerge('invalid')
        exception_msg = e.exception.args[0]
        self.assertTrue('must be pfutil.HyperLogLog' in exception_msg)

    def test_pickle_serialization(self):
        h = HyperLogLog.from_elements('widget')
        with self.assertRaises(TypeError) as e:
            import pickle
            pickle.dumps(h)
        exception_msg = e.exception.args[0]
        self.assertTrue('cannot pickle' in exception_msg)

    def test_json_serialization(self):
        h = HyperLogLog.from_elements('widget')
        with self.assertRaises(TypeError) as e:
            import json
            json.dumps(h)
        exception_msg = e.exception.args[0]
        self.assertTrue('is not JSON serializable' in exception_msg)


@unittest.skip('Test with Redis is not enabled')
class TestRedisCompatibility(unittest.TestCase):
    def setUp(self):
        import redis
        self.r = redis.Redis()

    def test_load_from_redis(self):
        self.r.delete('t')
        self.r.pfadd('t', 'a', 'b', 'c')
        h = HyperLogLog.from_bytes(self.r.get('t'))
        self.assertEqual(h.pfcount(), 3)

    def test_store_to_redis(self):
        h = HyperLogLog.from_elements('a', 'b', 'c')
        self.r.set('t', h.to_bytes())
        self.assertEqual(self.r.pfcount('t'), 3)


if __name__ == '__main__':
    unittest.main()

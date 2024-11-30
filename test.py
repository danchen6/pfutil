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

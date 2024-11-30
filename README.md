pfutil
======

Fast [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) extension for Python 3.  The underlying binary representation is compatible with [Redis](https://redis.io).

```python
#!python3
import redis
from pfutil import HyperLogLog

r = redis.Redis()
r.pfadd('foo', 'a', 'b', 'c')
r.pfadd('bar', 'x', 'y', 'z')
r.pfmerge('bar', 'foo')
assert r.pfcount('foo') == 3
assert r.pfcount('bar') == 6

foo = HyperLogLog.from_bytes(r.get('foo'))
bar = HyperLogLog.from_elements('x', 'y')
bar.pfadd('z')
bar.pfmerge(foo)
assert foo.pfcount() == 3
assert bar.pfcount() == 6
assert r.get('bar') == bar.to_bytes()
```


Install
-------

Install from [PyPI](https://pypi.org/project/pfutil/):
```
pip install pfutil
```

Install from source:
```
python setup.py install
```


License
-------

* This `pfutil` software is released under the [3-Clause BSD License](https://opensource.org/license/bsd-3-clause)
* The files in `src/redis/` are extracted and modified from [Redis 6.2.12](https://github.com/redis/redis/tree/6.2.12), which is released under the 3-Clause BSD License as well.

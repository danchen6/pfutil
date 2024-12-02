from pathlib import Path
from setuptools import setup, Extension

module = Extension(
    'pfutil',
    sources=['src/pfutil.c', 'src/redis/sds.c', 'src/redis/hyperloglog.c'],
    include_dirs=['src', 'src/redis'],
)

setup(
    name='pfutil',
    version='1.0.3',
    description='Fast and Redis-compatible HyperLogLog extension for Python 3',
    author='Dan Chen',
    author_email='danchen666666@gmail.com',
    url='https://github.com/danchen6/pfutil',
    long_description=(Path(__file__).parent / 'README.md').read_text(),
    long_description_content_type='text/markdown',
    license='3-Clause BSD License',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: C',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
    ],
    ext_modules=[module],
)

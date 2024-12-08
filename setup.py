import os

from setuptools import setup, Extension


readme_md_content = None
with open(os.path.join(os.path.dirname(__file__), 'README.md')) as f:
    readme_md_content = f.read()

module = Extension(
    'pfutil',
    sources=['src/pfutil.c', 'src/redis/sds.c', 'src/redis/hyperloglog.c'],
    include_dirs=['src', 'src/redis'],
    extra_compile_args=['-std=c99'],  # for older GCC
)

setup(
    name='pfutil',
    version='1.0.4',
    description='Fast and Redis-compatible HyperLogLog extension for Python 3',
    author='Dan Chen',
    author_email='danchen666666@gmail.com',
    url='https://github.com/danchen6/pfutil',
    long_description=readme_md_content,
    long_description_content_type='text/markdown',
    license='3-Clause BSD License',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: C',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
    ],
    ext_modules=[module],
)

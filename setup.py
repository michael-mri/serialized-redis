#!/usr/bin/env python
import os
import sys

from serialized_redis import __version__

try:
    from setuptools import setup
    from setuptools.command.test import test as TestCommand

    class PyTest(TestCommand):
        def finalize_options(self):
            TestCommand.finalize_options(self)
            self.test_args = []
            self.test_suite = True

        def run_tests(self):
            # import here, because outside the eggs aren't loaded
            import pytest
            errno = pytest.main(self.test_args)
            sys.exit(errno)

except ImportError:

    from distutils.core import setup

    def PyTest(x):
        x

f = open(os.path.join(os.path.dirname(__file__), 'README.rst'))
long_description = f.read()
f.close()

setup(
    name='serialized-redis-interface',
    version=__version__,
    description='Redis python interface that serializes all values using json, pickle, msgpack or a custom serializer.',
    long_description=long_description,
    url='https://github.com/michael-mri/serialized-redis',
    author='Michael Rigoni',
    author_email='michael.rigoni@gmail.com',
    maintainer='Michael Rigoni',
    maintainer_email='michael.rigon@igmail.com',
    keywords=['Redis', 'key-value store', 'json', 'pickle', 'msgpack'],
    license='MIT',
    packages=['serialized_redis'],
    python_requires='>=3',
    install_requires=['redis'],
    extras_require={
        'msgpack': ['msgpack'],
    },
    tests_require=[
        'mock',
        'pytest>=2.5.0',
        'msgpack',
    ],
    cmdclass={'test': PyTest},
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)

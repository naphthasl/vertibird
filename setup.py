#!/usr/bin/env python

import sys
from setuptools import setup

if sys.version_info < (2, 7):
    raise NotImplementedError("Sorry, Python 2.X isn't supported.")

import vertibird

setup(name='vertibird',
    version=vertibird.__version__,
    description='A really, really simple Python virtualization module which interfaces directly with QEMU.',
    long_description=vertibird.__doc__,
    long_description_content_type="text/markdown",
    author=vertibird.__author__,
    author_email='naphtha@lotte.link',
    url='https://github.com/naphthasl/vertibird',
    py_modules=['vertibird'],
    license=vertibird.__license__,
    install_requires=[
        'sqlalchemy',
        'pillow-simd',
        'vncdotool',
        'numpy',
        'opencv-python',
        'psutil',
        'filelock',
        'pyzmq',
        'python-dateutil'
    ],
    platforms='any',
    python_requires='>=3.6',
    classifiers=[
            'Intended Audience :: Developers',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
        ],
    )

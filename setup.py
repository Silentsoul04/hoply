#!/usr/bin/env python
import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='hoply',
    version='0.10.1',
    author='Amirouche Boubekki',
    author_email='amirouche@hypermove.net',
    url='https://github.com/amirouche/hoply',
    description='Explore relational data',
    long_description=read('README.rst'),
    py_modules=['hoply'],
    zip_safe=False,
    license='GPLv2 or GPLv3',
    install_requires=[
        "wiredtiger-ffi==3.1.0.1",
        "immutables==0.6",
        "daiquiri==1.5.0",
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering',
    ],
)

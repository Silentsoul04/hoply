#!/usr/bin/env python
import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='AjguDB',
    version='0.10.0',
    author='Amirouche Boubekki',
    author_email='amirouche@hypermove.net',
    url='https://github.com/amirouche/ajgudb',
    description='Explore you connected data',
    long_description=read('README.rst'),
    py_modules=['ajgudb'],
    zip_safe=False,
    license='Apache 2',
    install_requires=[
        "plyvel>=1.0",
    ],
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Software Development',
        'Programming Language :: Python :: 3',
    ],
)

#!/usr/bin/env python
import os
from setuptools import setup
from setuptools import find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="hoply",
    version="0.13.0",
    author="Amirouche Boubekki",
    author_email="amirouche@hypermove.net",
    url="https://github.com/amirouche/hoply",
    description="Explore relational data",
    long_description=read("README.rst"),
    packages=find_packages(),
    zip_safe=False,
    license="GPLv2 or GPLv3",
    install_requires=["immutables==0.6", "daiquiri==1.5.0", "sortedcontainers==2.0.5"],
    classifiers=[
        "Intended Audience :: Developers",
        "Topic :: Software Development",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering",
    ],
)

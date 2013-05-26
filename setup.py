# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name='thrush',
    version='0.1',
    packages=["thrush"],

    # package metadata
    author="Tobias Heinzen",
    author_email="tobias.heinzen@0xdeadbeef.ch",
    url="http://dev.0xdeadbeef.ch/thrush.git",
    description="Alternative OO wrapper for rrdtool",
    license="BSD",
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)


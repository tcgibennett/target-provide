#!/usr/bin/env python

from setuptools import setup

setup(name='target-provide',
    version='0.1.0',
    description='Singer.io target for writing to Provide Platform',
    author='Thomas Bennett',
    url='',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_provide'],
    install_requires=[
        'jsonschema==2.6.0',
        'singer-python==2.1.4',
        'provide-python==0.0.3'
    ],
    entry_points='''
        [console_scripts]
        target-provide=target_provide:main
    ''',
)
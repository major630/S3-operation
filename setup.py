#!/usr/bin/env python

from distutils.core import setup

setup(
    name='s3-operation',
    version='1.0.0',
    description='encapsulate API of AWS s3',
    long_description='''Encapsulate API of AWS s3. Just make the use more comforable and will
not make many changes if AWS update the APIs ''',
    author='Barry Shi',
    author_email='major630@163.com',
    url='',
    license='MIT',
    platforms='Linux',
    keywords=['s3','boto3','aws','python'],
    py_modules=['s3-operation'],
)

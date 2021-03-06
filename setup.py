#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import censuslib


if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as f:
    readme = f.read()

packages = [
    'censuslib'
]

scripts=[ ]

package_data = {} #{"": ['*.html', '*.css', '*.rst']}

requires = [ ]

classifiers = [
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
]

setup(
    name='ambry-censuslib',
    version=censuslib.__version__,
    description='Common support for building census bundles',
    long_description=readme,
    packages=packages,
    package_data=package_data,
    scripts=scripts,
    install_requires=requires,
    author=censuslib.__author__,
    author_email='eric@civicknowledge',
    url='https://github.com/CivicKnowledge/censuslib',
    license='LICENSE',
    classifiers=classifiers,
)
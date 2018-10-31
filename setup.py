#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Note: To use the 'upload' functionality of this file, you must:
#   $ pip install twine

import os
import sys
from shutil import rmtree

from setuptools import find_packages, setup, Command

# Package meta-data.
NAME = 'gsheetsdb'
DESCRIPTION = 'Python DB-API and SQLAlchemy interface for Google Spreadsheets.'
URL = 'https://github.com/betodealmeida/gsheets-db-api'
EMAIL = 'beto@dealmeida.net'
AUTHOR = 'Beto Dealmeida'

# What packages are required for this module to be executed?
REQUIRED = [
    'google-auth',
    'moz_sql_parser',
    'requests>=2.20.0',
    'six',
]
if sys.version_info < (3, 4):
    REQUIRED.append('enum34')

sqlalchemy_extras = [
    'sqlalchemy',
]

cli_extras = [
    'docopt',
    'pygments',
    'prompt_toolkit>=2',
    'tabulate',
]

development_extras = [
    'coverage',
    'flake8',
    'nose',
    'pipreqs',
    'pytest>=2.8',
    'pytest-cov',
    'requests_mock',
    'twine',
]
if sys.version_info < (3, 3):
    development_extras.append('mock')


# The rest you shouldn't have to touch too much :)
# ------------------------------------------------
# Except, perhaps the License and Trove Classifiers!
# If you do change the License, remember to change the Trove Classifier for
# that!

here = os.path.abspath(os.path.dirname(__file__))

long_description = ''

# Load the package's __version__.py module as a dictionary.
about = {}
with open(os.path.join(here, NAME, '__version__.py')) as f:
    exec(f.read(), about)


class UploadCommand(Command):
    """Support setup.py upload."""

    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print('\033[1m{0}\033[0m'.format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status('Removing previous builds…')
            rmtree(os.path.join(here, 'dist'))
        except OSError:
            pass

        self.status('Building Source and Wheel (universal) distribution…')
        os.system(
            '{0} setup.py sdist bdist_wheel --universal'.format(
                sys.executable))

        self.status('Uploading the package to PyPi via Twine…')
        os.system('twine upload dist/*')

        sys.exit()


# Where the magic happens:
setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    author=AUTHOR,
    author_email=EMAIL,
    url=URL,
    packages=find_packages(exclude=('tests',)),
    # If your package is a single module, use this instead of 'packages':
    # py_modules=['mypackage'],

    entry_points={
        'console_scripts': [
            'gsheetsdb = gsheetsdb.console:main',
        ],
        'sqlalchemy.dialects': [
            'gsheets = gsheetsdb.dialect:GSheetsDialect',
        ],
    },
    install_requires=REQUIRED,
    extras_require={
        'cli': cli_extras,
        'dev': development_extras,
        'sqlalchemy': sqlalchemy_extras,
    },
    include_package_data=True,
    license='MIT',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ],
    # $ setup.py publish support.
    cmdclass={
        'upload': UploadCommand,
    },
)

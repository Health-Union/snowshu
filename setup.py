#!/usr/local/bin/python3

import os

from setuptools import find_packages, setup

VERSION = '0.0.3'
PYTHON_REQUIRES = '3.8'

packagedata = {
    'include_package_data': True,
    'name': "snowshu",
    'version': VERSION,
    'author': "Health Union Data Team",
    'author_email': 'data@health-union.com',
    'url': 'https://snowshu.readthedocs.io/en/master/index.html',
    'description': "Sample image management for data transform TDD.",
    'classifiers': ["Development Status :: 4 - Beta", "License :: OSI Approved :: Apache Software License",
                    "Operating System :: OS Independent"],
    'python_requires': f'>={PYTHON_REQUIRES}',
    'install_requires': [],
    'packages': find_packages(exclude=['tests', ]),
    'entry_points': {'console_scripts': ['snowshu=snowshu.core.main:cli']}
}

with open('./README.md') as readme:
    packagedata['long_description'] = readme.read()
    packagedata['long_description_content_type'] = 'text/markdown'

for file_name in ['base.txt', 'snowflake_pins.txt']:
    with open(f'./requirements/{file_name}', 'r') as requirements:
        for line in requirements.readlines():
            if not line.startswith('-r'):
                packagedata['install_requires'].append(line)

setup(**packagedata)

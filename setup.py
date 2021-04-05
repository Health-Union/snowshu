#!/usr/local/bin/python3

import os

from setuptools import find_packages, setup

VERSION='0.0.3b2'
PYTHON_REQUIRES='3.7'

packagedata=dict()

packagedata['include_package_data']=True
packagedata['name']="snowshu"
packagedata['version']=VERSION
packagedata['author']="Health Union Data Team"
packagedata['author_email']='data@health-union.com'
packagedata['url']='https://snowshu.readthedocs.io/en/master/index.html'
packagedata['description']="Sample image management for data transform TDD."
packagedata['classifiers']=["Development Status :: 4 - Beta", "License :: OSI Approved :: Apache Software License", "Operating System :: OS Independent"]
packagedata['python_requires']=f'>={PYTHON_REQUIRES}'
packagedata['install_requires']=list()
packagedata['packages']=find_packages(exclude=['tests',])
packagedata['entry_points']=dict(console_scripts=['snowshu= snowshu.core.main:cli'])

with open('./README.md','r') as readme:
    packagedata['long_description']=readme.read()
    packagedata['long_description_content_type']='text/markdown'

for file_name in ['base.txt', 'snowflake_pins.txt']:
    with open(f'./requirements/{file_name}', 'r') as requirements:
        for line in requirements.readlines():
            if not line.startswith('-r'):
                packagedata['install_requires'].append(line)

setup(**packagedata)

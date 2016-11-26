#!/usr/bin/python3

import sys
import os
from setuptools import setup, find_packages


NAME = 'selinonlib'


def get_requirements():
    with open('requirements.txt') as fd:
        return fd.read().splitlines()


def get_version():
    with open(os.path.join(NAME, 'version.py')) as f:
        version = f.readline()
    return version.split(' = ')[1]


if sys.version_info[0] != 3:
    sys.exit("Python3 is required in order to install selinonlib")

setup(
    name=NAME,
    version='0.1.0rc3',
    packages=find_packages(),
    scripts=['selinonlib-cli'],
    install_requires=get_requirements(),
    author='Fridolin Pokorny',
    author_email='fpokorny@redhat.com',
    maintainer='Fridolin Pokorny',
    maintainer_email='fpokorny@redhat.com',
    description='a simple tool to visualize, check and generate Python code from a YAML configuration file for Selinon'
                ' dispatcher for Celery',
    url='https://github.com/selinon/selinonlib',
    license='GPLv2+',
    keywords='node task graph edge celery selinon yaml condition',
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language:: Python:: 3",
        "Programming Language:: Python:: 3.4",
        "Programming Language:: Python:: 3.5",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)",
        "Operating System :: OS Independent",
        "Topic :: System :: Distributed Computing",
        "Programming Language:: Python:: Implementation:: CPython",
        "Programming Language:: Python:: Implementation:: PyPy"
    ]
)

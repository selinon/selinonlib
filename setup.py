#!/usr/bin/python3

import sys
from setuptools import setup, find_packages

# Note: We are not distributing examples/ for now


def get_requirements():
    with open('requirements.txt') as fd:
        return fd.read().splitlines()

if sys.version_info[0] != 3:
    sys.exit("Python3 is required in order to install selinonlib")

setup(
    name='selinonlib',
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
    url='https://github.com/fridex/selinonlib',
    license='GPL',
    keywords='node task graph edge celery selinon yaml condition',
)

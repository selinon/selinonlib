#!/usr/bin/python

import subprocess
from parsley import parsley_version
from setuptools import setup, find_packages

# Note: We are not distributing examples/ for now


def get_requirements():
    with open('requirements.txt') as fd:
        return fd.read().splitlines()


subprocess.call("sh ./bump-version.sh")


setup(
    name='parsley',
    version=parsley_version,
    packages=find_packages(),
    scripts=['parsley-cli'],
    install_requires=get_requirements(),
    author='Fridolin Pokorny',
    author_email='fpokorny@redhat.com',
    maintainer='Fridolin Pokorny',
    maintainer_email='fpokorny@redhat.com',
    description='a simple tool to visualize, check and generate Python code from a YAML configuration file for Celeriac'
                ' dispatcher for Celery',
    url='https://github.com/fridex/parsley',
    license='GPL',
    keywords='node task graph edge celery celeriac yaml condition',
)

# Selinonlib

A simple tool to visualize, check dependencies and generate Python code for [Selinon](https://github.com/selinon/selinon). You can find generated Sphinx documentation [here](https://selinonlib.readthedocs.io). Project is hosted on [Github](https://github.com/selinon/selinonlib).

![PyPI Current Version](https://img.shields.io/pypi/v/selinonlib.svg)
![PyPI Implementation](https://img.shields.io/pypi/implementation/selinonlib.svg)
![PyPI Wheel](https://img.shields.io/pypi/wheel/selinonlib.svg)
![Travis CI](https://travis-ci.org/selinon/selinonlib.svg?branch=master)
![GitHub stars](https://img.shields.io/github/stars/selinon/selinonlib.svg)
![GitHub license](https://img.shields.io/badge/license-GPLv2-blue.svg)
![Twitter](https://img.shields.io/twitter/url/http/github.com/selinon/selinonlib.svg?style=social)

Is this project helpful? [Send me a simple warm message](https://saythanks.io/to/fridex)!

## Crossroad

 * [PyPI](https://pypi.python.org/pypi/selinonlib)
 * [Developers documentation](https://selinonlib.readthedocs.io)
 * [Travis CI](https://travis-ci.org/selinon/selinonlib)

## The Idea

A system consists of two main parts:
  * nodes
  * directed edges with conditions
  
A node can produce or accept a message. Each node is uniquely identified by its name which has to be unique in defined system. There are available two main types of nodes:
  * task
  * flow
  
A flow consists of tasks and each flow can be seen as a task (a black box) so flows can be used inside another flows as desired.

Conditions are made of predicates that can be used with logical operators *and*, *or* or *not*. You can run multiple tasks based on conditions or you can inspect multiple results of tasks in order to proceed with computation in the flow.

Cyclic dependencies on tasks and flows are fully supported. See [Selinon](https://github.com/selinon/selinon) for more info and examples.

## Installation

```
$ pip3 install selinonlib
```

## FAQ

### Why is this tool useful?

See [Selinon](https://github.com/selinon/selinon) for usage examples. This tool is intended to automatically generate Python code from a YAML configuration file, perform additional consistency checks or plot flow graphs. It also provides a Selinon user a pack of predefined storage/database adapters and other tools suitable for user-specific Selinon configuration.

### Examples:

Plot graphs of flows:
```
$ selinonlib-cli -vvv plot --nodes-definitions nodes.yml --flow-definitions flow1.yml flow2.yml --format svg --output-dir ./ && xdg-open flow1.svg
```

Generate Python code configuration for Selinon:
```
$ selinonlib-cli -vvv inspect --nodes-definitions nodes.yml --flow-definitions flow1.yml flow2.yml --dump out.py && cat out.py
```

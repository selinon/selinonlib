# Selinonlib

A simple tool to visualize, check dependencies and generate Python code for [Selinon](https://github.com/fridex/selinon). You can find generated Sphinx documentation [here](https://fridex.github.io/selinonlib). Project is hosted on [Github](https://github.com/fridex/selinon).

## The Idea

A system consists of two main parts:
  * nodes
  * directed edges with conditions
  
A node can produce or accept a message. Each node is uniquely identified by its name which has to be unique in defined system. There are available two main types of nodes:
  * task
  * flow
  
A flow consists of tasks and each flow can be seen as a task (a black box) so flows can be used inside another flows as desired.

Conditions are made of predicates that can be used with logical operators *and*, *or* or *not*. You can run multiple tasks based on conditions or you can inspect multiple results of tasks in order to proceed with computation in the flow.

Cyclic dependencies on tasks and flows are fully supported. See [Selinon](https://github.com/fridex/selinon) for more info and examples.

## Installation

```
$ pip3 install selinonlib
```

## FAQ

### Why is this tool useful?

See [Selinon](https://github.com/fridex/selinon) dispatcher for usage examples. This tool is intended to automatically generate Python code from a YAML configuration file, perform additional consistency checks or plot flow graphs.

### Examples:

Plot graphs of flows:
```
$ ./selinonlib-cli -tasks-definition your_example.yml -flow-definition your_example_flows.yml -verbose -graph ${PWD} && xdg-open flow1.svg
```

Generate Python code configuration for Selinon:
```
$ ./selinonlib-cli -tasks example/example.yaml -flow examples/examples.yaml -v -dump dump.py && cat dump.py
```

Plot graphs of flows with a custom style (you can use shortcuts of arguments as shown bellow, refer to graphviz library for style configuration options):
```
$ ./selinonlib-cli -config example.config.yml -tasks exampes/example.yml -flow examples/example.yml -v -graph . && xdg-open flow1.svg
```

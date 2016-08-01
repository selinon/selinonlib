# Parsley

A simple tool to visualize, check dependencies and generate Python code for [Celeriac](https://github.com/fridex/celeriac).

## The Idea

A system consists of two main parts:
  * nodes
  * directed edges with conditions
  
A node can produce or accept a message. Each node is uniquely identified by its name which has to be unique in defined system. There are available two main types of nodes:
  * task
  * flow
  
A flow consists of tasks and each flow can be seen as a task (a black box) so flows can be used inside another flows as desired. The only restriction is the fact that you cannot inspect results of a flow since the flow acts like a black box.

Conditions are made of predicates that can be used with logical operators *and*, *or* or *not*. You can run multiple tasks based on conditions or you can inspect multiple results of tasks in order to proceed with computation in the flow.

Cyclic dependencies on tasks and flows are fully supported.

## Configuration File

Refer to examples/example.yml configuration with comments for more info. There are also available outputs in examples/ directory.

## Installation

*Currently not available!*
```
$ pip install parsley
```

or see *A Quick First Touch* section to use directly git repo.

## FAQ

### Why is this tool useful?

See [Celeriac](https://github.com/fridex/celeriac) dispatcher for usage examples. This tool is intended to automatically generate Python code from a YAML configuration file, perform additional consistency checks or plot flow graphs.

### I'm getting warning: "Multiple starting nodes found in a flow". Why?

In order to propagate arguments to a flow, you should start flow with one single task (e.g. init task) which result is then propagated as an argument to each direct child tasks or transitive child tasks. This avoids various inconsistency errors and race conditions. If you define multiple starting nodes, arguments are not propagated from the first task. If you don't want to propagate arguments from an init task, you can ignore this warning for a certain flow or specify arguments explicitly in Celeriac dispatcher.

### How should I name tasks and flows?

You should use names that can became part of function name (or Python identifier). Keep in mind that there is no strict difference between tasks and (sub)flows, so they share name space.

### How can I access nested keys in a dict - e.g. ```message['foo']['bar'] == "baz"```?

Predicates were designed to deal with this - just provide list of keys, where position in a list describes key position:
```yaml
condition:
    name: "fieldEqual"
    args:
        key:
            - "foo"
            - "bar"
        value: "baz"
        
```

### What exceptions can predicates raise?

Predicates were designed to return *always* True/False. If a condition cannot be satisfied, there is returned False. So it is safe for example to access possibly non-existing keys - predicates will return False. But there can be raised exceptions if there is problem with a database - see [https://github.com/fridex/celeriac](Celeriac)'s DBAdapter.


## A Quick First Touch

Clone the repo:
```
$ git clone https://github.com/fridex/parsley && cd parsley
```

Install requirements:
```
$ pip install -r requirements.txt
```

Or use virtualenv:
```
make venv && source venv/bin/activate
```

#### Examples:

Plot graphs of flows:
```
$ ./parsley-cli -tasks-definition exampes/example.yml -flow-definition examples/example.yml -verbose -graph ${PWD} && xdg-open flow{1,2}.svg
```

Generate Python code configuration for Celeriac:
```
$ ./parsley-cli -tasks example/example.yaml -flow examples/examples.yaml -v -dump dump.py && cat dump.py
```

Plot graphs of flows with a custom style (you can use shortcuts of arguments as shown bellow):
```
$ ./parsley-cli -config examples/example.config.yml -tasks exampes/example.yml -flow examples/example.yml -v -graph . && xdg-open flow{1,2}.svg
```

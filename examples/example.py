#!/usr/bin/env python3
# auto-generated using Parsley v3dde2e3

from parsley.predicates import fieldGreater, fieldNone, alwaysTrue, fieldEqual

# Tasks
from worker.task1 import Task1
from worker.task2 import Task2
from worker.task3 import Task3Class
from worker.task1 import Task1
from worker.task5 import Task5

# Storages
from storage.storage1 import Storage1
from storage.storage2 import storage2


def get_task_class(name):
    if name == 'Task1':
        return Task1()

    if name == 'Task2':
        return Task2()

    if name == 'Task3':
        return Task3Class()

    if name == 'Task4':
        return Task1()

    if name == 'Task5':
        return Task5()

    raise ValueError("Unknown task with name '%s'" % name)

def get_storage(name):
    if name == 'storage1':
        return Storage1(username='scams', connection=[{'host': 'os1.socuc.lan', 'port': 5432}, {'host': 'os2.socuc.lan', 'port': 5432}], password='shadowedentity')

    if name == 'storage2':
        return storage2(username='bae', connection={'host': 'socuc.lan', 'port': 5432}, password='imissmybae')

    raise ValueError("Unknown storage with name '%s'" % name)

def is_flow(name):
    return name in ['flow1', 'flow2', 'flow3']

################################################################################

output_schemas = {,
    'Task5': 'path/to/output/schema.json'
}

################################################################################

def init_max_retry():
    Task1.max_retry = 1
    Task2.max_retry = 1
    Task3Class.max_retry = 1
    Task1.max_retry = 1
    Task5.max_retry = 2

################################################################################

def init_time_limit():
    Task1.time_limit = 3600
    Task2.time_limit = 3600
    Task3Class.time_limit = 3600
    Task1.time_limit = 3600
    Task5.time_limit = 3600

################################################################################

def init_get_storages():
    Task1.get_storage = get_storage
    Task2.get_storage = get_storage
    Task3Class.get_storage = get_storage
    Task5.get_storage = get_storage

################################################################################

def init_output_schemas():
    Task1.output_schema_path = None
    Task1.output_schema = None
    Task2.output_schema_path = None
    Task2.output_schema = None
    Task3Class.output_schema_path = None
    Task3Class.output_schema = None
    Task5.output_schema_path = "path/to/output/schema.json"
    Task5.output_schema = None

################################################################################

def init():
    init_max_retry()
    init_time_limit()
    init_get_storages()
    init_output_schemas()

################################################################################

def _condition_flow1_0(db):
    return alwaysTrue()


def _condition_flow1_1(db):
    return alwaysTrue()


def _condition_flow2_0(db):
    return alwaysTrue()


def _condition_flow2_1(db):
    return fieldEqual(db.get('storage1', 'flow2', 'Task3'), key=['foo', 'bar'], value='baz')


def _condition_flow2_2(db):
    return (
(not 
fieldEqual(db.get('storage1', 'flow2', 'Task4'), key='foo', value=True)) and 
fieldGreater(db.get('storage1', 'flow2', 'Task4'), key='bar', value=5))


def _condition_flow3_0(db):
    return alwaysTrue()


def _condition_flow3_1(db):
    return fieldEqual(db.get('storage1', 'flow3', 'Task3'), key=['foo', 'bar'], value='baz')


def _condition_flow3_2(db):
    return (
(not 
fieldEqual(db.get('storage1', 'flow3', 'Task4'), key='foo', value=True)) and 
fieldGreater(db.get('storage1', 'flow3', 'Task4'), key='bar', value=5))


def _condition_flow3_3(db):
    return (not 
fieldNone(db.get('storage2', 'flow3', 'Task5'), key='foo'))


################################################################################

edge_table = {
    'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': _condition_flow1_0},
              {'from': [], 'to': ['Task1'], 'condition': _condition_flow1_1}],
    'flow2': [{'from': [], 'to': ['Task3'], 'condition': _condition_flow2_0},
              {'from': ['Task3'], 'to': ['Task4', 'flow1'], 'condition': _condition_flow2_1},
              {'from': ['Task4', 'flow1'], 'to': ['Task5'], 'condition': _condition_flow2_2}],
    'flow3': [{'from': [], 'to': ['Task3'], 'condition': _condition_flow3_0},
              {'from': ['Task3'], 'to': ['Task4', 'flow1'], 'condition': _condition_flow3_1},
              {'from': ['Task4'], 'to': ['Task5'], 'condition': _condition_flow3_2},
              {'from': ['Task5'], 'to': ['Task3'], 'condition': _condition_flow3_3}]
}

################################################################################


#!/usr/bin/env python
# auto-generated using Parsley v0.1.0a1

from parsley.predicates import *
from worker.task1 import Task1
from worker.task2 import Task2
from worker.task3 import Task3Class
from worker.task4 import Task4
from worker.task5 import Task5


def get_task_instance(name):
    if name == 'Task1':
        return Task1()

    if name == 'Task2':
        return Task2()

    if name == 'Task3':
        return Task3Class()

    if name == 'Task4':
        return Task4()

    if name == 'Task5':
        return Task5()

def is_flow(name):
    return name in ['flow1', 'flow2', 'flow3']

################################################################################

def _condition_flow1_0(db):
    return alwaysTrue()


def _condition_flow1_1(db):
    return alwaysTrue()


def _condition_flow2_0(db):
    return alwaysTrue()


def _condition_flow2_1(db):
    return fieldEqual(db['Task3'], key=['foo', 'bar'], value='baz')


def _condition_flow2_2(db):
    return ((not fieldEqual(db['Task4'], key='foo', value=True)) and fieldGreater(db['Task4'], key='bar', value=5))

def _condition_flow3_0(db):
    return alwaysTrue()


def _condition_flow3_1(db):
    return fieldEqual(db['Task3'], key=['foo', 'bar'], value='baz')


def _condition_flow3_2(db):
    return ((not fieldEqual(db['Task4'], key='foo', value=True)) and fieldGreater(db['Task4'], key='bar', value=5))


def _condition_flow3_3(db):
    return (not fieldNone(db['Task5'], key='foo'))


################################################################################

edge_table = {
    'flow1': [{'nodes_from': ['Task1'], 'nodes_to': ['Task2'], 'condition': _condition_flow1_0},
              {'nodes_from': [], 'nodes_to': ['Task1'], 'condition': _condition_flow1_1}],
    'flow2': [{'nodes_from': [], 'nodes_to': ['Task3'], 'condition': _condition_flow2_0},
              {'nodes_from': ['Task3'], 'nodes_to': ['Task4', 'flow1'], 'condition': _condition_flow2_1},
              {'nodes_from': ['Task4', 'flow1'], 'nodes_to': ['Task5'], 'condition': _condition_flow2_2}],
    'flow3': [{'nodes_from': [], 'nodes_to': ['Task3'], 'condition': _condition_flow3_0},
              {'nodes_from': ['Task3'], 'nodes_to': ['Task4', 'flow1'], 'condition': _condition_flow3_1},
              {'nodes_from': ['Task4'], 'nodes_to': ['Task5'], 'condition': _condition_flow3_2},
              {'nodes_from': ['Task5'], 'nodes_to': ['Task3'], 'condition': _condition_flow3_3}]
}

################################################################################


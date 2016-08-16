#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################

import codegen
import graphviz
import os
import yaml
from .task import Task
from .storage import Storage
from .flow import Flow
from .edge import Edge
from .version import parsley_version
from .config import Config
from .logger import Logger
from .helpers import dict2strkwargs
from .leafPredicate import LeafPredicate


_logger = Logger.get_logger(__name__)


class System(object):
    """
    The representation of the whole system
    """
    def __init__(self, tasks=None, flows=None, storages=None):
        self._flows = flows if flows else []
        self._tasks = tasks if tasks else []
        self._storages = storages if storages else []

    @property
    def tasks(self):
        """
        :return: tasks available in the system
        :rtype: List[Task]
        """
        return self._tasks

    @property
    def flows(self):
        """
        :return: flows available in the system
        :rtype: List[Flow]
        """
        return self._flows

    def _check_name_collision(self, name):
        """
        Tasks and Flows share name space, check for collisions
        :param name: a node name
        :raises: ValueError
        """
        if any(flow.name == name for flow in self._flows):
            raise ValueError("Unable to add node with name '%s', a flow with the same name already exist" % name)
        if any(task.name == name for task in self._tasks):
            raise ValueError("Unable to add node with name '%s', a task with the same name already exist" % name)

    def add_task(self, task):
        """
        Register a task in the system
        :param task: a task to be registered
        :type task: Task
        """
        self._check_name_collision(task.name)
        self._tasks.append(task)

    def add_flow(self, flow):
        """
        Register a flow in the system
        :param flow: a flow to be registered
        :type flow: flow
        """
        self._check_name_collision(flow.name)
        self._flows.append(flow)

    def add_storage(self, storage):
        # We need to check for name collision with tasks as well since we import them by name
        for task in self._tasks:
            if task.name == storage.name:
                raise ValueError("Storage has same name as task '%s'" % storage.name)

        for stored_storage in self._storages:
            if stored_storage.name == storage:
                raise ValueError("Multiple storages of the same name '{}'".format(storage.name))
        self._storages.append(storage)

    def storage_by_name(self, name, graceful=False):
        for storage in self._storages:
            if storage.name == name:
                return storage
        if not graceful:
            raise KeyError("Storage with name {} not found in the system".format(name))
        return None

    def task_by_name(self, name, graceful=False):
        """
        Find a task by its name
        :param name: a task name
        :param graceful: if True, no exception is raised if task couldn't be found
        :return: task with name 'name'
        :rtype: Task
        :raises: KeyError
        """
        for task in self._tasks:
            if task.name == name:
                return task
        if not graceful:
            raise KeyError("Task with name {} not found in the system".format(name))
        return None

    def flow_by_name(self, name, graceful=False):
        """
        Find a flow by its name
        :param name: a flow name
        :param graceful: if True, no exception is raised if flow couldn't be found
        :return: flow with name 'name'
        :rtype: Flow
        :raises: KeyError
        """
        for flow in self._flows:
            if flow.name == name:
                return flow
        if not graceful:
            raise KeyError("Flow with name '{}' not found in the system".format(name))
        return None

    def node_by_name(self, name, graceful=False):
        """
        Find a node (flow or task) by its name
        :param name: a node name
        :param graceful: if True, no exception is raised if node couldn't be found
        :return: flow with name 'name'
        :rtype: Node
        :raises: KeyError
        """
        node = self.task_by_name(name, graceful=True)

        if not node:
            node = self.flow_by_name(name, graceful=True)

        if not node and not graceful:
            raise KeyError("Entity with name '{}' not found in the system".format(name))

        return node

    def _dump_imports(self, output):
        """
        Dump used imports of tasks to a stream
        :param output: a stream to write to
        """
        predicates = set([])
        for flow in self._flows:
            for edge in flow.edges:
                predicates.update([p.__name__ for p in edge.predicate.predicates_used()])
        output.write('from parsley.predicates import %s\n' % ", ".join(predicates))

        output.write("\n# Tasks\n")

        for task in self._tasks:
            output.write("from {} import {}\n".format(task.import_path, task.class_name))

        output.write("\n# Storages\n")

        for storage in self._storages:
            if len(storage.tasks) > 0:
                output.write("from {} import {}\n".format(storage.import_path, storage.class_name))

        output.write('\n\n')

    def _dump_is_flow(self, output):
        """
        Dump is_flow() check to a stream
        :param output: a stream to write to
        """
        output.write('def is_flow(name):\n')
        output.write('    return name in %s\n\n' % str([flow.name for flow in self._flows]))

    def _dump_output_schemas(self, output):
        """
        Dump output schema mapping to a stream
        :param output: a stream to write to
        """
        output.write('output_schemas = {')
        for i, task in enumerate(self._tasks):
            if task.output_schema:
                if i > 0:
                    output.write(",\n    '%s': '%s'" % (task.name, task.output_schema))
                else:
                    output.write("\n    '%s': '%s'" % (task.name, task.output_schema))
        output.write('\n}\n\n')

    def _dump_get_task_instance(self, output):
        """
        Dump get_task_instance() function to a stream
        :param output: a stream to write to
        """
        output.write('def get_task_class(name):\n')
        for task in self._tasks:
            output.write("    if name == '{}':\n".format(task.name))
            output.write("        return {}()\n\n".format(task.class_name))
        output.write("    raise ValueError(\"Unknown task with name '%s'\" % name)\n\n")

    def _dump_get_storage(self, output):
        """
        Dump get_storage to a stream
        :param output: a stream to write to
        """
        output.write('def get_storage(name):\n')
        for storage in self._storages:
            if len(storage.tasks) > 0:
                output.write("    if name == '{}':\n".format(storage.name))
                if storage.configuration:
                    output.write("        return {}({})\n\n".format(storage.class_name,
                                                                    dict2strkwargs(storage.configuration)))
                else:
                    output.write("        return {}()\n\n".format(storage.class_name))
        output.write("    raise ValueError(\"Unknown storage with name '%s'\" % name)\n\n")
        pass

    @staticmethod
    def _dump_condition_name(flow_name, idx):
        """
        Create condition name for a dump
        :param flow_name: flow name
        :type flow_name: str
        :param idx: index of condition within the flow
        :type idx: int
        :return: condition function representation
        """
        assert(idx >= 0)
        return '_condition_{}_{}'.format(flow_name, idx)

    def _dump_condition_functions(self, output):
        """
        Dump condition functions to a stream
        :param output: a stream to write to
        """
        for flow in self._flows:
            for idx, edge in enumerate(flow.edges):
                output.write('def {}(db):\n'.format(self._dump_condition_name(flow.name, idx)))
                output.write('    return {}\n\n\n'.format(codegen.to_source(edge.predicate.ast())))

    def _dump_init_max_retry(self, output):
        """
        Dump max_retry initialization to a stream
        :param output: a stream to write to
        """
        output.write('def init_max_retry():\n')
        seen_classes = {}
        for task in self._tasks:
            if task.class_name in seen_classes:
                if task.max_retry != seen_classes[task.class_name][1]:
                    raise ValueError("Unable to set different max_retry to a same task class: %s and %s for class '%s'"
                                     % ((task.name, task.max_retry), seen_classes[task.class_name], task.class_name))
            seen_classes[task.class_name] = (task.name, task.max_retry)
            output.write('    %s.max_retry = %s\n' % (task.class_name, task.max_retry))
        output.write('\n')

    def _dump_init_time_limit(self, output):
        """
        Dump time limit initialization to a stream
        :param output: a stream to write to
        """
        output.write('def init_time_limit():\n')
        seen_classes = {}
        for task in self._tasks:
            if task.class_name in seen_classes:
                if task.time_limit != seen_classes[task.class_name][1]:
                    raise ValueError("Unable to set different time_limit to a same task class: %s and %s for class '%s'"
                                     % ((task.name, task.time_limit), seen_classes[task.class_name], task.class_name))
            seen_classes[task.class_name] = (task.name, task.time_limit)
            output.write('    %s.time_limit = %s\n' % (task.class_name, task.time_limit))
        output.write('\n')

    def _dump_init_get_storages(self, output):
        """
        Dump get_storages() to output stream
        :param output: a stream to write to
        """
        # NOTE: we could assign to CeleriacTask since all Tasks inherit from it, but be explicit here for now
        output.write('def init_get_storages():\n')
        seen_classes = {}
        for task in self._tasks:
            if not seen_classes.get(task.class_name, False):
                seen_classes[task.class_name] = True
                output.write('    %s.get_storage = get_storage\n' % task.class_name)
        output.write('\n')

    def _dump_init_output_schemas(self, output):
        """
        Dump init_output_schemas() to output stream
        :param output: a stream to write to
        """
        output.write('def init_output_schemas():\n')
        seen_classes = {}
        for task in self._tasks:
            if not seen_classes.get(task.class_name, False):
                seen_classes[task.class_name] = (task.name, task.output_schema)

                if task.output_schema:
                    output.write('    %s.output_schema_path = "%s"\n' % (task.class_name, task.output_schema))
                else:
                    output.write('    %s.output_schema_path = None\n' % task.class_name)
                output.write('    %s.output_schema = None\n' % task.class_name)
            else:
                # we could handle this case to create a list of schemas and access it using task_name that is passed
                # to CeleriacTask
                if seen_classes[task.class_name][1] != task.output_schema:
                    raise ValueError("Unable to set different output schemas to a same task class: %s and %s "
                                     "for class '%s'"
                                     % ((task.name, task.output_schema), seen_classes[task.class_name], task.class_name))
        output.write('\n')

    @staticmethod
    def _dump_init(output):
        """
        Dump init function to a stream
        :param output: a stream to write to
        :return:
        """
        output.write('def init():\n')
        output.write('    init_max_retry()\n')
        output.write('    init_time_limit()\n')
        output.write('    init_get_storages()\n')
        output.write('    init_output_schemas()\n')
        output.write('\n')

    def _dump_edge_table(self, output):
        """
        Dump edge definition table to a stream
        :param output: a stream to write to
        """
        output.write('edge_table = {\n')
        for idx, flow in enumerate(self._flows):
            output.write("    '{}': [".format(flow.name))
            for idx_edge, edge in enumerate(flow.edges):
                if idx_edge > 0:
                    output.write(',\n')
                    output.write(' '*(len(flow.name) + 4 + 5)) # align to previous line
                output.write("{'from': %s" % str([node.name for node in edge.nodes_from]))
                output.write(", 'to': %s" % str([node.name for node in edge.nodes_to]))
                output.write(", 'condition': %s}" % self._dump_condition_name(flow.name, idx_edge))
            if idx + 1 < len(self._flows):
                output.write('],\n')
            else:
                output.write(']\n')
        output.write('}\n\n')

    def dump2stream(self, f):
        """
        Perform system dump to a Python source code to an output stream
        :param f: an output stream to write to
        """
        f.write('#!/usr/bin/env python\n')
        f.write('# auto-generated using Parsley v{}\n\n'.format(parsley_version))
        self._dump_imports(f)
        self._dump_get_task_instance(f)
        self._dump_get_storage(f)
        self._dump_is_flow(f)
        f.write('#'*80+'\n\n')
        self._dump_output_schemas(f)
        f.write('#'*80+'\n\n')
        self._dump_init_max_retry(f)
        f.write('#'*80+'\n\n')
        self._dump_init_time_limit(f)
        f.write('#'*80+'\n\n')
        self._dump_init_get_storages(f)
        f.write('#'*80+'\n\n')
        self._dump_init_output_schemas(f)
        f.write('#'*80+'\n\n')
        self._dump_init(f)
        f.write('#'*80+'\n\n')
        self._dump_condition_functions(f)
        f.write('#'*80+'\n\n')
        self._dump_edge_table(f)
        f.write('#'*80+'\n\n')

    def dump2file(self, output_file):
        """
        Perform system dump to a Python source code
        :param output_file: an output file to write to
        """
        _logger.debug("Performing system dump to '%s'" % output_file)
        with open(output_file, 'w') as f:
            self.dump2stream(f)

    def plot_graph(self, output_dir, image_format=None):
        """
        Plot system flows to graphs - each flow in a separate file
        :param output_dir: output directory to write graphs of flows to
        :param image_format: image format, the default is svg if None
        :return: list of file names to which the graph was rendered
        :rtype: List[str]
        """
        _logger.debug("Rendering system flows to '%s'" % output_dir)
        ret = []
        storage_connections = []
        image_format = image_format if image_format else 'svg'

        for flow in self._flows:
            graph = graphviz.Digraph(format=image_format)
            graph.graph_attr.update(Config().style_graph())
            graph.node_attr.update(Config().style_task())
            graph.edge_attr.update(Config().style_edge())

            for idx, edge in enumerate(flow.edges):
                condition_node = "%s_%s" % (flow.name, idx)
                graph.node(name=condition_node, label=str(edge.predicate), _attributes=Config().style_condition())

                for node in edge.nodes_to:
                    # Plot storage connection
                    if node.is_flow():
                        graph.node(name=node.name, _attributes=Config().style_flow())
                    else:
                        graph.node(name=node.name)
                        if node.storage:
                            graph.node(name=node.storage.name, _attributes=Config().style_storage())
                            if (node.name, node.storage.name) not in storage_connections:
                                graph.edge(node.name, node.storage.name)
                                storage_connections.append((node.name, node.storage.name,))
                    graph.edge(condition_node, node.name)
                for node in edge.nodes_from:
                    if node.is_flow():
                        graph.node(name=node.name, _attributes=Config().style_flow())
                    else:
                        graph.node(name=node.name)
                        if node.storage:
                            graph.node(name=node.storage.name, _attributes=Config().style_storage())
                            if (node.name, node.storage.name) not in storage_connections:
                                graph.edge(node.name, node.storage.name)
                                storage_connections.append((node.name, node.storage.name,))
                    graph.edge(node.name, condition_node)

            file = os.path.join(output_dir, "%s" % flow.name)
            graph.render(filename=file, cleanup=True)
            ret.append(file)
            _logger.info("Graph rendered to '%s.%s'" % (file, image_format))

        return ret

    def post_parse_check(self):
        """
        Called once parse was done to ensure that system was correctly defined in config file
        :raises: ValueError
        """
        _logger.debug("Post parse check is going to be executed")
        # we want to have circular dependencies, so we need to check consistency after parsing since all flows
        # are listed (by names) in a separate definition
        for flow in self._flows:
            if len(flow.edges) == 0:
                raise ValueError("Empty flow: %s" % flow.name)

    def check(self):
        """
        Check system for consistency
        :raises: ValueError
        """
        _logger.info("Checking system for consistency")

        for storage in self._storages:
            if len(storage.tasks) == 0:
                _logger.warning("Storage '{}' not used in any flow".format(storage.name))

        # We want to check that if we depend on a node, that node is being started at least once in the flow
        # This also covers check for starting node definition
        for flow in self._flows:
            try:
                all_nodes_from = set()
                all_nodes_to = set()
                starting_nodes_count = 0

                for edge in flow.edges:
                    if len(edge.nodes_from) == 0:
                        starting_nodes_count += 1

                    node_seen = {}
                    for node_from in edge.nodes_from:
                        if not node_seen.get(node_from.name, False):
                            node_seen[node_from.name] = True
                        else:
                            raise ValueError("Nodes cannot be dependent on a node of a same type mode than once; "
                                             "node from '%s' more than once in flow '%s'" % (node_from.name, flow.name))

                    edge.predicate.check()

                if starting_nodes_count > 1:
                    _logger.warning("Multiple starting nodes defined in flow '%s'" % flow.name)

                if starting_nodes_count == 0:
                    raise ValueError("No starting node found in flow '%s'" % flow.name)

                for edge in flow.edges:
                    all_nodes_from = all_nodes_from | set(edge.nodes_from)
                    all_nodes_to = all_nodes_to | set(edge.nodes_to)

                not_started = all_nodes_from - all_nodes_to

                error = False
                for node in not_started:
                    if node.is_task():
                        _logger.error("Dependency in flow '%s' on node '%s', but this node is not started in the flow"
                                      % (flow.name, node.name))
                        error = True

                if error:
                    raise ValueError("Dependency on not started node detected in flow '%s'" % flow.name)
            except:
                _logger.error("Check of flow '%s' failed" % flow.name)
                raise

    @staticmethod
    def from_files(nodes_definition_file, flow_definition_files):
        """
        Construct System from files
        :param nodes_definition_file: path to nodes definition file
        :param flow_definition_files: path to files that describe flows
        :return: System instance
        :rtype: System
        """
        system = System()

        with open(nodes_definition_file, 'r') as f:
            _logger.debug("Parsing '{}'".format(nodes_definition_file))
            try:
                content = yaml.load(f)
            except:
                _logger.error("Bad YAML file, unable to load tasks from {}".format(nodes_definition_file))
                raise

        for storage_dict in content.get('storages', []):
            storage = Storage.from_dict(storage_dict)
            system.add_storage(storage)

        for task_dict in content['tasks']:
            task = Task.from_dict(task_dict, system)
            system.add_task(task)

        for flow_name in content['flows']:
            flow = Flow(flow_name)
            system.add_flow(flow)

        if not isinstance(flow_definition_files, list):
            flow_definition_files = [flow_definition_files]

        for flow_file in flow_definition_files:
            with open(flow_file, 'r') as f:
                _logger.debug("Parsing '{}'".format(flow_file))
                try:
                    content = yaml.load(f)
                except:
                    _logger.error("Bad YAML file, unable to load flow from {}".format(flow_file))
                    raise

            for flow_def in content['flow-definitions']:
                flow = system.flow_by_name(flow_def['name'])
                for edge_def in flow_def['edges']:
                    edge = Edge.from_dict(edge_def, system, flow)
                    flow.add_edge(edge)

        system.post_parse_check()
        return system

#!/usr/bin/env python3
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

from .node import Node
from .logger import Logger

_logger = Logger.get_logger(__name__)


class Flow(Node):
    """
    Flow representation
    """
    def __init__(self, name, edges=None, failures=None, nowait_nodes=None):
        """
        :param name: flow name
        :type name: str
        :param edges: edges in the flow
        :type edges: List[Edge]
        :param nowait_nodes: nodes that should not be waited for
        :type nowait_nodes: List[Node]
        """
        super(Flow, self).__init__(name)
        _logger.debug("Creating flow '{}'".format(name))
        self._edges = edges if edges else []
        self._failures = failures if failures else None
        self._nowait_nodes = nowait_nodes if nowait_nodes else []

        self._propagate_finished = None
        self._propagate_node_args = None
        self._propagate_parent = None

    @staticmethod
    def from_dict(d):
        raise NotImplementedError()

    @property
    def edges(self):
        """
        :return: edges presented in the flow
        """
        return self._edges

    @property
    def failures(self):
        """
        :return: failures defined within the flow
        """
        return self._failures

    @failures.setter
    def failures(self, failures):
        """
        :param failures: failures to be set
        """
        self._failures = failures

    @property
    def nowait_nodes(self):
        """
        :return: nodes that are marked with nowait flag - dispatcher will not wait for their completion
        """
        return self._nowait_nodes

    def add_edge(self, edge):
        """
        :param edge: edge to be added
        :type edge: List[Edge]
        """
        self._edges.append(edge)

    def add_nowait_node(self, node):
        """
        :param node: add a node that should be marked with nowait flag
        """
        self._nowait_nodes.append(node)

    @property
    def propagate_finished(self):
        """
        :return: subflows from which should be propagated finished nodes
        """
        return self._propagate_finished

    @propagate_finished.setter
    def propagate_finished(self, nodes):
        """
        :param nodes: set nodes (subflows) which should be used to propagate finished
        """
        self._propagate_finished = nodes

    @property
    def propagate_node_args(self):
        """
        :return: nodes to which should be propagated node arguments
        """
        return self._propagate_node_args

    @propagate_node_args.setter
    def propagate_node_args(self, nodes):
        """
        :param nodes: set nodes (subflows) to which should be propagated node arguments
        """
        self._propagate_node_args = nodes

    @property
    def propagate_parent(self):
        """
        :return: flows to which should be propagated parent nodes
        """
        return self._propagate_parent

    @propagate_parent.setter
    def propagate_parent(self, nodes):
        """
        :param nodes: set flows to which shoule be propagated parent nodes
        """
        self._propagate_parent = nodes

    def all_nodes_from(self):
        """
        :return: all source nodes in flow, excluding failures
        """
        all_nodes_from = set()

        for edge in self._edges:
            all_nodes_from = all_nodes_from | set(edge.nodes_from)

        return list(all_nodes_from)

    def all_nodes_to(self):
        """
        :return: all destination nodes in flow, excluding failures
        """
        all_nodes_to = set()

        for edge in self._edges:
            all_nodes_to = all_nodes_to | set(edge.nodes_to)

        return list(all_nodes_to)

    def all_source_nodes(self):
        """
        :return: all source nodes in flow, including failures
        """
        if self.failures:
            return list(set(self.all_nodes_from()) | set(self.failures.all_waiting_nodes()))
        else:
            return self.all_nodes_from()

    def all_destination_nodes(self):
        """
        :return: all destination nodes in flow, including failures
        """
        if self.failures:
            return list(set(self.all_nodes_to()) | set(self.failures.all_fallback_nodes()))
        else:
            return self.all_nodes_to()

    def all_used_nodes(self):
        """
        :return: all used nodes in flow
        """
        return list(set(self.all_destination_nodes()) | set(self.all_source_nodes()))

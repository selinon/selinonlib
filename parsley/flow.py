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

        self._propagate_parent = True
        self._propagate_finished = True

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
        return self._failures

    @failures.setter
    def failures(self, failures):
        self._failures = failures

    @property
    def propagate_parent(self):
        return self._propagate_parent

    @propagate_parent.setter
    def propagate_parent(self, propagate_parent):
        if not isinstance(propagate_parent, bool):
            raise ValueError("Flag 'propagate_parent' expects boolean value in flow '%s'" % self.name)
        self._propagate_parent = propagate_parent

    @property
    def propagate_finished(self):
        return self._propagate_finished

    @propagate_finished.setter
    def propagate_finished(self, propagate_finished):
        if not isinstance(propagate_finished, bool):
            raise ValueError("Flag 'propagate_finished' expects boolean value in flow '%s'" % self.name)
        self._propagate_finished = propagate_finished

    @property
    def nowait_nodes(self):
        return self._nowait_nodes

    def add_edge(self, edge):
        """
        :param edge: edge to be added
        :type edge: List[Edge]
        """
        self._edges.append(edge)

    def add_nowait_node(self, node):
        self._nowait_nodes.append(node)

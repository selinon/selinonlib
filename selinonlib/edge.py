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

from .predicate import Predicate
from .buildinPredicate import AlwaysTruePredicate


class Edge(object):
    """
    Edge representation
    """
    def __init__(self, nodes_from, nodes_to, predicate, flow, foreach):
        """
        :param nodes_from: nodes from where edge starts
        :type nodes_from: List[Node]
        :param nodes_to: nodes where edge ends
        :type nodes_to: List[Node]
        :param predicate: predicate condition
        :type predicate: Predicate
        :param flow: flow to which edge belongs to
        :type flow: Flow
        :param foreach: foreach defining function and import over which we want to iterate
        :type foreach: dict
        """
        self.nodes_from = nodes_from
        self.nodes_to = nodes_to
        self.predicate = predicate
        self.flow = flow
        self.foreach = foreach

    def check(self):
        """
        Check edge consistency
        """
        if self.foreach and self.foreach['propagate_result']:
            # We can propagate result of our foreach function only if:
            #  1. all nodes to are flows
            #  2. propagate_node_args is not set for flows listed in nodes to
            for node_to in self.nodes_to:
                if not node_to.is_flow():
                    raise ValueError("Flag propagate_result listed in foreach configuration in flow '%s' "
                                     "requires all nodes to to be flows, but '%s' is a task"
                                     % (self.flow.name, node_to.name))

                if (isinstance(self.flow.propagate_node_args, bool) and self.flow.propagate_node_args) \
                        or (isinstance(self.flow.propagate_node_args, list)
                            and node_to.name in self.flow.propagate_node_args):
                    raise ValueError("Cannot propagate node arguments to subflow when propagate_result is set"
                                     " in foreach definition in flow '%s' for node to '%s'"
                                     % (self.flow.name, node_to.name))


    @staticmethod
    def from_dict(d, system, flow):
        """
        Construct edge from a dict

        :param d: a dictionary from which the system should be created
        :type d: dict
        :param system:
        :type system: System
        :param flow: flow to which edge belongs to
        :type flow: Flow
        :return:
        """
        if 'from' not in d:
            raise ValueError("Edge definition requires 'from' explicitly to be specified, use empty for starting edge")

        # we allow empty list for a starting edge
        if d['from']:
            from_names = d['from'] if isinstance(d['from'], list) else [d['from']]
            nodes_from = [system.node_by_name(n) for n in from_names]
        else:
            nodes_from = []

        if 'to' not in d or not d['to']:
            raise ValueError("Edge definition requires 'to' specified")

        to_names = d['to'] if isinstance(d['to'], list) else [d['to']]
        nodes_to = [system.node_by_name(n) for n in to_names]

        if 'condition' in d:
            predicate = Predicate.construct(d.get('condition'), nodes_from, flow)
        else:
            predicate = AlwaysTruePredicate(flow=flow)

        foreach = None
        if 'foreach' in d:
            foreach_def = d['foreach']
            if foreach_def is None or 'function' not in foreach_def or 'import' not in foreach_def:
                raise ValueError("Specification of 'foreach' requires 'function' and 'import' to be set in flow '%s',"
                                 " got %s instead" % (flow.name, foreach_def))

            foreach = {
                'function': foreach_def['function'],
                'import': foreach_def['import'],
                'propagate_result': False
            }

            if 'propagate_result' in foreach_def:
                if not isinstance(foreach_def['propagate_result'], bool):
                    raise ValueError("Propagate result should be bool in flow '%s', got %s instead"
                                     % (flow.name, foreach_def['propagate_result']))

                if foreach_def['propagate_result']:
                    foreach['propagate_result'] = True

                # additional checks for 'propagate_result' are done in Edge.check() since we have chicken-egg problem
                # here

            if not isinstance(foreach_def['function'], str):
                raise ValueError("Wrong function name '%s' supplied in foreach section in flow %s"
                                 % (foreach_def['function'], flow.name))

            if not isinstance(foreach_def['import'], str):
                raise ValueError("Wrong import statement '%s' supplied in foreach section in flow %s"
                                 % (foreach_def['import'], flow.name))

        return Edge(nodes_from=nodes_from, nodes_to=nodes_to, predicate=predicate, flow=flow, foreach=foreach)

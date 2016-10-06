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
            foreach = {'function': foreach_def['function'], 'import': foreach_def['import']}

            if not isinstance(foreach_def['function'], str):
                raise ValueError("Wrong function name '%s' supplied in foreach section in flow %s"
                                 % (foreach_def['function'], flow.name))

            if not isinstance(foreach_def['import'], str):
                raise ValueError("Wrong import statement '%s' supplied in foreach section in flow %s"
                                 % (foreach_def['import'], flow.name))

        return Edge(nodes_from=nodes_from, nodes_to=nodes_to, predicate=predicate, flow=flow, foreach=foreach)

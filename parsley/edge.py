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
    def __init__(self, nodes_from, nodes_to, predicate, flow):
        """
        :param nodes_from: nodes from where edge starts
        :type nodes_from: List[Node]
        :param nodes_to: nodes where edge ends
        :type nodes_to: List[Node]
        :param predicate: predicate condition
        :type predicate: Predicate
        :param flow: flow to which edge belongs to
        :type flow: Flow
        """
        self._nodes_from = nodes_from
        self._nodes_to = nodes_to
        self._predicate = predicate
        self._flow = flow

    @property
    def nodes_from(self):
        """
        :return: edge's source nodes
        """
        return self._nodes_from

    @property
    def nodes_to(self):
        """
        :return: edge's destination nodes
        """
        return self._nodes_to

    @property
    def predicate(self):
        """
        :return: edge condition
        """
        return self._predicate

    @property
    def flow(self):
        """
        :return: flow that edge is defined in
        """
        return self._flow

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

        return Edge(nodes_from=nodes_from, nodes_to=nodes_to, predicate=predicate, flow=flow)

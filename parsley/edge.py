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

from .predicate import Predicate


class Edge(object):
    """
    Edge representation
    """
    def __init__(self, nodes_from, nodes_to, predicate):
        """
        :param nodes_from: nodes from where edge starts
        :type nodes_from: List[Node]
        :param nodes_to: nodes where edge ends
        :type nodes_to: List[Node]
        :param predicate: predicate condition
        :type predicate: Predicate
        """
        self._nodes_from = nodes_from
        self._nodes_to = nodes_to
        self._predicate = predicate

    @property
    def nodes_from(self):
        return self._nodes_from

    @property
    def nodes_to(self):
        return self._nodes_to

    @property
    def predicate(self):
        return self._predicate

    @staticmethod
    def from_dict(d, system):
        """
        Construct edge from a dict
        :param d: a dictionary from which the system should be created
        :type d: dict
        :param system:
        :type system: System
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

        predicate = Predicate.construct(d.get('condition'), nodes_from)

        return Edge(nodes_from=nodes_from, nodes_to=nodes_to, predicate=predicate)

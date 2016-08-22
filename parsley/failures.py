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

from .failureNode import FailureNode


class Failures(object):
    """
    Node failures and fallback handling
    """
    def __init__(self, raw_definition, last_allocated=None, starting_nodes=None):
        """
        :param raw_definition: raw definition of failures
        :param last_allocated: last allocated starting node for linked list
        :param starting_nodes: starting nodes for failures
        """
        self._raw_definition = raw_definition
        self._last_allocated = last_allocated
        self._starting_nodes = starting_nodes

    @staticmethod
    def construct(flow, failures_dict):
        """
        :param flow: a flow to which failures conform
        :param failures_dict: construct failures from failures dict
        :rtype: Failures
        """

        for failure in failures_dict:
            if 'nodes' not in failure or failure['nodes'] is None:
                raise ValueError("Failure should state nodes for state 'nodes' to fallback from in flow '%s'"
                                 % flow.name)

            if 'fallback' not in failure:
                raise ValueError("No fallback stated in failure in flow '%s'" % flow.name)

            if not isinstance(failure['nodes'], list):
                failure['nodes'] = [failure['nodes']]

            if not isinstance(failure['fallback'], list) and failure['fallback'] is not True:
                failure['fallback'] = [failure['fallback']]

            if failure['fallback'] is not True and len(failure['fallback']) == 1 and len(failure['nodes']) == 1 \
                    and failure['fallback'][0] == failure['nodes'][0]:
                raise ValueError("Detect cyclic fallback dependency in flow %s, failure on %s"
                                 % (flow.name, failure['nodes'][0]))

        last_allocated, starting_nodes = FailureNode.construct(flow, failures_dict)
        return Failures(failures_dict, last_allocated, starting_nodes)

    @staticmethod
    def starting_nodes_name(flow_name):
        """
        A starting node name representation for generated Python config
        """
        return "_%s_failure_starting_nodes" % flow_name

    @staticmethod
    def failure_node_name(flow_name, failure_node):
        """
        A failure node name representation for generated Python config
        """
        return "_%s_fail_%s" % (flow_name, "_".join(failure_node.traversed))

    @property
    def raw_definition(self):
        """
        :return: raw definition as in config YAML
        """
        return self._raw_definition

    def started_nodes_names(self):
        """
        :return: names of nodes that are started by fallbacks in all failures
        """
        ret = []

        failure_node = self._last_allocated
        while failure_node:
            if isinstance(failure_node.fallback, list):
                ret.extend(failure_node.fallback)
            failure_node = failure_node.failure_link

        return ret

    def waiting_nodes_names(self):
        """
        :return: names of all nodes that we are expecting to fail for fallbacks
        """
        return list(self._starting_nodes.keys())

    def dump2stream(self, f, flow_name):
        """
        Dumo failures to the Python config file for Dispatcher
        :param f: output stream to dump to
        :param flow_name: a name of a flow that failures belong to
        """
        fail_node = self._last_allocated

        while fail_node:
            next_dict = {}
            for k, v in fail_node.next.items():
                next_dict[k] = self.failure_node_name(flow_name, v)

            f.write("%s = {'next': " % self.failure_node_name(flow_name, fail_node))

            # print "next_dict"
            f.write('{')
            printed = False
            for k, v in next_dict.items():
                if printed:
                    f.write(", ")
                f.write("'%s': %s" % (k, v))
                printed = True
            f.write('}, ')

            # now list of nodes that should be started in case of failure (fallback)
            f.write("'fallback': %s}\n" % fail_node.fallback)
            fail_node = fail_node.failure_link

        f.write("\n%s = {" % self.starting_nodes_name(flow_name))

        printed = False
        for k, v in self._starting_nodes.items():
            if printed:
                f.write(",")
            f.write("\n    '%s': %s" % (k, self.failure_node_name(flow_name, v)))
            printed = True
        f.write("\n}\n\n")

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
    def __init__(self, last_allocated=None, starting_nodes=None):
        """
        :param last_allocated: last allocated starting node for linked list
        :param starting_nodes: starting nodes for failures
        """
        self._last_allocated = last_allocated
        self._starting_nodes = starting_nodes

    @staticmethod
    def construct(failures_dict):
        """
        :param failures_dict: construct failures from failures dict
        :rtype: Failures
        """
        last_allocated, starting_nodes = FailureNode.construct(failures_dict)
        return Failures(last_allocated, starting_nodes)

    @staticmethod
    def starting_nodes_name(flow_name):
        """
        A starting node name representation for generated Python config
        """
        return "%s_failure_starting_nodes" %  flow_name

    @staticmethod
    def failure_node_name(flow_name, failure_node):
        """
        A failure node name representation for generated Python config
        """
        return "%s_fail_%s" % (flow_name, "_".join(failure_node.traversed))

    # TODO: extend flow checks with this one
    def started_nodes_names(self):
        """
        :return: names of nodes that are started by fallbacks in all failures
        """
        ret = []

        failure_node = self._last_allocated
        while failure_node:
            ret.extend(failure_node.fallback)

        return ret

    # TODO: extend flow checks with this one
    def waiting_nodes_names(self):
        """
        :return: names of all nodes that we are expecting to fail for faillbacks
        """
        ret = []

        for node in self._starting_nodes:
            ret.extend(node.traversed)

        return ret

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

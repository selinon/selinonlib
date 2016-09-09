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

import abc
import re


class Node(metaclass=abc.ABCMeta):
    """
    An abstract class for node representation
    """
    def __init__(self, name):
        if not self.check_name(name):
            raise ValueError("Invalid node name '%s'" % name)
        self._name = name

    @abc.abstractstaticmethod
    def from_dict(d):
        """
        Construct node from a dict
        :return: instantiated node
        """
        pass

    @property
    def name(self):
        """
        :return: a name of the node
        """
        return self._name

    def is_flow(self):
        """
        :return: True if node represents a Flow
        """
        from .flow import Flow
        return isinstance(self, Flow)

    def is_task(self):
        """
        :return: True if node represents a Task
        """
        from .task import Task
        return isinstance(self, Task)

    @staticmethod
    def check_name(name):
        """
        Check whether name is a correct node (flow/task) name
        :param name: node name
        :return: True if name is a correct node name
        :rtype: bool
        """
        r = re.compile(r"^[_a-zA-Z][_a-zA-Z0-9]*$")
        return r.match(name)
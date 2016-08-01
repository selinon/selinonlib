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
from .helpers import check_node_name, ABC


class Node(ABC):
    """
    An abstract class for node representation
    """
    def __init__(self, name):
        if not check_node_name(name):
            raise ValueError("Invalid node name '%s'" % name)
        self._name = name

    @abc.abstractmethod
    def from_dict(d):
        """
        Construct node from a dict
        :return: instantiated node
        """
        # Actually @abc.abstractstaticmethod, but this does not work with Python2
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


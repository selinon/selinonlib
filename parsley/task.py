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


class Task(Node):
    """
    A task representation within the system
    """
    def __init__(self, name, import_path, class_name=None):
        """
        :param name: name of the task
        :param import_path: tasks's import
        :param class_name: tasks's class name, if None, 'name' is used
        """
        super(Task, self).__init__(name)
        self._import_path = import_path
        self._class_name = class_name if class_name else name
        _logger.debug("Creating task with name '%s' import path '%s', class name '%s'"
                      % (self.name, self.import_path, self.class_name))

    @property
    def import_path(self):
        """
        :return: a task import
        """
        return self._import_path

    @property
    def class_name(self):
        """
        :return: a task class name
        """
        return self._class_name

    @staticmethod
    def from_dict(d):
        """
        Construct task from a dict
        :param d: dictionary to be used to construct the task
        :return: Task instance
        :rtype: Task
        """
        if 'name' not in d or not d['name']:
            raise KeyError('Task name definition is mandatory')
        if 'import' not in d or not d['import']:
            raise KeyError('Task import definition is mandatory')
        return Task(d['name'], d['import'], d.get('classname'))

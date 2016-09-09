#!/usr/bin/env python3
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


class TaskClass(object):
    def __init__(self, class_name, import_path):
        self._class_name = class_name
        self._import_path = import_path
        self._tasks = []

    @property
    def class_name(self):
        """
        :return: name of the class
        """
        return self._class_name

    @property
    def import_path(self):
        """
        :return: import of the task
        """
        return self._import_path

    @property
    def tasks(self):
        """
        :return: tasks that are instantiated from this task
        """
        return self._tasks

    def task_of_class(self, task):
        """
        :param task: task to be checked
        :return: True if task is of this class
        """
        return self._class_name == task.class_name and self._import_path == task.import_path

    def add_task(self, task):
        """
        Register a task to this class
        :param task: task to be added
        """
        assert(self.task_of_class(task))
        self._tasks.append(task)


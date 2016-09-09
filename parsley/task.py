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

from .node import Node
from .logger import Logger

_logger = Logger.get_logger(__name__)

_DEFAULT_MAX_RETRY = 0
_DEFAULT_RETRY_COUNTDOWN = 5


class Task(Node):
    """
    A task representation within the system
    """
    def __init__(self, name, import_path, class_name=None, storage=None, max_retry=None, retry_countdown=None,
                 time_limit=None, output_schema=None, task_class=None):
        """
        :param name: name of the task
        :param import_path: tasks's import
        :param class_name: tasks's class name, if None, 'name' is used
        :param storage: storage that should be used
        :param max_retry: configured maximum retry count
        :param retry_countdown: countdown in seconds to retry task in case of failure
        :param time_limit: configured time limit for task run
        :param output_schema: task result output schema
        :param task_class: class of the task
        """
        if not isinstance(import_path, str):
            raise ValueError("Error in task '%s' definition - import path should be string; got '%s'"
                             % (name, import_path))

        if class_name is not None and not isinstance(class_name, str):
            raise ValueError("Error in task '%s' definition - class instance should be string; got '%s'"
                             % (name, class_name))

        if output_schema is not None and not isinstance(output_schema, str):
            raise ValueError("Error in task '%s' definition - output schema should be string; got '%s'"
                             % (name, output_schema))

        if max_retry is not None and (not isinstance(max_retry, int) or max_retry < 0):
            raise ValueError("Error in task '%s' definition - max_retry should be None, zero or positive integer;"
                             " got '%s'" % (name, max_retry))

        if time_limit is not None and (not isinstance(time_limit, int) or time_limit <= 0):
            raise ValueError("Error in task '%s' definition - time_limit should be None or positive integer;"
                             " got '%s'" % (name, time_limit))

        if retry_countdown is not None and (not isinstance(retry_countdown, int) or retry_countdown < 0):
            raise ValueError("Error in task '%s' definition - retr_countdown should be None or positive integer;"
                             " got '%s'" % (name, retry_countdown))

        super(Task, self).__init__(name)
        self._import_path = import_path
        self._max_retry = max_retry
        self._retry_countdown = retry_countdown
        self._time_limit = time_limit
        self._class_name = class_name if class_name else name
        self._storage = storage
        self._output_schema = output_schema
        self._task_class = task_class
        # register task usage
        if storage:
            storage.register_task(self)
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

    @property
    def storage(self):
        return self._storage

    @property
    def max_retry(self):
        """
        :return: task max_retry count (see Celery max_retry)
        """
        return self._max_retry

    @property
    def retry_countdown(self):
        """
        :return: task retry_countdown (see Celery self.retry() countdown)
        """
        return self._retry_countdown

    @property
    def time_limit(self):
        """
        :return: task time_limit (see Celery time_limit)
        """
        return self._time_limit

    @property
    def output_schema(self):
        """
        :return: path to task output schema or None if not defined
        """
        return self._output_schema

    @property
    def task_class(self):
        """
        :return: class of the task
        :rtype: TaskClass
        """
        return self._task_class

    @task_class.setter
    def task_class(self, task_class):
        """
        :param task_class: set task_class for task
        :type task_class: TaskClass
        """
        self._task_class = task_class

    @staticmethod
    def from_dict(d, system):
        """
        Construct task from a dict
        :param d: dictionary to be used to construct the task
        :return: Task instance
        :param system: system that should be used to for lookup a storage
        :type system: System
        :rtype: Task
        """
        if 'name' not in d or not d['name']:
            raise KeyError('Task name definition is mandatory')
        if 'import' not in d or not d['import']:
            raise KeyError('Task import definition is mandatory')
        if 'storage' in d:
            storage = system.storage_by_name(d['storage'])
        else:
            storage = None
        return Task(d['name'], d['import'], d.get('classname'), storage, d.get('max_retry', _DEFAULT_MAX_RETRY),
                    d.get('retry_countdown', _DEFAULT_RETRY_COUNTDOWN), d.get('time_limit'),
                    d.get('output_schema'))

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

from .cacheConfig import CacheConfig


class Storage(object):
    """
    A storage representation
    """
    def __init__(self, name, import_path, configuration, cache_config, class_name=None):
        """
        :param name: storage name
        :param import_path: storage import path
        :param configuration: storage configuration that will be passed
        :param cache_config: cache configuration information
        :param class_name: storage class name
        """
        self.name = name
        self.import_path = import_path
        self.configuration = configuration
        self.class_name = class_name or name
        self.tasks = []
        self.cache_config = cache_config

    def register_task(self, task):
        """
        Register a new that uses this storage

        :param task: task to be registered
        """
        self.tasks.append(task)

    @staticmethod
    def from_dict(d):
        """
        Construct storage instance from a dict

        :param d: dict that should be used to instantiate Storage
        :rtype: Storage
        """
        if 'name' not in d or not d['name']:
            raise KeyError('Storage name definition is mandatory')
        if 'import' not in d or not d['import']:
            raise KeyError('Storage import definition is mandatory')
        if 'configuration' not in d or not d['configuration']:
            raise KeyError('Storage configuration definition is mandatory')
        if 'classname' in d and not isinstance(d['classname'], str):
            raise ValueError("Storage classname definition should be string, got '%s' instead" % d['classname'])
        if 'cache' in d:
            if not isinstance(d['cache'], dict):
                raise ValueError("Storage cache for storage '%s' should be a dict with configuration, got '%s' instead"
                                 % (d['name'], d['cache']))

            cache_config = CacheConfig.from_dict(d['cache'], d['name'])
        else:
            cache_config = CacheConfig.get_default(d['name'])

        return Storage(d['name'], d['import'], d['configuration'], cache_config, d.get('classname'))

    @property
    def var_name(self):
        """
        return: name of storage variable that will be used in config.py file
        """
        return "_storage_%s" % self.name

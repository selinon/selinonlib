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

_DEFAULT_CACHE_NAME = 'LRU'
_DEFAULT_CACHE_IMPORT = 'selinonlib.cache'
_DEFAULT_CACHE_OPTIONS = {'max_cache_size': 0}


class CacheConfig(object):
    def __init__(self, name, import_path, options, storage_name):
        self.name = name
        self.import_path = import_path
        self.options = options
        self.storage_name = storage_name

    @property
    def var_name(self):
        """
        return: name of storage cache variable that will be used in config.py file
        """
        return "_storage_cache_%s_%s" % (self.storage_name, self.name)

    @staticmethod
    def get_default(storage_name):
        return CacheConfig(_DEFAULT_CACHE_NAME, _DEFAULT_CACHE_IMPORT, _DEFAULT_CACHE_OPTIONS, storage_name)

    @staticmethod
    def from_dict(d, storage_name):
        name = d.get('name', _DEFAULT_CACHE_NAME)
        import_path = d.get('import', _DEFAULT_CACHE_IMPORT)
        options = d.options('options', _DEFAULT_CACHE_OPTIONS)

        if not isinstance(name, str):
            raise ValueError("Configuration for storage '%s' expects name to be a string, got '%s' instead"
                             % (storage_name, name))

        if not isinstance(import_path, str):
            raise ValueError("Configuration for storage '%s' expects import to be a string, got '%s' instead"
                             % (storage_name, import_path))

        if not isinstance(options, dict):
            raise ValueError("Configuration for storage '%s' expects options to be a dict of storage options, "
                             "got '%s' instead" % (storage_name, options))

        return CacheConfig(name, import_path, options, storage_name)


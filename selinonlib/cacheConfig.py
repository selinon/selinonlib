#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Configuration for caching"""

from .helpers import check_conf_keys

_DEFAULT_CACHE_NAME = 'LRU'
_DEFAULT_CACHE_IMPORT = 'selinonlib.caches'
_DEFAULT_CACHE_OPTIONS = {'max_cache_size': 0}


class CacheConfig(object):
    """Configuration for Caching"""

    def __init__(self, name, import_path, options, entity_name):
        self.name = name
        self.import_path = import_path
        self.options = options
        self.entity_name = entity_name

    @property
    def var_name(self):
        """
        return: name of cache variable that will be used in config.py file
        """
        return "_cache_%s_%s" % (self.entity_name, self.name)

    @staticmethod
    def get_default(entity_name):
        """Get default cache configuration

        :param entity_name: entity name that will use the default cache - entity is either storage name or
                            flow name (when caching async results)
        :return: CacheConfig for the given entity
        :rtype: CacheConfig
        """
        return CacheConfig(_DEFAULT_CACHE_NAME, _DEFAULT_CACHE_IMPORT, _DEFAULT_CACHE_OPTIONS, entity_name)

    @staticmethod
    def from_dict(dict_, entity_name):
        """Parse cache configuration from a dict

        :param dict_: dict from which cache configuration should be parsed
        :param entity_name: entity name that will use the default cache - entity is either storage name or
                            flow name (when caching async results)
        :return: cache for the given entity based on configuration
        :rtype: CacheConfig
        """
        name = dict_.get('name', _DEFAULT_CACHE_NAME)
        import_path = dict_.get('import', _DEFAULT_CACHE_IMPORT)
        options = dict_.get('options', _DEFAULT_CACHE_OPTIONS)

        if not isinstance(name, str):
            raise ValueError("Cache configuration for '%s' expects name to be a string, got '%s' instead"
                             % (entity_name, name))

        if not isinstance(import_path, str):
            raise ValueError("Cache configuration for '%s' expects import to be a string, got '%s' instead"
                             % (entity_name, import_path))

        if not isinstance(options, dict):
            raise ValueError("Cache configuration for '%s' expects options to be a dict of cache options, "
                             "got '%s' instead" % (entity_name, options))

        # check supplied configuration options
        unknown_conf = check_conf_keys(dict_, known_conf_opts=('name', 'import', 'options'))
        if unknown_conf:
            raise ValueError("Unknown configuration options for cache '%s' supplied: %s"
                             % (name, unknown_conf))

        return CacheConfig(name, import_path, options, entity_name)

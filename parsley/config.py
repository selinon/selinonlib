#!/bin/env python
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

import yaml


class _ConfigSingleton(type):
    """
    Config singleton metaclass
    """
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(_ConfigSingleton, cls).__call__(*args, **kwargs)
        return cls._instance

    @classmethod
    def set_config(cls, config):
        """
        Set config which should be used in within Config singleton
        :param config: configuration that should be used
        :type: dict
        """
        # set _config before the singleton is instantiated
        assert(cls._instance is None)
        cls._config = config


# A hack to workaround metaclass differences in Python3 and Python2
_ConfigParent = _ConfigSingleton('Config', (object,), {})


class Config(_ConfigParent):
    _config = None

    def __init__(self):
        self._raw_config = None

        if self._config is None:
            return

        with open(self._config) as f:
            self._raw_config = yaml.load(f)

    def style_task(self):
        """
        Return style for tasks in the graph, see graphviz styling options
        :return: style definition
        :rtype: dict
        """
        if self._raw_config is None or 'style' not in self._raw_config:
            return {}
        ret = self._raw_config['style'].get('task', {})
        return ret if ret is not None else {}

    def style_flow(self):
        """
        Return style for a flow node in the graph, see graphviz styling options
        :return: style definition
        :rtype: dict
        """
        default = {'style': 'filled', 'fillcolor': '#CCCCCC'}
        if self._raw_config is None or 'style' not in self._raw_config:
            return default
        ret = self._raw_config['style'].get('flow', default)
        return ret if ret is not None else default

    def style_condition(self):
        """
        Return style for conditions in the graph, see graphviz styling options
        :return: style definition
        :rtype: dict
        """
        default_style = {'shape': 'hexagon'}
        if self._raw_config is None or 'style' not in self._raw_config:
            return default_style
        ret = self._raw_config['style'].get('condition', default_style)
        return ret if ret is not None else {}

    def style_edge(self):
        """
        Return style for edges in the graph, see graphviz styling options
        :return: style definition
        :rtype: dict
        """
        if self._raw_config is None or 'style' not in self._raw_config:
            return {}
        ret = self._raw_config['style'].get('edge', {})
        return ret if ret is not None else {}

    def style_graph(self):
        """
        Return style for the whole graph, see graphviz styling options
        :return: style definition
        :rtype: dict
        """
        if self._raw_config is None or 'style' not in self._raw_config:
            return {}
        ret = self._raw_config['style'].get('graph', {})
        return ret if ret is not None else {}

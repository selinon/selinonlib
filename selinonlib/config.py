#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""User configuration."""

import yaml


class _ConfigSingleton(type):
    """Config singleton metaclass."""

    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(_ConfigSingleton, cls).__call__(*args, **kwargs)
        return cls._instance

    @classmethod
    def set_config(mcs, config):
        """Set config which should be used in within Config singleton.

        :param config: configuration that should be used
        :type config: dict
        """
        # set _config before the singleton is instantiated
        assert mcs._instance is None  # nosec
        mcs._config = config


class Config(metaclass=_ConfigSingleton):
    """Configuration supplied by user."""

    _config = None

    def __init__(self):
        """Instantiate configuration."""
        self._raw_config = None

        if self._config is None:
            return

        with open(self._config) as input_file:
            self._raw_config = yaml.load(input_file, Loader=yaml.SafeLoader)

    def style_task(self):
        """Return style for tasks in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        if self._raw_config is None or 'style' not in self._raw_config:
            return {}
        ret = self._raw_config['style'].get('task', {})
        return ret if ret is not None else {}

    def style_flow(self):
        """Return style for a flow node in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        default = {'style': 'filled', 'fillcolor': '#CCCCCC'}
        if self._raw_config is None or 'style' not in self._raw_config:
            return default
        ret = self._raw_config['style'].get('flow', default)
        return ret if ret is not None else default

    def style_condition(self):
        """Return style for conditions in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        default_style = {'shape': 'octagon'}
        if self._raw_config is None or 'style' not in self._raw_config:
            return default_style
        ret = self._raw_config['style'].get('condition', default_style)
        return ret if ret is not None else default_style

    def style_condition_foreach(self):
        """Return style for foreach edges in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        default_style = {'shape': 'doubleoctagon'}
        if self._raw_config is None or 'style' not in self._raw_config:
            return default_style
        ret = self._raw_config['style'].get('condition', default_style)
        return ret if ret is not None else default_style

    def style_storage(self):
        """Return style for storage in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        default_style = {'shape': 'folder', 'color': '#AAAAAA'}
        if self._raw_config is None or 'style' not in self._raw_config:
            return default_style
        ret = self._raw_config['style'].get('storage', default_style)
        return ret if ret is not None else {}

    def style_edge(self):
        """Return style for edges in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        if self._raw_config is None or 'style' not in self._raw_config:
            return {}
        ret = self._raw_config['style'].get('edge', {})
        return ret if ret is not None else {}

    def style_store_edge(self):
        """Return style for edges that lead to a storage in the graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        default_style = {'color': '#AAAAAA', 'style': 'dashed'}
        if self._raw_config is None or 'style' not in self._raw_config:
            return default_style
        ret = self._raw_config['style'].get('store-edge', default_style)
        return ret if ret is not None else default_style

    def style_graph(self):
        """Return style for the whole graph, see graphviz styling options.

        :return: style definition
        :rtype: dict
        """
        if self._raw_config is None or 'style' not in self._raw_config:
            return {}
        ret = self._raw_config['style'].get('graph', {})
        return ret if ret is not None else {}

    def style_fallback_edge(self):
        """Return style for fallback edges.

        :return: style definition
        :rtype: dict
        """
        default_style = {'color': '#FF0000', 'style': 'dashed'}
        if self._raw_config is None or 'style' not in self._raw_config:
            return default_style
        ret = self._raw_config['style'].get('fallback-edge', default_style)
        return ret if ret is not None else default_style

    def style_fallback_node(self):
        """Return style for fallback node.

        :return: style definition
        :rtype: dict
        """
        default_style = {'color': '#FF0000', 'shape': 'point'}
        if self._raw_config is None or 'style' not in self._raw_config:
            return default_style
        ret = self._raw_config['style'].get('fallback-edge', default_style)
        return ret if ret is not None else default_style

    def style_fallback_true(self):
        """Return style for fallback true node.

        :return: style definition
        :rtype: dict
        """
        default_style = {'color': '#222222', 'shape': 'doubleoctagon'}
        if self._raw_config is None or 'style' not in self._raw_config:
            return default_style
        ret = self._raw_config['style'].get('fallback-edge', default_style)
        return ret if ret is not None else default_style

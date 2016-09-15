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

import importlib
from .helpers import get_function_arguments

_EXPECTED_STRATEGY_FUNC_ARGS = {'previous_retry', 'active_nodes', 'failed_nodes', 'started_nodes', 'fallback_nodes'}
_DEFAULT_CONFIG_VERSION = 0
_DEFAULT_TASK_VERSION = 0


class GlobalConfig(object):
    """
    User global configuration stated in YAML file
    """
    predicates_module = 'parsley.predicates'

    strategy_module = 'parsley.strategies'
    strategy_function = 'biexponential_increase'
    strategy_func_args = {'start_retry': 2, 'max_retry': 120}

    default_task_version = _DEFAULT_TASK_VERSION
    config_version = _DEFAULT_CONFIG_VERSION

    _trace_logging = None
    _trace_import = None
    _trace_function = None
    _trace_storage = None

    def __init__(self):
        raise NotImplementedError("Cannot instantiate global config")

    @classmethod
    def _parse_strategy(cls, strategy_dict):
        """
        Parse strategy entry

        :param strategy_dict: strategy entry in config to be parsed
        """
        if strategy_dict is None:
            return

        if not strategy_dict:
            raise ValueError('Strategy not defined properly in global configuration section')

        if 'name' not in strategy_dict:
            raise ValueError('Sampling strategy stated in global configuration but no strategy name defined')

        if 'import' in strategy_dict:
            cls.strategy_module = strategy_dict['import']

        module = importlib.import_module(cls.strategy_module)

        strategy_func = getattr(module, strategy_dict['name'])

        if strategy_dict.get('args') is not None and not isinstance(strategy_dict['args'], dict):
            raise ValueError('Arguments to strategy function should be stated as dict')
        user_args = strategy_dict.get('args', {})

        user_args_keys = user_args.keys()
        func_args = set(get_function_arguments(strategy_func))

        if (func_args - user_args_keys) != _EXPECTED_STRATEGY_FUNC_ARGS:
            raise ValueError('Unknown or invalid arguments supplied to sampling strategy function, expected %s, got %s'
                             % ((func_args - _EXPECTED_STRATEGY_FUNC_ARGS), set(user_args_keys)))
        else:
            cls.strategy_func_args = user_args

    @classmethod
    def dump_trace(cls, output, config_name, indent_count=0):
        """
        Dump trace configuration to output stream

        :param output: output stream to write to
        :param config_name: name of configuration class instance to be referenced when initializing trace
        :param indent_count: indentation that should be used to indent source
        """
        indent = indent_count * 4 * " "

        if cls._trace_logging:
            output.write('%s%s.trace_by_logging()\n' % (indent, config_name))
            return

        if cls._trace_storage:
            output.write('%s%s.trace_by_func(functools.partial(%s.%s, %s)\n'
                         % (indent, config_name, cls._trace_storage.class_name,
                            cls._trace_function, cls._trace_storage.var_name))

        if cls._trace_import:
            output.write('%sfrom %s import %s\n' % (indent, cls._trace_import, cls._trace_function))
            output.write('%s%s.trace_by_func(%s)\n' % (indent, config_name, cls._trace_function))

    @classmethod
    def _parse_trace(cls, system, trace_record):
        """
        Parse trace configuration entry

        @param system: system instance for which the parsing is done (for storage lookup)
        @param trace_record: trace record to be parsed
        """
        if trace_record is None:
            raise ValueError('Trace not defined properly in global configuration section')

        if trace_record is False:
            return

        if trace_record is True:
            cls._trace_logging = True
            return

        if 'storage' in trace_record:
            if 'method' in trace_record:
                cls._trace_function = trace_record['method']
            else:
                cls._trace_function = 'trace'

            storage = system.storage_by_name(trace_record['storage'])
            cls._trace_storage = storage
        else:
            if 'import' not in trace_record:
                raise ValueError('Expected import definition if trace is not logging nor storage')

            if 'function' not in trace_record:
                raise ValueError('Expected function definition if trace is not logging nor storage')

            cls._trace_import = trace_record['import']
            cls._trace_function = trace_record['function']

    @classmethod
    def from_dict(cls, system, d):
        """
        Parse global configuration from a dictionary

        :param system: system instance for storage lookup
        :param d: dictionary containing global configuration as stated in YAML config file
        """
        if 'predicates_module' in d:
            cls.predicates_module = d['predicates_module']

        if 'strategy' in d:
            cls._parse_strategy(d['strategy'])

        if 'trace' in d:
            cls._parse_trace(system, d['trace'])

        cls.config_version = d.get('config_version', _DEFAULT_CONFIG_VERSION)
        cls.default_task_version = d.get('default_task_version', _DEFAULT_TASK_VERSION)

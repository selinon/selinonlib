#!/usr/bin/env python

import importlib
from .helpers import get_function_arguments


class Strategy(object):
    _DEFAULT_MODULE = 'selinonlib.strategies'
    _DEFAULT_FUNCTION = 'biexponential_increase'
    _DEFAULT_FUNC_ARGS = {'start_retry': 2, 'max_retry': 120}

    _EXPECTED_STRATEGY_FUNC_ARGS = {'previous_retry', 'active_nodes', 'failed_nodes',
                                    'new_started_nodes', 'new_fallback_nodes'}

    def __init__(self, module=None, function=None, func_args=None):
        self.module = module or self._DEFAULT_MODULE
        self.function = function or self._DEFAULT_FUNCTION
        self.func_args = func_args or self._DEFAULT_FUNC_ARGS

    @classmethod
    def from_dict(cls, strategy_dict):
        """
        Parse strategy entry

        :param strategy_dict: strategy entry in config to be parsed
        """
        if strategy_dict is None:
            return cls()

        if not isinstance(strategy_dict, dict):
            raise ValueError('Strategy not defined properly in global configuration section, expected dict, got {}'
                             % strategy_dict)

        if 'name' not in strategy_dict:
            raise ValueError('Sampling strategy stated in global configuration but no strategy name defined')

        if not isinstance(strategy_dict['args'], dict):
            raise ValueError('Arguments to strategy function should be stated as dict, got %s instead'
                             % strategy_dict['args'])

        strategy_module = strategy_dict.get('import', cls._DEFAULT_MODULE)

        raw_module = importlib.import_module(strategy_module)
        raw_func = getattr(raw_module, strategy_dict['name'])

        # perform checks on args supplied
        user_args_keys = strategy_dict['args'].keys()
        func_args = set(get_function_arguments(raw_func))

        if (func_args - user_args_keys) != cls._EXPECTED_STRATEGY_FUNC_ARGS:
            raise ValueError('Unknown or invalid arguments supplied to sampling strategy function, expected %s, got %s'
                             % ((func_args - cls._EXPECTED_STRATEGY_FUNC_ARGS), set(user_args_keys)))

        return cls(strategy_module, strategy_dict['name'], strategy_dict['args'])



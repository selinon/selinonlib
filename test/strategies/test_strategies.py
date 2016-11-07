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

import pytest
from functools import partial
from selinonlib.strategies import (
    linear_increase,
    linear_adapt,
    biexponential_increase,
    biexponential_decrease,
    biexponential_adapt,
    random,
    constant
)

# Values used in tests for strategies functions
_TEST_START_RETRY = 2
_TEST_MAX_RETRY = 20
_TEST_STEP_RETRY = 5
_TEST_PREV_RETRY = 42

# Available strategies that take 1 additional arguments
_STRATEGIES_ONE_ARG = [
    partial(constant, retry=_TEST_START_RETRY)
]

# Available strategies that take 2 additional arguments
_STRATEGIES_TWO_ARG = [
    partial(biexponential_increase, start_retry=_TEST_START_RETRY, max_retry=_TEST_MAX_RETRY),
    partial(biexponential_decrease, start_retry=_TEST_START_RETRY, stop_retry=_TEST_MAX_RETRY),
    partial(biexponential_adapt, start_retry=_TEST_START_RETRY, max_retry=_TEST_MAX_RETRY),
    partial(random, start_retry=_TEST_START_RETRY, max_retry=_TEST_MAX_RETRY)
]


# Available strategies that take 3 additional arguments
_STRATEGIES_THREE_ARG = [
    partial(linear_increase, start_retry=_TEST_START_RETRY, max_retry=_TEST_MAX_RETRY, step=_TEST_STEP_RETRY),
    partial(linear_adapt, start_retry=_TEST_START_RETRY, max_retry=_TEST_MAX_RETRY, step=_TEST_STEP_RETRY)
]

_STRATEGIES_ALL_ARG = _STRATEGIES_ONE_ARG + _STRATEGIES_TWO_ARG + _STRATEGIES_THREE_ARG


@pytest.mark.parametrize("strategy", _STRATEGIES_ALL_ARG)
class TestStrategies(object):
    def test_start(self, strategy):
        args = {
            'previous_retry': None,
            'active_nodes': ['Task1', 'Task2'],
            'failed_nodes': [],
            'new_started_nodes': ['Task1', 'Task2'],
            'new_fallback_nodes': [],
            'finished_nodes': []
        }

        retry = strategy(**args)
        assert retry is not None and retry > 0

    def test_only_active(self, strategy):
        args = {
            'previous_retry': _TEST_PREV_RETRY,
            'active_nodes': ['Task1', 'Task2'],
            'failed_nodes': [],
            'new_started_nodes': [],
            'new_fallback_nodes': [],
            'finished_nodes': []
        }

        retry = strategy(**args)
        assert retry is not None and retry > 0

    def test_active_and_started(self, strategy):
        args = {
            'previous_retry': _TEST_PREV_RETRY,
            'active_nodes': ['Task1', 'Task2'],
            'failed_nodes': [],
            'new_started_nodes': ['Task3'],
            'new_fallback_nodes': [],
            'finished_nodes': []
        }

        retry = strategy(**args)
        assert retry is not None and retry > 0

    def test_finish(self, strategy):
        args = {
            'previous_retry': _TEST_PREV_RETRY,
            'active_nodes': [],
            'failed_nodes': [],
            'new_started_nodes': [],
            'new_fallback_nodes': [],
            'finished_nodes': []
        }

        retry = strategy(**args)
        assert retry is None


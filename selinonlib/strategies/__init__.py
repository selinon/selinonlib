#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""
Functions for scheduling dispatcher - Dispatcher strategy configuration functions. All of them are receiving a dict
as a first argument containing:

  * previous_retry - value of previous retry, None if scheduling for the first time
  * active_nodes - active nodes within the system
  * failed_nodes - failed nodes within the system
  * new_started_nodes - newly started nodes in the system, these nodes are already in active_nodes
  * new_fallback_nodes - newly started fallback nodes, these nodes are already in active_nodes
  * finished nodes - already finished nodes

Other parameters are specific for strategy function and are configurable from YAML configuration file.
"""

from random import randint as gen_random

# There are done checks on user-defined strategies, so keep args
# pylint: disable=unused-argument


def linear_increase(status, start_retry, max_retry, step):
    """
    Increase linearly if no node started, decrease drastically to start_retry if no node scheduled

    :param status: flow status dict
    :param start_retry: starting retry to use
    :param max_retry: upper limit of scheduling
    :param step: step to use in linear increase
    """
    if len(status['active_nodes']) == 0:
        return None

    if status['previous_retry'] is None:
        return start_retry

    if len(status['new_started_nodes']) > 0 or len(status['new_fallback_nodes']) > 0:
        retry = status['previous_retry'] + step
        return retry if retry < max_retry else max_retry
    else:
        return start_retry


def linear_adapt(status, start_retry, max_retry, step):
    """
    Increase linearly if no node started, decrease linearly if a node scheduled

    :param status: flow status dict
    :param start_retry: starting retry to use
    :param max_retry: upper limit of scheduling
    :param step: step to use in linear increase
    """
    if len(status['active_nodes']) == 0:
        return None

    if status['previous_retry'] is None:
        return start_retry

    if len(status['new_started_nodes']) > 0 or len(status['new_fallback_nodes']) > 0:
        retry = status['previous_retry'] + step
        return retry if retry < max_retry else max_retry
    else:
        retry = status['previous_retry'] - step
        return retry if retry > start_retry else start_retry


def biexponential_increase(status, start_retry, max_retry):
    """
    Increase exponentially if no node started, decrease drastically to start_retry if a node scheduled

    :param status: flow status dict
    :param start_retry: starting retry to use
    :param max_retry: upper limit of scheduling
    """
    if len(status['active_nodes']) == 0:
        return None

    if status['previous_retry'] is None:
        return start_retry

    if len(status['new_started_nodes']) > 0 or len(status['new_fallback_nodes']) > 0:
        retry = status['previous_retry'] * 2
        return retry if retry < max_retry else max_retry
    else:
        return start_retry


def biexponential_decrease(status, start_retry, stop_retry):
    """
    Decrease by div 2 each time if no node started, decrease to stop_retry if a node scheduled

    :param status: flow status dict
    :param start_retry: starting retry to use
    :param stop_retry: upper limit of scheduling
    """
    if len(status['active_nodes']) == 0:
        return None

    if status['previous_retry'] is None:
        return start_retry

    retry = status['previous_retry'] / 2
    return retry if retry > stop_retry else stop_retry


def biexponential_adapt(status, start_retry, max_retry):
    """
    Increase exponentially if no node started, decrease exponentially if a node scheduled

    :param status: flow status dict
    :param start_retry: starting retry to use
    :param max_retry: upper limit of scheduling
    """
    if status['previous_retry'] is None:
        return start_retry

    if len(status['active_nodes']) == 0:
        return None

    if len(status['new_started_nodes']) > 0 or len(status['new_fallback_nodes']) > 0:
        retry = status['previous_retry'] * 2
        return retry if retry < max_retry else max_retry
    else:
        retry = status['previous_retry'] / 2
        return retry if retry > start_retry else start_retry


def random(status, start_retry, max_retry):
    """
    Schedule randomly

    :param status: flow status dict
    :param start_retry: lower limit of scheduling
    :param max_retry: upper limit of scheduling
    """
    if len(status['active_nodes']) == 0:
        return None

    return gen_random(start_retry, max_retry)


def constant(status, retry):
    """
    Schedule randomly
    :param status: flow status dict
    :param retry: constant retry timeout
    """
    if len(status['active_nodes']) == 0:
        return None

    return retry

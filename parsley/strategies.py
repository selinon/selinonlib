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

from random import randint as gen_random


def linear_increase(start_retry, max_retry, step,
                    previous_retry, active_count, failed_count, started_count, fallback_count):
    if started_count > 0 or fallback_count > 0:
        retry = previous_retry + step
        if retry > max_retry:
            return max_retry
        else:
            return retry
    else:
        return start_retry


def linear_adopt(start_retry, max_retry, step,
                 previous_retry, active_count, failed_count, started_count, fallback_count):
    if started_count > 0 or fallback_count > 0:
        retry = previous_retry + step
        if retry > max_retry:
            return max_retry
        else:
            return retry
    else:
        retry = previous_retry - step
        if retry < start_retry:
            return start_retry
        else:
            return retry


def biexponential_increase(start_retry, max_retry,
                           previous_retry, active_count, failed_count, started_count, fallback_count):
    if started_count > 0 or fallback_count > 0:
        retry = previous_retry * 2
        if retry > max_retry:
            return max_retry
        else:
            return retry
    else:
        return start_retry


def biexponential_adopt(start_retry, max_retry, step,
                        previous_retry, active_count, failed_count, started_count, fallback_count):
    if started_count > 0 or fallback_count > 0:
        retry = previous_retry * 2
        if retry > max_retry:
            return max_retry
        else:
            return retry
    else:
        retry = previous_retry / 2
        if retry < start_retry:
            return start_retry
        else:
            return retry


def random(start_retry, max_retry, previous_retry, active_count, failed_count, started_count, fallback_count):
    return gen_random(start_retry, max_retry)




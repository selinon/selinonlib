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

import logging


class _LoggerWrapper(object):
    """
    Logger wrapper to be sure that logger is called with a correct logging level in the whole library after imports
    """
    def __init__(self, name):
        self._instance = None
        self._name = name

    def __getattr__(self, item):
        if item in ['_name', '_level', '_instance']:
            return super(_LoggerWrapper, self).__getattribute__(item)
        if not self._instance:
            logging.basicConfig(level=Logger.get_level())
            self._instance = logging.getLogger(self._name)
        return getattr(self._instance, item)


class Logger(object):
    """
    Library logger
    """
    _verbosity = 0

    @classmethod
    def set_verbosity(cls, verbosity):
        """
        :param verbosity: logger verbosity
        :type verbosity: int
        """
        cls._verbosity = verbosity

    @classmethod
    def get_level(cls):
        """
        :return: logging level for logging module
        """
        if not cls._verbosity:
            return logging.WARNING

        if cls._verbosity == 0:
            return logging.WARNING
        elif cls._verbosity == 1:
            return logging.INFO
        elif cls._verbosity > 1:
            return logging.DEBUG

    @classmethod
    def get_logger(cls, name):
        """
        Get logging logger
        :param name: logging logger name
        :rtype: _LoggerWrapper
        """
        return _LoggerWrapper(name)

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

import ast
import importlib
from .predicate import Predicate
from .helpers import dict2strkwargs, get_function_arguments
from .logger import Logger
from .globalConfig import GlobalConfig

_logger = Logger.get_logger(__name__)


class LeafPredicate(Predicate):
    """
    Leaf predicate representation
    """
    def __init__(self, predicate_func, node, flow, args=None):
        """
        :param predicate_func: predicate function
        :param node: node that predicate conforms to
        :param flow: flow to which predicate belongs to
        :param args: predicate arguments that should be used
        """
        self.node = node
        self.flow = flow

        self._func = predicate_func
        self._args = args if args is not None else {}
        self._func_args = get_function_arguments(self._func)

    def requires_message(self):
        """
        :return: True if predicate requires a message from a parent node
        """
        return 'message' in self._func_args

    def requires_node_args(self):
        """
        :return: True if predicate requires a message from a parent node
        """
        return 'node_args' in self._func_args

    def _check_parameters(self):
        """
        Check user defined predicate parameters against predicate parameters

        :raises: ValueError
        """
        func_args = get_function_arguments(self._func)
        user_args = self._args.keys()

        if 'message' in func_args:
            # message argument is implicit and does not need to be specified by user
            func_args.remove('message')

        if 'node_args' in func_args:
            # node_args are implicit as well
            func_args.remove('node_args')

        func_args = set(func_args)
        user_args = set(user_args)

        error = False

        for arg in func_args - user_args:
            _logger.error("Argument '%s' of predicate '%s' not specified in flow '%s'"
                          % (arg, self._func.__name__, self.flow.name))
            error = True

        for arg in user_args - func_args:
            _logger.error("Invalid argument '%s' for predicate '%s' in flow '%s'"
                          % (arg, self._func.__name__, self.flow.name))
            error = True

        if error:
            raise ValueError("Bad predicate arguments specified in flow '%s'" % self.flow.name)

    def _check_usage(self):
        """
        Check correct predicate usage

        :raises: ValueError
        """
        if self.requires_message() and self.node and self.node.is_flow():
            raise ValueError("Results of sub-flows cannot be used in predicates")
        if self.requires_message() and not self.node:
            raise ValueError("Cannot inspect results in starting edge in predicate '%s'" % self._func.__name__)
        if self.requires_message() and not self.node.storage:
            raise ValueError("Cannot use predicate that requires a message without storage '%s'" % self._func.__name__)

    def check(self):
        """
        Check whether predicate is correctly used

        :raises: ValueError
        """
        self._check_usage()
        self._check_parameters()

    def __str__(self):
        if self.requires_message():
            return "%s(db.get('%s'), %s)"\
                   % (self._func.__name__, self._task_str_name(), dict2strkwargs(self._args))
        else:
            # we hide node_args parameter
            return "%s(%s)" % (self._func.__name__, dict2strkwargs(self._args))

    def _task_str_name(self):
        # task_name can be None if we have starting edge
        if self.node is None:
            return 'None'
        else:
            return "%s" % self.node.name

    def ast(self):
        """
        :return: AST representation of predicate
        """
        # we could directly use db[task] in predicates, but predicates should not handle database errors,
        # so leave them on higher level (celeriac) and index database before predicate is being called

        kwargs = []
        # we want to avoid querying to database if possible, if a predicate does not require message, do not ask for it
        if self.requires_message():
            # this can raise an exception if check was not run, since we are accessing storage that can be None
            kwargs.append(ast.keyword(arg='message',
                                      value=ast.Call(func=ast.Attribute(value=ast.Name(id='db', ctx=ast.Load()),
                                                                        attr='get', ctx=ast.Load()),
                                                     args = [ast.Str(s=self._task_str_name())],
                                                     keywords=[], starargs = None, kwargs = None)))
        if self.requires_node_args():
            kwargs.append(ast.keyword(arg='node_args', value=ast.Name(id='node_args', ctx=ast.Load())))

        kwargs.extend([ast.keyword(arg=k, value=ast.Str(s=v)) for k, v in self._args.items()])

        return ast.Call(func=ast.Name(id=self._func.__name__, ctx=ast.Load()),
                        args=[], starargs=None, kwargs=None, keywords=kwargs)

    def predicates_used(self):
        """
        :return: list of predicates that are used
        :rtype: List[Predicate]
        """
        return [self._func]

    @classmethod
    def create(cls, name, node, flow, args=None):
        """
        Create predicate

        :param name: predicate name
        :type name: str
        :param node: node to which predicate belongs
        :type node: Node
        :param flow: flow to which predicate belongs
        :type flow: Flow
        :param args: predicate arguments
        :return: an instantiated predicate
        :raises: ImportError
        """
        try:
            module = importlib.import_module(GlobalConfig.predicates_module)
            predicate = getattr(module, name)
        except ImportError:
            _logger.error("Cannot import predicate '{}'".format(name))
            raise
        return LeafPredicate(predicate, node, flow, args)

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

import ast
import sys
import importlib
from dill.source import getsource
from .predicate import Predicate
from .helpers import dict2strkwargs
from .logger import Logger

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
        self._func = predicate_func
        self._node = node
        self._args = args if args is not None else {}
        self._flow = flow

    def requires_message(self):
        """
        :return: True if predicate requires a message from a parent node
        """
        return 'message' in self._get_arguments()

    def _get_arguments(self):
        """
        :return: list of arguments that predicate function expects
        """
        ret = []

        func_source = getsource(self._func)
        func_ast = ast.parse(func_source)
        call = func_ast.body[0]

        for arg in call.args.args:
            # Python2 and Python3 have different AST representations for arg
            if sys.version_info[0] == 2:
                ret.append(arg.id)
            else:
                ret.append(arg.arg)

        return ret

    def _check_parameters(self):
        """
        Check user defined predicate parameters against predicate parameters
        :raises: ValueError
        """
        func_args = self._get_arguments()
        user_args = self._args.keys()

        if 'message' in func_args:
            # message argument is implicit and does not need to be specified by user
            func_args.remove('message')

        func_args = set(func_args)
        user_args = set(user_args)

        error = False

        for arg in func_args - user_args:
            _logger.error("Argument '%s' of predicate '%s' not specified in node %s"
                          % (arg, self._func.__name__, self.node.name))
            error = True

        for arg in user_args - func_args:
            _logger.error("Invalid argument '%s' for predicate '%s' in node '%s'"
                          % (arg, self._func.__name__, self.node.name))
            error = True

        if error:
            raise ValueError("Bad predicate arguments specified in node '%s'" % self.node.name)

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
            return "%s(db.get('%s', '%s'), %s)"\
                   % (self._func.__name__, self._flow.name, self._task_str_name(), dict2strkwargs(self._args))
        else:
            return "%s(%s)" % (self._func.__name__, dict2strkwargs(self._args))

    @property
    def node(self):
        """
        :return: node to which predicate corresponds
        :rtype: Node
        """
        return self._node

    @property
    def flow(self):
        """
        :return: flow to which predicate belongs to
        :rtype: Flow
        """
        return self._flow

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

        # we want to avoid querying to database if possible, if a predicate does not require message, do not ask for it
        if self.requires_message():
            # this can raise an exception if check was not run, since we are accessing storage that can be None
            args = [ast.Call(func=ast.Attribute(value=ast.Name(id='db', ctx=ast.Load()), attr='get', ctx=ast.Load()),
                             args = [ast.Str(s=self._flow.name), ast.Str(s=self._task_str_name())],
                             keywords=[], starargs = None, kwargs = None)]
        else:
            args = []

        return ast.Call(func=ast.Name(id=self._func.__name__, ctx=ast.Load()),
                        args=args, starargs=None, kwargs=None,
                        keywords=[ast.keyword(arg=k, value=ast.Str(s=v)) for k, v in self._args.items()])

    def predicates_used(self):
        """
        :return: list of predicates that are used
        :rtype: List[Predicate]
        """
        return [self._func]

    @staticmethod
    def create(name, node, flow, args=None):
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
        # TODO: make predicates module configurable so a user can define his own predicates? possibly...
        try:
            module = importlib.import_module('parsley.predicates.%s' % name)
        except ImportError:
            _logger.error("Cannot import predicate '{}'".format(name))
            raise
        return LeafPredicate(getattr(module, name), node, flow, args)

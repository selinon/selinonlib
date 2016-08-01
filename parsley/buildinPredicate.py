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
from functools import reduce
from .logger import Logger
from .predicate import Predicate


_logger = Logger.get_logger(__name__)


class BuildinPredicate(Predicate):
    """
    Build in predicate abstract class
    """
    pass


class NaryPredicate(BuildinPredicate):
    """
    N-ary predicate abstract class
    """
    def __init__(self, children):
        self._children = children

    def _str(self, op):
        ret = ""
        for child in self._children:
            if len(ret) > 0:
                ret += " %s " % op
            ret += str(child)

        if len(self._children) > 1:
            ret = "(" + ret + ")"

        return ret

    @staticmethod
    def _create(tree, cls, nodes_from):
        """
        Instantiate N-ary predicate cls
        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param cls: class of type NaryPredicate
        :param nodes_from: nodes that are used in described edge
        :return: instance of cls
        """
        if not isinstance(tree, list):
            raise ValueError("Nary logical operators expect list of children")
        children = []
        for child in tree:
            children.append(Predicate.construct(child, nodes_from))
        return cls(children)

    def predicates_used(self):
        """
        :return: used predicates by children
        """
        return reduce(lambda x, y: x + y.predicates_used(), self._children, [])


class UnaryPredicate(BuildinPredicate):
    """
    Unary predicate abstract class
    """
    def __init__(self, child):
        self._child = child

    @staticmethod
    def _create(tree, cls, nodes_from):
        """
        Instantiate N-ary predicate cls
        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param cls: class of type NaryPredicate
        :param nodes_from: nodes that are used in described edge
        :return: instance of cls
        """
        if isinstance(tree, list):
            raise ValueError("Unary logical operators expect one child")
        return cls(Predicate.construct(tree, nodes_from))

    def predicates_used(self):
        """
        :return: used predicates by children
        """
        return self._child.predicates_used()


class AndPredicate(NaryPredicate):
    """
    And predicate representation
    """
    def __str__(self):
        return "(" + reduce(lambda x, y: str(x) + ' and ' + str(y), self._children) + ")"

    def ast(self):
        """
        :return: AST of describing all children predicates
        """
        return ast.BoolOp(ast.And(), [ast.Expr(value=x.ast()) for x in self._children])

    @staticmethod
    def create(tree, nodes_from):
        """
        Create And predicate
        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param nodes_from: nodes that are used in described edge
        :return: instance of cls
        """
        return NaryPredicate._create(tree, AndPredicate, nodes_from)


class OrPredicate(NaryPredicate):
    """
    And predicate representation
    """
    def __str__(self):
        return "(" + reduce(lambda x, y: str(x) + ' or ' + str(y), self._children) + ")"

    def ast(self):
        """
        :return: AST of describing all children predicates
        """
        return ast.BoolOp(ast.Or(), [ast.Expr(value=x.ast()) for x in self._children])

    @staticmethod
    def create(tree, nodes_from):
        """
        Create Or predicate
        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param nodes_from: nodes that are used in described edge
        :return: instance of cls
        """
        return NaryPredicate._create(tree, OrPredicate, nodes_from)


class NotPredicate(UnaryPredicate):
    """
    Unary or predicate representation
    """
    def __str__(self):
        return "(not %s)" % str(self._child)

    def ast(self):
        """
        :return: AST of describing all children predicates
        """
        return ast.UnaryOp(ast.Not(), ast.Expr(value=self._child.ast()))

    @staticmethod
    def create(tree, nodes_from):
        """
        Create Or predicate
        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param nodes_from: nodes that are used in described edge
        :return: instance of cls
        """
        return UnaryPredicate._create(tree, NotPredicate, nodes_from)

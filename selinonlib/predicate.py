#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Predicate interface - predicate for building conditions."""

import abc

from .helpers import check_conf_keys
from .helpers import dict2json


class Predicate(metaclass=abc.ABCMeta):
    """An abstract predicate representation."""

    @abc.abstractmethod
    def __init__(self):
        """Instantiate predicate representation."""
        pass

    @abc.abstractmethod
    def __str__(self):
        """String representation of this predicate.

        :return: string representation (Python code)
        :rtype: str
        """
        pass

    @abc.abstractclassmethod
    def create(cls, tree, nodes_from, flow):
        """Create the predicate.

        :param tree: node from which should be predicate instantiated
        :type tree: List
        :param nodes_from: nodes which are used within edge definition
        :type nodes_from: List[Nodes]
        :param flow: flow to which predicate belongs to
        :type flow: Flow
        :return: Predicate instance
        """
        pass

    @abc.abstractmethod
    def ast(self):
        """Python AST of this predicate (construct transitively for all indirect children as well).

        :return: AST of describing all children predicates
        """
        pass

    @abc.abstractmethod
    def predicates_used(self):
        """Compute all predicates that are used (transitively) by child/children.

        :return: used predicates by children
        :rtype: List[Predicate]
        """
        pass

    @abc.abstractmethod
    def nodes_used(self):
        """Compute all nodes that are used (transitively) by child/children.

        :return: list of nodes that are used
        :rtype: List[Node]
        """
        pass

    @staticmethod
    def construct(tree, nodes_from, flow):  # pylint: disable=too-many-branches
        """Top-down creation of predicates - recursively called to construct predicates.

        :param tree: a dictionary describing nodes
        :type tree: dict
        :param nodes_from: nodes which are used within edge
        :param flow: flow to which predicate belongs to
        :type flow: Flow
        :rtype: Predicate
        """
        from .leafPredicate import LeafPredicate
        from .builtinPredicate import OrPredicate, AndPredicate, NotPredicate

        if not tree:
            raise ValueError("Bad condition '%s'" % tree)

        if 'name' in tree:
            if 'node' in tree:
                node = None
                for node_from in nodes_from:
                    if node_from.name == tree['node']:
                        node = node_from
                        break
                if node is None:
                    raise ValueError("Node listed node '%s' in predicate '%s' is not requested in 'nodes_from'"
                                     % (tree['node'], tree['name']))
            else:
                if len(nodes_from) == 1:
                    node = nodes_from[0]
                else:
                    # e.g. starting edge has no nodes_from
                    node = None

            unknown_conf = check_conf_keys(tree, known_conf_opts=('name', 'node', 'args'))
            if unknown_conf:
                raise ValueError("Unknown configuration option for predicate '%s' in flow '%s': %s"
                                 % (tree['name'], flow.name, unknown_conf.keys()))

            return LeafPredicate.create(tree['name'], node, flow, tree.get('args'))
        elif 'or' in tree:
            return OrPredicate.create(tree['or'], nodes_from, flow)
        elif 'not' in tree:
            return NotPredicate.create(tree['not'], nodes_from, flow)
        elif 'and' in tree:
            return AndPredicate.create(tree['and'], nodes_from, flow)
        else:
            raise ValueError("Unknown predicate:\n%s" % dict2json(tree))

    @abc.abstractmethod
    def check(self):
        """Recursively/transitively check predicate correctness.

        :raises ValueError: if predicate is not correct
        """
        pass

    @abc.abstractmethod
    def requires_message(self):
        """Recursively check if one of the predicates require message from storage (result of previous task).

        :return: True if a result from storage is required
        """
        pass

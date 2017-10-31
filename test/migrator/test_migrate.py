#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import os
import copy
import pytest
from selinonlib.migrator import Migrator
from selinonlib import MigrationSkew
from selinonlibTestCase import SelinonlibTestCase


def migrate_message_test(original_message):
    """A migration test (successful) execution. Files are expected to be named based on test name."""
    def decorator(test):
        def wrapper(test_instance):
            migration_dir = test_instance.get_migration_dir(test.__name__)
            migrator = Migrator(migration_dir)
            migrated_message = migrator.perform_migration(copy.deepcopy(original_message))
            test(test_instance, original_message, migrated_message)
            test_instance.sanity_assert(original_message, migrated_message)

        return wrapper

    return decorator


def migrate_message_exception(exception, original_message):
    """A migration test (exception) execution. Files are expected to be named based on test name."""
    def decorator(test):
        def wrapper(test_instance):
            migration_dir = test_instance.get_migration_dir(test.__name__)
            migrator = Migrator(migration_dir)

            with pytest.raises(exception):
                migrator.perform_migration(copy.deepcopy(original_message))

        return wrapper

    return decorator


class TestMigrate(SelinonlibTestCase):
    """Test actual migration based on generated migrations."""

    def get_migration_dir(self, test_name):
        """Get configuration files for specific test case - path respects test method naming.

        :param test_name: name of test that is going to be executed
        :return: a path to test migration directory
        """
        # Force structure of test data
        return os.path.join(self.DATA_DIR, 'migrator', 'migration_dirs', test_name[len('test_'):])

    @staticmethod
    def sanity_assert(original_message, migrated_message):
        assert 'flow_name' in original_message
        assert 'node_args' in original_message

        # Migration versions cannot decrement
        assert original_message.pop('migration_version', 0) <= migrated_message.pop('migration_version')

        # Messages has to match except migration version field and waiting nodes
        original_message.get('state', {}).pop('waiting_edges', None)
        migrated_message.get('state', {}).pop('waiting_edges', None)
        assert original_message == migrated_message

    @migrate_message_test({'migration_version': 0, 'flow_name': 'flow1', 'node_args': {}})
    def test_zero_migration(self, original_message, migrated_message):
        """Test when no migration is defined and message keeps migration of version 0."""
        assert original_message == migrated_message

    @migrate_message_test({'flow_name': 'flow1', 'node_args': {}})
    def test_no_migration_version(self, original_message, migrated_message):
        """Test no migration version in message - this can occur in case of new flow is scheduled."""
        assert 'migration_version' not in original_message
        assert 'migration_version' in migrated_message
        assert migrated_message['migration_version'] == 0

    @migrate_message_test({'migration_version': 0, 'flow_name': 'flow1', 'node_args': {}})
    def test_one_migration(self, original_message, migrated_message):
        """Test one single migration from version 0 to version 1."""
        assert original_message['migration_version'] == 0
        assert migrated_message['migration_version'] == 1

    @migrate_message_test({'migration_version': 0, 'flow_name': 'flow1', 'node_args': {},
                           'state': {'waiting_edges': [1]}})
    def test_one_migration_with_edges(self, original_message, migrated_message):
        """Test one single migration from version 0 to version 1."""
        assert original_message['migration_version'] == 0
        assert migrated_message['migration_version'] == 1
        # Migration was actually done
        assert len(migrated_message['state']['waiting_edges']) == 1
        assert migrated_message['state']['waiting_edges'][0] == 2

    #@migrate_message_test({'migration_version': 2, 'flow_name': 'flow1', 'node_args': {},
    #                       'state': {'waiting_edges': [1]}})
    def test_migration_chaining(self):
        # TODO: implement
        pass

    #@migrate_message_test({'migration_version': 2, 'flow_name': 'flow1', 'node_args': {},
    #                       'state': {'waiting_edges': [1]}})
    def test_migration_chaining_with_new_active_edge(self):
        # TODO: implement
        pass

    @migrate_message_exception(MigrationSkew, {'migration_version': 2, 'flow_name': 'flow1', 'node_args': {}})
    def test_migration_skew(self):
        """Test signalizing migration skew - migration version is from future."""

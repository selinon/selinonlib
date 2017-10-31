#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Test creation and generation of migration files.

All inputs/outputs used in these sources are present at test/data/migration/input and test/data/migration/output
respectively. Methods have "empty" implementation as name of tests distinguish file location and test decorators
specify test case expectations (failure/success).
"""

import os
import shutil
import tempfile
import pytest
import json
from selinonlib.migrator import Migrator
from selinonlib import MigrationNotNeeded
from selinonlibTestCase import SelinonlibTestCase


def migration_test(test):
    """A migration test (successful) execution. Files are expected to be named based on test name."""
    def wrapper(test_instance):
        old_config_files = test_instance.get_test_config_files(test.__name__, new_config_files=False)
        new_config_files = test_instance.get_test_config_files(test.__name__, new_config_files=True)
        migrator = Migrator(test_instance.migration_dir)
        migration_file_path = migrator.create_migration_file(old_config_files[0], old_config_files[1],
                                                             new_config_files[0], new_config_files[1])
        test_instance.check_migration_match(migration_file_path, test_instance.get_reference_test_output(test.__name__))
        test(test_instance)

    return wrapper


def migration_test_exception(exc):
    """A migration test (exception) execution. Files are expected to be named based on test name."""
    def decorator(test):
        def wrapper(test_instance):
            old_config_files = test_instance.get_test_config_files(test.__name__, new_config_files=False)
            new_config_files = test_instance.get_test_config_files(test.__name__, new_config_files=True)
            migrator = Migrator(test_instance.migration_dir)

            with pytest.raises(exc):
                migrator.create_migration_file(old_config_files[0], old_config_files[1],
                                               new_config_files[0], new_config_files[1])

        return wrapper

    return decorator


class TestCreateMigration(SelinonlibTestCase):
    """Test creation and generation of coniguration migrations."""

    migration_dir = None

    def setup_method(self):
        """Set up migration test - create a temporary migration directory."""
        self.migration_dir = tempfile.mkdtemp(prefix='migration_dir_')

    def teardown_method(self):
        """Clear all temporary files present in the migration directory after each method."""
        shutil.rmtree(self.migration_dir)

    def get_test_config_files(self, test_name, new_config_files=True):
        """Get configuration files for specific test case - path respects test method naming.

        :param test_name: name of test that is going to be executed
        :param new_config_files: True if new config files should be listed
        :return: path to nodes yaml and flow configuration files
        :rtype: tuple
        """
        # Force structure of test data
        test_dir = os.path.join(
            self.DATA_DIR,
            'migrator',
            'input',
            'new' if new_config_files else 'old',
            test_name[len('test_'):]
        )

        nodes_yaml = os.path.join(test_dir, 'nodes.yaml')
        flows_yaml = []
        flows_yaml_path = os.path.join(os.path.join(test_dir, 'flows'))
        for flow_file in os.listdir(flows_yaml_path):
            if flow_file.endswith('.yaml'):
                flows_yaml.append(os.path.join(flows_yaml_path, flow_file))

        return nodes_yaml, flows_yaml

    def get_reference_test_output(self, test_name):
        """Get output for test - path naming respects test method naming."""
        return os.path.join(
            self.DATA_DIR,
            'migrator',
            'output',
            "{test_name}.json".format(test_name=test_name[len('test_'):])
        )

    @staticmethod
    def check_migration_match(migration_file_path, reference_migration_file_path):
        """Check that two migrations match and migration metadata precedence."""
        with open(migration_file_path, 'r') as migration_file:
            migration = json.load(migration_file)

        with open(reference_migration_file_path, 'r') as reference_migration_file:
            reference_migration = json.load(reference_migration_file)

        # we check only migration, metadata may vary
        assert reference_migration.get('migration') == migration.get('migration')

        # check for metadata precedence
        assert '_meta' in migration, "No metadata present in the output!"
        assert 'datetime' in migration['_meta']
        assert 'host' in migration['_meta']
        assert 'selinonlib_version' in migration['_meta']
        assert 'user' in migration['_meta']

    @migration_test_exception(MigrationNotNeeded)
    def test_no_change(self):
        """Test that if no change is done in configuration files, an exception is raised."""

    @migration_test_exception(MigrationNotNeeded)
    def test_add_to(self):
        """Test adding source node - this doesn't require any migrations to be present."""

    @migration_test_exception(MigrationNotNeeded)
    def test_remove_to(self):
        """Test removing source node - this doesn't require any migrations to be present."""

    @migration_test_exception(MigrationNotNeeded)
    def test_add_flow(self):
        """Test adding a new flow - this doesn't require any migrations to be present."""

    @migration_test
    def test_add_from(self):
        """Test adding source node - edge should be discarded (mapping to None)."""

    @migration_test
    def test_remove_from(self):
        """Test removing source node - edge should be discarded (mapping to None)."""

    @migration_test
    def test_remove_edge(self):
        """Test removing whole edge definition.

        Edge should be discarded (mapping to None) and all subsequent indexes shifted.
        """

    @migration_test
    def test_add_edge(self):
        """Test adding whole edge definition.

        Edge should be discarded (mapping to None) and all subsequent indexes shifted.
        """

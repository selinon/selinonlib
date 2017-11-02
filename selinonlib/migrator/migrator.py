#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Migration of configuration files."""

from datetime import datetime
import logging
import os
import platform

import yaml

from selinonlib import MigrationNotNeeded
from selinonlib import MigrationSkew
from selinonlib import selinonlib_version
from selinonlib.helpers import dict2json

_logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Migrator(object):
    """Main class for performing configuration file migrations."""

    def __init__(self, migrations_dir):
        """Initialize migrator.

        :param migrations_dir: a path to directory containing migration files
        """
        self.migrations_dir = migrations_dir
        self.old_nodes_definition = None
        self.old_flow_definitions = {}
        self.new_nodes_definition = None
        self.new_flow_definitions = {}

    def _report_diff_flow(self):
        """Report added and removed flows."""
        new_flows = set(self.new_nodes_definition.get('flows', [])) - set(self.old_nodes_definition.get('flows', []))
        if new_flows:
            _logger.info("Newly introduced flows in your configuration: %s", ", ".join(new_flows))
            for new_flow in new_flows:
                self.new_flow_definitions.pop(new_flow)

        removed_flows = set(self.old_nodes_definition.get('flows', [])) -\
            set(self.new_nodes_definition.get('flows', []))

        if removed_flows:
            _logger.info("Removed flows from your configuration: %s", ", ".join(removed_flows))
            for removed_flow in removed_flows:
                self.old_flow_definitions.pop(removed_flow)

    def _load_flows(self, flow_files, is_old_flow=True):
        """Load flow into instance attributes.

        :param flow_files: a list of paths to flow configuration files
        :type flow_files: list
        :param is_old_flow: true if loading old configuration
        :type is_old_flow: bool
        """
        for flow_file_path in flow_files:
            with open(flow_file_path, 'r') as flow_file:
                content = yaml.safe_load(flow_file)
                for flow in content['flow-definitions']:
                    # Let's store only edges as other configuration options do not affect messages that are
                    # accepted by dispatcher
                    entry = {
                        'edges': flow['edges'],
                        'file_path': flow_file_path
                    }

                    if is_old_flow:
                        self.old_flow_definitions[flow['name']] = entry
                    else:
                        self.new_flow_definitions[flow['name']] = entry

    def _get_new_migration_file_name(self):
        """Generate a new migration file name.

        :return: a name of new migration file where migrations should be stored
        :rtype: str
        """
        if not os.path.isdir(self.migrations_dir):
            _logger.debug("Creating migration directory %r", self.migrations_dir)
            try:
                os.mkdir(self.migrations_dir)
            except Exception as exc:
                raise MigrationSkew("Migration directory does not exist, unable to create a new directory: %s"
                                    % str(exc))

        new_migration_number = 0
        for file_name in os.listdir(self.migrations_dir):
            file_path = os.path.join(self.migrations_dir, file_name)
            if not os.path.isfile(file_path) or not file_name.endswith('.json') or file_name[0] == '.':
                _logger.debug("Skipping %r, not a file nor JSON file (or hidden file)", file_path)
                continue

            migration_number = file_name[:-len('.json')]
            try:
                migration_number = int(migration_number)
            except ValueError as exc:
                raise MigrationSkew("Unable to parse previous migrations, file name %r does not correspond "
                                    "to migration file - migration files should be named numerically"
                                    % file_path) from exc

            new_migration_number = max(migration_number, new_migration_number)

        return str(new_migration_number + 1) + ".json"

    @staticmethod
    def _get_migration_metadata():
        """Add metadata to migration content."""
        try:
            user = os.getlogin()
        except Exception:  # pylint: disable=broad-except
            # Travis CI fails here, but let's be tolerant in other cases as well
            user = None

        return {
            'selinonlib_version': selinonlib_version,
            'host': platform.node(),
            'datetime': str(datetime.utcnow()),
            'user': user
        }

    def _write_migration_file(self, migration):
        """Write computed migration to migration dir."""
        new_migration_file_name = self._get_new_migration_file_name()
        new_migration_file_path = os.path.join(self.migrations_dir, new_migration_file_name)
        migration_file_content = {
            '_meta': self._get_migration_metadata(),
            'migration': migration
        }

        with open(new_migration_file_path, 'w') as migration_file:
            migration_file.write(dict2json(migration_file_content))

        return new_migration_file_path

    @staticmethod
    def _preprocess_edges(edges):
        """Preprocess edges before computing migrations.

        :param edges: edges from the flow YAML configuration file to be preprocessed
        """
        for idx, edge in enumerate(edges):
            for key in edge.keys():
                if key not in ('from', 'to'):
                    edge.pop(key)
            edge['_idx'] = idx
            # We don't care about node order
            if not isinstance(edge['from'], list):
                edge['from'] = [edge['from']] if edge['from'] is not None else []

            if not isinstance(edge['to'], list):
                edge['to'] = [edge['to']] if edge['to'] is not None else []

            edge['from'] = set(edge['from'])
            edge['to'] = set(edge['to'])

    def _calculate_flow_migration(self, old_flow_edges, new_flow_edges):
        """Calculate migration for a flow.

        :param old_flow_edges: edges definition of old flow
        :type old_flow_edges: dict
        :param new_flow_edges: edges definition of new flow
        :param new_flow_edges: dict
        :return: a dict containing flow migration definition
        :rtype: dict
        """
        self._preprocess_edges(old_flow_edges)
        self._preprocess_edges(new_flow_edges)

        # Let's first construct subset of edges that was not affected by user's change
        for old_edge_idx, old_edge in enumerate(old_flow_edges):
            for new_edge_idx, new_edge in enumerate(new_flow_edges):
                if old_edge == new_edge:
                    old_edge['_new_edge'] = new_edge_idx
                    new_edge['_old_edge'] = old_edge_idx
                    break

        for new_edge_idx, new_edge in enumerate(new_flow_edges):
            for old_edge_idx, old_edge in enumerate(old_flow_edges):
                if old_edge.get('_new_edge') is not None:
                    continue
                if new_edge == old_edge:
                    new_edge['_old_edge_idx'] = old_edge_idx
                    old_edge['_new_edge_idx'] = new_edge_idx
                    break

        old_unmatched = [edge for edge in old_flow_edges if '_new_edge' not in edge.keys()]
        new_unmatched = [edge for edge in new_flow_edges if '_old_edge' not in edge.keys()]

        migration = {}
        for old_edge in old_unmatched:
            matched = False
            for new_edge in new_unmatched:
                if old_edge['from'] == new_edge['from']:
                    migration[old_edge['_idx']] = new_edge['_idx']
                    matched = True

            if not matched:
                migration[old_edge['_idx']] = None

        return migration

    def _calculate_migrations(self):
        """Calculate migration of configuration files and store output in migration directory.

        :return: a path to newly created migration file in migration directory
        """
        migrations = {}
        for flow_name in self.old_flow_definitions:
            old_flow = self.old_flow_definitions[flow_name]
            new_flow = self.new_flow_definitions[flow_name]
            migration = self._calculate_flow_migration(old_flow['edges'], new_flow['edges'])
            if migration:
                migrations[flow_name] = migration

        if not migrations:
            raise MigrationNotNeeded("Flow configuration changes do not require new migration")

        return self._write_migration_file(migrations)

    def create_migration_file(self, old_nodes_definition_path, old_flow_definitions_path,
                              new_nodes_definition_path, new_flow_definitions_path):
        """Generate migration of configuration files, store output in the migration directory.

        :param old_nodes_definition_path: a path to old nodes.yaml
        :type old_nodes_definition_path: str
        :param old_flow_definitions_path: a list of paths to old flow definition files
        :type old_flow_definitions_path: list
        :param new_nodes_definition_path: a path to new nodes.yaml
        :type new_nodes_definition_path: str
        :param new_flow_definitions_path: a list of paths to new flow definition files
        :type new_flow_definitions_path: list
        :return: a path to newly created migration file
        """
        _logger.info("Performing configuration files migrations, storing results in %r", self.migrations_dir)

        with open(old_nodes_definition_path, 'r') as old_nodes_definition_file:
            self.old_nodes_definition = yaml.safe_load(old_nodes_definition_file)

        self._load_flows(old_flow_definitions_path, is_old_flow=True)

        with open(new_nodes_definition_path, 'r') as new_nodes_definition_file:
            self.new_nodes_definition = yaml.safe_load(new_nodes_definition_file)

        _logger.info("Calculating config file migrations")
        self._load_flows(new_flow_definitions_path, is_old_flow=False)
        self._report_diff_flow()
        return self._calculate_migrations()

    @staticmethod
    def _do_migration(migration_spec, flow_name, message):
        """Do single migration of message based on migration definition."""
        flow_migration = migration_spec.get(flow_name)
        if not flow_migration:
            # Nothing to do, no changes in flow in the migration
            return message

        for idx, waiting_edge in enumerate(message['waiting_edges']):
            if str(waiting_edge) in flow_migration:
                message['waiting_edges'][idx] = flow_migration[str(idx)]

        message['waiting_edges'] = list(filter(lambda x: x is not None, message['waiting_edges']))

    def perform_migration(self, flow_name, message):
        """Perform actual migration based on message received.

        :param flow_name: name of flow for which the migration is performed
        :param message: massage that was retrieved and keeps information about the system state
        :return: migrated message
        """
        migration_version = message.get('migration_version')
        if migration_version is None:
            raise RuntimeError("Migration version not present in the message.")

        if migration_version == 0:
            _logger.debug("No migrations needed")
            return message

        nodes2start = []
        while True:
            current_migration_version = migration_version + 1
            current_migration_path = os.path.join(self.migrations_dir, str(current_migration_version) + '.json')

            try:
                with open(current_migration_path, 'r') as migration_file:
                    migration_spec = yaml.safe_load(migration_file)
            except FileNotFoundError:
                _logger.debug("Migration done from %d to version %d, last migration file is %r",
                              migration_version, current_migration_version, current_migration_path)
                break

            message, nodes2start = self._do_migration(migration_spec, flow_name, message)

        return message, nodes2start

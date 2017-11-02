#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Errors and exceptions that can occur in Selinonlib code base."""


class FatalTaskError(Exception):
    """An exception that is raised by task on fatal error - task will be not retried."""


class MigrationNotNeeded(Exception):
    """Raised when a migration is requested, but config changes do not require it."""


class MigrationSkew(Exception):
    """Raised if desired migration does not exist - migration file not present (message from future migrations)."""


class UnknownFlowError(Exception):
    """Raised if there was requested or referenced flow that is not stated in the YAML configuration file."""


class UnknownStorageError(Exception):
    """Raised if there was requested or referenced storage that is not stated in the YAML configuration file."""


class UnknownTaskError(Exception):
    """Raised if there was requested or referenced storage that is not stated in the YAML configuration file."""


class UnknownCacheError(Exception):
    """Raised if there was requested or referenced cache that is not stated in the YAML configuration file."""


class ConfigurationError(Exception):
    """Raised on errors that indicate errors in the configuration files."""


class SelectiveNoPathError(Exception):
    """Raised when there is no path in the flow to requested node in selective task runs."""


class NoParentNodeError(Exception):
    """An exception raised when requested parent node (task/flow), but no such parent defined."""


class RequestError(Exception):
    """An error raised if there was an issue with request issued by user - usually means bad usage error."""


class UnknownError(Exception):
    """An error raised on unknown scenarios - possibly some bug in code."""

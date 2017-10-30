#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Errors and exceptions that can occur in Selinonlib code base."""


class MigrationNotNeeded(Exception):
    """Raised when a migration is requested, but config changes do not require it."""

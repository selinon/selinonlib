#!/usr/bin/env python3

import os


def envEqual(env, value):
    if env not in os.environ:
        return False
    return os.environ[env] == value

#!/bin/env python3

from functools import reduce


def argsFieldExist(node_args, key):
    try:
        reduce(lambda m, k: m[k], key if isinstance(key, list) else [key], node_args)
        return True
    except:
        return False

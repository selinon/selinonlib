#!/usr/bin/env python

from functools import reduce


def fieldLenNotEqual(message, key, length):
    try:
        val = reduce(lambda m, k: m[k], key if isinstance(key, list) else [key], message)
        return len(val) != length
    except:
        return False

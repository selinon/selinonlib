#!/usr/bin/env python3

from functools import reduce


def fieldLenGreaterEqual(message, key, length):
    try:
        val = reduce(lambda m, k: m[k], key if isinstance(key, list) else [key], message)
        return len(val) <= length
    except:
        return False

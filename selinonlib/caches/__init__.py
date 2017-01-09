#!/usr/bin/env python3
"""Implementation of some well-known caches for Selinon"""

from .cache import Cache
from .cacheMissError import CacheMissError
from .fifo import FIFO
from .lifo import LIFO
from .lru import LRU
from .mru import MRU
from .rr import RR

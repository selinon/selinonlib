#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################

import pytest
from selinonlib.caches import (FIFO, LIFO, LRU, MRU, RR, CacheMissError)

# Available caches that should be tested
_CACHE_TYPES = [
    FIFO,
    LIFO,
    LRU,
    MRU,
    RR
]


class TestCache(object):
    """
    Generic cache test cases
    """
    @staticmethod
    def _item_id2item(i):
        return "x%d" % i

    @pytest.mark.parametrize("cache_cls", _CACHE_TYPES)
    def test_empty_miss(self, cache_cls):
        cache = cache_cls(max_cache_size=3)

        with pytest.raises(CacheMissError):
            cache.get("item_id")

    @pytest.mark.parametrize("cache_cls", _CACHE_TYPES)
    def test_zero_items(self, cache_cls):
        cache = cache_cls(max_cache_size=0)

        with pytest.raises(CacheMissError):
            cache.get("item_id1")

        cache.add("item_id1", "item")

        with pytest.raises(CacheMissError):
            cache.get("item_id1") == "item"

    @pytest.mark.parametrize("cache_cls", _CACHE_TYPES)
    def test_one_item(self, cache_cls):
        cache = cache_cls(max_cache_size=1)

        cache.add("item_id1", "item")
        assert cache.get("item_id1") == "item"

    @pytest.mark.parametrize("cache_cls", _CACHE_TYPES)
    def test_two_items(self, cache_cls):
        cache = cache_cls(max_cache_size=2)

        cache.add("item_id1", 1)
        assert cache.get("item_id1") == 1

        cache.add("item_id2", 2)
        assert cache.get("item_id2") == 2

    @pytest.mark.parametrize("cache_cls", _CACHE_TYPES)
    def test_multiple_items(self, cache_cls):
        item_count = 16
        cache = cache_cls(max_cache_size=item_count)

        for item_id in range(item_count):
            cache.add(item_id, self._item_id2item(item_id))
            assert cache.get(item_id) == self._item_id2item(item_id)

        for item_id in range(item_count - 1, -1, -1):
            assert cache.get(item_id) == self._item_id2item(item_id)


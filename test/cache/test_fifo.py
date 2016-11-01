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
from selinonlib.cache import (FIFO, CacheMissError)


class TestFIFO(object):
    """
    Test First-In-First-Out cache
    """
    @staticmethod
    def _item_id2item(i):
        return "x%s" % i

    def test_one_item_miss(self):
        cache = FIFO(max_cache_size=1)

        cache.add("item_id1", "item1")
        cache.add("item_id2", "item2")

        with pytest.raises(CacheMissError):
            cache.get("item_id1")

    def test_two_items(self):
        cache = FIFO(max_cache_size=2)

        cache.add("item_id1", "item1")
        cache.add("item_id2", "item2")
        cache.add("item_id3", "item3")

        with pytest.raises(CacheMissError):
            cache.get("item_id1")

    def test_multiple_items(self):
        item_count = 16
        cache = FIFO(max_cache_size=item_count)

        for item_id in range(item_count):
            cache.add(item_id, self._item_id2item(item_id))

        cache.add(item_count, self._item_id2item(item_count))

        with pytest.raises(CacheMissError):
            # first out
            cache.get(0)

    def test_multiple_items2(self):
        item_count = 16
        cache = FIFO(max_cache_size=item_count)

        for item_id in range(item_count):
            cache.add(item_id, self._item_id2item(item_id))

        for item_id in range(item_count - 1, -1, -1):
            assert cache.get(item_id) == self._item_id2item(item_id)

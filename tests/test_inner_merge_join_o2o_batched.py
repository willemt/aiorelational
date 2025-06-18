from itertools import chain

from aiorelational import inner_merge_join_o2o_batched
from aiorelational import list as gen2list

import pytest

from .conftest import cmp_func
from .conftest import lists_of_numbers
from .conftest import lists_of_numbers_hint_aware


merge_functions = [
    inner_merge_join_o2o_batched,
]


@pytest.mark.parametrize("merge_func", merge_functions)
@pytest.mark.asyncio_cooperative
async def test_sorted(merge_func):
    x = lists_of_numbers(0, 10, 1)
    y = lists_of_numbers(0, 10, 2)
    it = merge_func(cmp_func, x, y)
    results = await gen2list(it)
    results = list(chain(*results))
    assert results == [(0, 0), (2, 2), (4, 4), (6, 6), (8, 8)]


@pytest.mark.parametrize("merge_func", merge_functions)
@pytest.mark.asyncio_cooperative
async def test_more_in_right(merge_func):
    x = lists_of_numbers(0, 10, 1)
    y = lists_of_numbers(0, 20, 2)
    it = merge_func(cmp_func, x, y)
    results = await gen2list(it)
    results = list(chain(*results))
    assert results == [(0, 0), (2, 2), (4, 4), (6, 6), (8, 8)]


@pytest.mark.parametrize("merge_func", merge_functions)
@pytest.mark.asyncio_cooperative
async def test_more_in_left(merge_func):
    x = lists_of_numbers(0, 10, 1)
    y = lists_of_numbers(0, 20, 2)
    it = merge_func(cmp_func, x, y)
    results = await gen2list(it)
    results = list(chain(*results))
    assert results == [(0, 0), (2, 2), (4, 4), (6, 6), (8, 8)]


@pytest.mark.parametrize("merge_func", merge_functions)
@pytest.mark.asyncio_cooperative
async def test_different_start(merge_func):
    x = lists_of_numbers(0, 10, 1)
    y = lists_of_numbers(4, 10, 2)
    it = merge_func(cmp_func, x, y)
    results = await gen2list(it)
    results = list(chain(*results))
    assert results == [(4, 4), (6, 6), (8, 8)]


@pytest.mark.parametrize("merge_func", merge_functions)
@pytest.mark.asyncio_cooperative
async def test_sorted_hint_aware(merge_func):
    x = lists_of_numbers_hint_aware(0, 50, 2)
    y = lists_of_numbers_hint_aware(4, 60, 3)
    it = merge_func(cmp_func, x, y)
    results = await gen2list(it)
    results = list(chain(*results))
    assert results == [
        (4, 4),
        (10, 10),
        (16, 16),
        (22, 22),
        (28, 28),
        (34, 34),
        (40, 40),
        (46, 46),
    ]

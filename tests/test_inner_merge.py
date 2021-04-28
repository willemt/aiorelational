from aiorelational import (
    inner_merge_join_m2m,
    inner_merge_join_m2m_naive,
    inner_merge_join_o2m,
    inner_merge_join_o2o,
    list as gen2list,
)

from hypothesis import given

import pytest

from .conftest import cmp_func, list2asyncgen, numbers, unique_monotonic


merge_functions = [
    inner_merge_join_m2m,
    inner_merge_join_m2m_naive,
    inner_merge_join_o2m,
    inner_merge_join_o2o,
]


@pytest.mark.parametrize("merge_func", merge_functions)
@pytest.mark.asyncio_cooperative
async def test_sorted(merge_func):
    x = numbers(0, 10, 1)
    y = numbers(0, 10, 2)
    it = merge_func(cmp_func, x, y)
    results = await gen2list(it)
    assert results == [(0, 0), (2, 2), (4, 4), (6, 6), (8, 8)]


@pytest.mark.parametrize("merge_func", merge_functions)
@pytest.mark.asyncio_cooperative
async def test_more_in_right(merge_func):
    x = numbers(0, 10, 1)
    y = numbers(0, 20, 2)
    it = merge_func(cmp_func, x, y)
    results = await gen2list(it)
    assert results == [(0, 0), (2, 2), (4, 4), (6, 6), (8, 8)]


@pytest.mark.parametrize("merge_func", merge_functions)
@pytest.mark.asyncio_cooperative
async def test_more_in_left(merge_func):
    x = numbers(0, 10, 1)
    y = numbers(0, 20, 2)
    it = merge_func(cmp_func, x, y)
    results = await gen2list(it)
    assert results == [(0, 0), (2, 2), (4, 4), (6, 6), (8, 8)]


@pytest.mark.parametrize("merge_func", merge_functions)
@pytest.mark.asyncio_cooperative
async def test_different_start(merge_func):
    x = numbers(0, 10, 1)
    y = numbers(4, 10, 2)
    it = merge_func(cmp_func, x, y)
    results = await gen2list(it)
    assert results == [(4, 4), (6, 6), (8, 8)]


@given(unique_monotonic, unique_monotonic)
@pytest.mark.asyncio_cooperative
async def test_same_results_as_naive(x, y):
    it = inner_merge_join_m2m(cmp_func, list2asyncgen(x), list2asyncgen(y))
    it_expected = inner_merge_join_m2m_naive(
        cmp_func, list2asyncgen(x), list2asyncgen(y)
    )
    results = await gen2list(it)
    expected = await gen2list(it_expected)
    assert results == expected


@given(unique_monotonic, unique_monotonic)
@pytest.mark.asyncio_cooperative
async def test_property_tests(x, y):
    it = inner_merge_join_m2m(cmp_func, list2asyncgen(x), list2asyncgen(y))
    results = await gen2list(it)
    assert results == sorted(results)
    assert len(results) <= len(x)
    assert len(results) <= len(y)
    assert results == sorted(set(results))

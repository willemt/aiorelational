from aiorelational import full_outer_union, list as gen2list

from hypothesis import given

import pytest

from .conftest import cmp_func, list2asyncgen, unique_monotonic


@given(unique_monotonic, unique_monotonic)
@pytest.mark.asyncio_cooperative
async def test_property_tests(x, y):
    it = full_outer_union(cmp_func, list2asyncgen(x), list2asyncgen(y))
    results = await gen2list(it)
    assert len(results) == len(x) + len(y)
    assert results == sorted(results)

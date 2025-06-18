from itertools import chain

from aiorelational import from_batches
from aiorelational import full_outer_union_distinct_batched
from aiorelational import list as gen2list

from hypothesis import given

import pytest

from .conftest import cmp_func
from .conftest import list2asyncgen
from .conftest import unique_monotonic_sublists


@given(unique_monotonic_sublists(), unique_monotonic_sublists())
@pytest.mark.asyncio_cooperative
async def test_property_tests(x, y):
    it = full_outer_union_distinct_batched(cmp_func, list2asyncgen(x), list2asyncgen(y))
    results = await gen2list(from_batches(it))
    _x = list(chain(*x))
    _y = list(chain(*y))
    assert len(results) == len(set(_x + _y))
    assert results == sorted(results)

from aiorelational import filter, limited, list

import pytest

from .conftest import items


@pytest.mark.asyncio_cooperative
async def test_filter_with_none():
    it = filter(None, limited(5, items(0, 10, None)))
    results = await list(it)
    assert results == [10]


@pytest.mark.asyncio_cooperative
async def test_filter_with_function():
    it = filter(lambda x: x == 2, limited(5, items(0, 2, 10, 2, None)))
    results = await list(it)
    assert results == [2, 2]

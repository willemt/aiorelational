from aiorelational import enumerate, filter, limited, list

import pytest

from .conftest import items


@pytest.mark.asyncio_cooperative
async def test_enumerate_with_none():
    it = enumerate(limited(5, items(0, 10, None)))
    results = await list(it)
    assert results == [(0, 0), (1, 10), (2, None)]


@pytest.mark.asyncio_cooperative
async def test_enumerate_with_filter():
    it = enumerate(filter(lambda x: x == 2, limited(5, items(0, 2, 10, 2, None))))
    results = await list(it)
    assert results == [(0, 2), (1, 2)]

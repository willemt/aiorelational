from aiorelational import groupby, limited, list

import pytest

from .conftest import items


@pytest.mark.asyncio_cooperative
async def test_groupby():
    results = await list(
        groupby(limited(5, items((0, 0), (0, 2), (1, 10), (1, 2))), lambda x: x[0])
    )
    assert results == [(0, [(0, 0), (0, 2)]), (1, [(1, 10), (1, 2)])]


@pytest.mark.asyncio_cooperative
async def test_groupby_with_none():
    results = await list(
        groupby(limited(5, items((0, 0), (None, 2), (1, 10), (1, 2))), lambda x: x[0])
    )
    assert results == [(0, [(0, 0)]), (None, [(None, 2)]), (1, [(1, 10), (1, 2)])]

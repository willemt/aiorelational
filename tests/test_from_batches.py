from aiorelational import from_batches
from aiorelational import list

import pytest

from .conftest import lists_of_items


@pytest.mark.asyncio_cooperative
async def test_from_batches():
    items = [0, 1, 2, 3, 4, 5, 10, None]
    it = from_batches(lists_of_items(*items))
    assert await list(it) == items

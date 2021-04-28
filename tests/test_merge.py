from aiorelational import limited, list

import pytest

from .conftest import numbers


@pytest.mark.asyncio_cooperative
async def test_limited_takes_n():
    it = limited(5, numbers(0, 10, 1))
    results = await list(it)
    assert results == [0,1,2,3,4]

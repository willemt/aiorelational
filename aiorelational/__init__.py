import asyncio
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

T = TypeVar("T")
U = TypeVar("U")


async def get_next(it: AsyncGenerator[T, None], resevoir=[]) -> Optional[T]:
    if resevoir:
        return resevoir.pop(0)
    try:
        x = await it.__anext__()
        return x
    except StopAsyncIteration:
        return None


ComparisonFunc = Callable[[T, T], Awaitable[int]]

HashFunc = Callable[[T], Awaitable[U]]


async def merge_join(
    cmp: ComparisonFunc[T],
    left: AsyncGenerator[T, None],
    right: AsyncGenerator[T, None],
) -> AsyncGenerator[T, None]:
    """
    Merge 2 streams (with the same columns) into one
    The join uses the document ID
    Input must be sorted for the join to work.
    Output is sorted
    """
    pass


async def inner_hash_join_o2o(
    hash: HashFunc[T, U], left: AsyncGenerator[T, None], right: AsyncGenerator[T, None]
) -> AsyncGenerator[T, None]:
    """
    Join 2 streams (with different columns) into one.
    One to one join.
    Used for Table Joins.
    """
    ids = set()
    async for b in right:
        ids.add(await hash(b))
    async for a in left:
        if await hash(a) in ids:
            yield a


async def inner_merge_join_o2o(
    cmp: ComparisonFunc[T],
    left: AsyncGenerator[T, None],
    right: AsyncGenerator[T, None],
) -> AsyncGenerator[Tuple[T, T], None]:
    """
    One to one join.
    """
    a, b = await asyncio.gather(get_next(left), get_next(right))

    while a is not None and b is not None:
        ret: int = await cmp(a, b)
        if ret == -1:
            a = await get_next(left)
        elif ret == 1:
            b = await get_next(right)
        elif ret == 0:
            yield a, b
            a, b = await asyncio.gather(get_next(left), get_next(right))
        else:
            raise Exception

    await left.aclose()
    await right.aclose()


async def inner_merge_join_o2m(
    cmp: ComparisonFunc[T],
    left: AsyncGenerator[T, None],
    right: AsyncGenerator[T, None],
) -> AsyncGenerator[Tuple[T, T], None]:
    """
    One to many join.
    """
    a, b = await asyncio.gather(get_next(left), get_next(right))

    right_reservoir: List[T] = []

    while a is not None and b is not None:
        ret = await cmp(a, b)
        if ret == -1:
            a = await get_next(left)
        elif ret == 1:
            b = await get_next(right, right_reservoir)
        elif ret == 0:
            yield a, b

            b = await get_next(right, right_reservoir)
            while b:
                if await cmp(a, b) == 0:
                    yield a, b
                    b = await get_next(right, right_reservoir)
                else:
                    right_reservoir.append(b)
                    break

            a, b = await asyncio.gather(get_next(left), get_next(right, right_reservoir))
        else:
            raise Exception

    await left.aclose()
    await right.aclose()


async def inner_merge_join_m2m(
    cmp: ComparisonFunc[T],
    left: AsyncGenerator[T, None],
    right: AsyncGenerator[T, None],
) -> AsyncGenerator[Tuple[T, T], None]:
    """
    Join 2 streams (with different columns) into one.
    Many to many.
    Used for Table Joins.
    Assumes input is sorted.

    See: http://www.dcs.ed.ac.uk/home/tz/phd/thesis/node20.htm
    """
    a, b = await asyncio.gather(get_next(left), get_next(right))

    left_reservoir: List[T] = []
    right_reservoir: List[T] = []

    while a is not None and b is not None:
        if a is None:
            a = await get_next(left, left_reservoir)
            continue

        if b is None:
            b = await get_next(right, right_reservoir)
            continue

        ret = await cmp(a, b)

        if ret == -1:
            a = await get_next(left, left_reservoir)
        elif ret == 1:
            b = await get_next(right, right_reservoir)
        elif ret == 0:
            yield a, b

            first_b = b

            # match all the rights
            bs = [b]
            b = await get_next(right, right_reservoir)
            while b is not None:
                if await cmp(a, b) == 0:
                    yield a, b
                    bs.append(b)
                    b = await get_next(right, right_reservoir)
                else:
                    break

            # match all the lefts
            a = await get_next(left, left_reservoir)
            while a is not None:
                if not await cmp(a, first_b) == 0:
                    break
                for b_ in bs:
                    yield a, b_
                a = await get_next(left, left_reservoir)

        else:
            raise Exception

    await left.aclose()
    await right.aclose()


async def inner_merge_join_m2m_naive(
    cmp: ComparisonFunc[T],
    left: AsyncGenerator[T, None],
    right: AsyncGenerator[T, None],
) -> AsyncGenerator[Tuple[T, T], None]:
    """
    Expensive but correct
    """
    b_items = await list(right)
    async for a in left:
        for b in b_items:
            if await cmp(a, b) == 0:
                yield a, b


async def outer_merge_join_o2o(
    cmp: ComparisonFunc[T],
    left: AsyncGenerator[T, None],
    right: AsyncGenerator[T, None],
) -> AsyncGenerator[Tuple[T, Optional[T]], None]:
    """
    One to one.
    """
    a, b = await asyncio.gather(get_next(left), get_next(right))

    while a and b:
        ret: int = await cmp(a, b)
        if ret == -1:
            a = await get_next(left)
        elif ret == 1:
            b = await get_next(right)
        elif ret == 0:
            yield a, b
            a, b = await asyncio.gather(get_next(left), get_next(right))
        else:
            raise Exception

    await left.aclose()
    await right.aclose()


async def full_outer_union(
    cmp: ComparisonFunc[T],
    left: AsyncGenerator[T, None],
    right: AsyncGenerator[T, None],
) -> AsyncGenerator[T, None]:
    """
    Yield items from left and right in order
    Assumes input is ordered
    """

    a, b = await asyncio.gather(get_next(left), get_next(right))

    while b is not None and a is not None:
        ret = await cmp(a, b)
        if ret == -1:
            assert a is not None
            yield a
            a = await get_next(left)
        elif ret == 1:
            assert b is not None
            yield b
            b = await get_next(right)
        elif ret == 0:
            assert a is not None and b is not None
            yield a
            yield b
            a, b = await asyncio.gather(get_next(left), get_next(right))
        else:
            raise Exception(ret)

    while b is not None:
        yield b
        b = await get_next(right)

    while a is not None:
        yield a
        a = await get_next(left)

    await left.aclose()
    await right.aclose()


async def limited(limit: int, it: AsyncGenerator[T, None]) -> AsyncGenerator[T, None]:
    i = 0
    async for r in it:
        if i >= limit:
            break
        yield r
        i += 1


async def filter(
    func: Union[Callable[[Any], bool], None], it: AsyncGenerator[T, None]
) -> AsyncGenerator[T, None]:
    """
    Equivalent of python's filter()
    """
    if func:
        async for r in it:
            if func(r):
                yield r
    else:
        async for r in it:
            if r:
                yield r


inner_merge_join = inner_merge_join_m2m


async def list(source: AsyncGenerator[T, None]) -> List[T]:
    items = []
    async for item in source:
        items.append(item)
    return items

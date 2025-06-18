import asyncio
from dataclasses import dataclass
from typing import Any
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import Generic
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union
from typing import cast

X = TypeVar("X")
Y = TypeVar("Y")
U = TypeVar("U")


@dataclass
class Hint(Generic[U]):
    other: List[U]


async def get_next(
    gen: AsyncGenerator[X, U],
) -> Optional[X]:
    try:
        return await gen.__anext__()
    except StopAsyncIteration:
        return None


async def get_next_with_resevoir(
    gen: AsyncGenerator[X, U],
    resevoir: List[X],
) -> Optional[X]:
    if resevoir:
        return resevoir.pop(0)
    try:
        return await gen.__anext__()
    except StopAsyncIteration:
        return None


async def get_next_hinted(
    gen: AsyncGenerator[X, Hint[U]],
    hint: U,
) -> Optional[X]:
    try:
        return await gen.asend(Hint[U]([hint]))
    except StopAsyncIteration:
        return None


async def get_next_batch(gen: AsyncGenerator[List[X], U]) -> Optional[List[X]]:
    try:
        return await gen.__anext__()
    except StopAsyncIteration:
        return None


async def get_next_batch_hinted(
    gen: AsyncGenerator[List[X], Hint[U]], hint: List[U]
) -> Optional[List[X]]:
    try:
        return await gen.asend(Hint[U](hint))
    except StopAsyncIteration:
        return None


ComparisonFunc = Callable[[X, Y], Awaitable[int]]

HashFunc = Callable[[X], Awaitable[U]]


# async def merge_join(
#     cmp: ComparisonFunc[X, Y],
#     left: AsyncGenerator[X, None],
#     right: AsyncGenerator[Y, None],
# ) -> AsyncGenerator[X, None]:
#     """
#     Merge 2 streams (with the same columns) into one
#     The join uses the document ID
#     Input must be sorted for the join to work.
#     Output is sorted
#     """
#     pass


async def inner_hash_join_o2o(
    hash: HashFunc[Union[X, Y], U],
    left: AsyncGenerator[X, None],
    right: AsyncGenerator[Y, None],
) -> AsyncGenerator[X, None]:
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
    cmp: ComparisonFunc[X, Y],
    left: AsyncGenerator[X, Hint[Y]],
    right: AsyncGenerator[Y, Hint[X]],
) -> AsyncGenerator[Tuple[X, Y], None]:
    """
    One to one join.
    """
    a, b = await asyncio.gather(get_next(left), get_next(right))

    while a is not None and b is not None:
        ret: int = await cmp(a, b)
        if ret == -1:
            a = await get_next_hinted(left, hint=b)
        elif ret == 1:
            b = await get_next_hinted(right, hint=a)
        elif ret == 0:
            yield a, b
            a, b = await asyncio.gather(get_next(left), get_next(right))
        else:
            raise Exception

    await left.aclose()
    await right.aclose()


async def left_inner_merge_join_o2o(
    cmp: ComparisonFunc[X, Y],
    left: AsyncGenerator[X, Hint[Y]],
    right: AsyncGenerator[Y, Hint[X]],
) -> AsyncGenerator[X, None]:
    """
    One to one join.
    """
    a, b = await asyncio.gather(get_next(left), get_next(right))

    while a is not None and b is not None:
        ret: int = await cmp(a, b)
        if ret == -1:
            a = await get_next_hinted(left, hint=b)
        elif ret == 1:
            b = await get_next_hinted(right, hint=a)
        elif ret == 0:
            yield a
            a, b = await asyncio.gather(get_next(left), get_next(right))
        else:
            raise Exception

    await left.aclose()
    await right.aclose()


async def inner_merge_join_o2o_batched(
    cmp: ComparisonFunc[X, Y],
    left: AsyncGenerator[List[X], Hint[Y]],
    right: AsyncGenerator[List[Y], Hint[X]],
) -> AsyncGenerator[List[Tuple[X, Y]], U]:
    """
    One to one join optimized for sorted batches without duplicates.
    """
    a_batch, b_batch = await asyncio.gather(
        get_next_batch(left),
        get_next_batch(right),
    )
    a_index, b_index = 0, 0

    while a_batch is not None and b_batch is not None:
        result_batch = []
        while a_index < len(a_batch) and b_index < len(b_batch):
            a = a_batch[a_index]
            b = b_batch[b_index]
            ret = await cmp(a, b)
            if ret == 0:
                result_batch.append((a, b))
                a_index += 1
                b_index += 1
            elif ret < 0:
                a_index += 1
            else:
                b_index += 1

        if result_batch:
            yield result_batch

        if a_index >= len(a_batch):
            if b_index < len(b_batch):
                a_batch = await get_next_batch_hinted(left, hint=b_batch[b_index:])  # type: ignore
            else:
                a_batch = await get_next_batch(left)
            a_index = 0

        if b_index >= len(b_batch):
            if a_batch and a_index < len(a_batch):
                b_batch = await get_next_batch_hinted(right, hint=a_batch[a_index:])  # type: ignore
            else:
                b_batch = await get_next_batch(right)
            b_index = 0

    await left.aclose()
    await right.aclose()


async def inner_merge_join_o2m(
    cmp: ComparisonFunc[X, Y],
    left: AsyncGenerator[X, Hint[U]],
    right: AsyncGenerator[Y, Hint[U]],
) -> AsyncGenerator[Tuple[X, Y], Hint[U]]:
    """
    One to many join.
    """
    a, b = await asyncio.gather(get_next(left), get_next(right))

    right_reservoir: List[Optional[Y]] = []

    while a is not None and b is not None:
        ret = await cmp(a, b)
        if ret == -1:
            a = await get_next(left)
        elif ret == 1:
            b = await get_next_with_resevoir(right, right_reservoir)
        elif ret == 0:
            yield a, b

            b = await get_next_with_resevoir(right, right_reservoir)
            while b:
                if await cmp(a, b) == 0:
                    yield a, b
                    b = await get_next_with_resevoir(right, right_reservoir)
                else:
                    assert b is not None
                    right_reservoir.append(b)
                    break

            a, b = await asyncio.gather(
                get_next(left), get_next_with_resevoir(right, right_reservoir)
            )
        else:
            raise Exception

    await left.aclose()
    await right.aclose()


async def inner_merge_join_m2m(
    cmp: ComparisonFunc[X, Y],
    left: AsyncGenerator[X, None],
    right: AsyncGenerator[Y, None],
) -> AsyncGenerator[Tuple[X, Y], None]:
    """
    Join 2 streams (with different columns) into one.
    Many to many.
    Used for Table Joins.
    Assumes input is sorted.

    See: http://www.dcs.ed.ac.uk/home/tz/phd/thesis/node20.htm
    """
    a, b = await asyncio.gather(get_next(left), get_next(right))

    left_reservoir: List[Optional[X]] = []
    right_reservoir: List[Optional[Y]] = []

    while a is not None and b is not None:
        if a is None:
            a = await get_next(left, resevoir=left_reservoir)
            continue

        if b is None:
            b = await get_next(right, resevoir=right_reservoir)
            continue

        ret = await cmp(a, b)

        if ret == -1:
            a = await get_next_with_resevoir(left, resevoir=left_reservoir)
        elif ret == 1:
            b = await get_next_with_resevoir(right, resevoir=right_reservoir)
        elif ret == 0:
            yield a, b

            first_b = b

            # match all the rights
            bs = [b]
            b = await get_next_with_resevoir(right, resevoir=right_reservoir)
            while b is not None:
                if await cmp(a, b) == 0:
                    yield a, b
                    bs.append(b)
                    b = await get_next_with_resevoir(right, resevoir=right_reservoir)
                else:
                    break

            # match all the lefts
            a = await get_next_with_resevoir(left, resevoir=left_reservoir)
            while a is not None:
                if not await cmp(a, first_b) == 0:
                    break
                for b_ in bs:
                    yield a, b_
                a = await get_next_with_resevoir(left, resevoir=left_reservoir)

        else:
            raise Exception

    await left.aclose()
    await right.aclose()


async def inner_merge_join_m2m_naive(
    cmp: ComparisonFunc[X, Y],
    left: AsyncGenerator[X, None],
    right: AsyncGenerator[Y, None],
) -> AsyncGenerator[Tuple[X, Y], None]:
    """
    Expensive but correct
    """
    b_items = await list(right)
    async for a in left:
        for b in b_items:
            if await cmp(a, b) == 0:
                yield a, b


async def outer_merge_join_o2o(
    cmp: ComparisonFunc[X, Y],
    left: AsyncGenerator[X, None],
    right: AsyncGenerator[Y, None],
) -> AsyncGenerator[Tuple[X, Optional[Y]], None]:
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
    cmp: ComparisonFunc[X, Y],
    left: AsyncGenerator[X, None],
    right: AsyncGenerator[Y, None],
) -> AsyncGenerator[Union[X, Y], None]:
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


async def full_outer_union_distinct(
    cmp: ComparisonFunc[X, Y],
    left: AsyncGenerator[X, None],
    right: AsyncGenerator[Y, None],
) -> AsyncGenerator[Union[X, Y], None]:
    """
    Yield items from left and right in order
    Yield items found in both only once
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


async def full_outer_union_distinct_batched(
    cmp: ComparisonFunc[X, Y],
    left: AsyncGenerator[List[X], Hint[Y]],
    right: AsyncGenerator[List[Y], Hint[X]],
) -> AsyncGenerator[List[Union[X, Y]], U]:
    """
    Yield items from left and right in order
    Yield items found in both only once
    Assumes input is ordered
    """
    a_batch = await get_next_batch(left)
    b_batch = await get_next_batch(right)
    a_index, b_index = 0, 0

    while a_batch is not None and b_batch is not None:
        result_batch: List[Union[X, Y]] = []
        while a_index < len(a_batch) and b_index < len(b_batch):
            a = a_batch[a_index]
            b = b_batch[b_index]
            ret = await cmp(a, b)
            if ret == -1:
                result_batch.append(a)
                a_index += 1
            elif ret == 1:
                result_batch.append(b)
                b_index += 1
            elif ret == 0:
                result_batch.append(a)
                a_index += 1
                b_index += 1
            else:
                raise Exception(ret)

        if result_batch:
            yield result_batch

        if a_index >= len(a_batch):
            if b_index < len(b_batch):
                a_batch = await get_next_batch_hinted(left, hint=b_batch[b_index:])  # type: ignore
            else:
                a_batch = await get_next_batch(left)
            a_index = 0

        if b_index >= len(b_batch):
            if a_batch and a_index < len(a_batch):
                b_batch = await get_next_batch_hinted(right, hint=a_batch[a_index:])  # type: ignore
            else:
                b_batch = await get_next_batch(right)
            b_index = 0

    while b_batch is not None:
        yield cast(List[Union[X, Y]], b_batch[b_index:])
        b_batch = await get_next_batch(right)
        b_index = 0

    while a_batch is not None:
        yield cast(List[Union[X, Y]], a_batch[a_index:])
        a_batch = await get_next_batch(left)
        a_index = 0

    await left.aclose()
    await right.aclose()


async def take(limit: int, it: AsyncGenerator[X, None]) -> AsyncGenerator[X, None]:
    i = 0
    async for r in it:
        if i >= limit:
            break
        yield r
        i += 1


limited = take


async def filter(
    func: Union[Callable[[Any], bool], None], it: AsyncGenerator[X, None]
) -> AsyncGenerator[X, None]:
    """
    Equivalent of Python's filter()
    """
    if func:
        async for r in it:
            if func(r):
                yield r
    else:
        async for r in it:
            if r:
                yield r


async def groupby(
    it: AsyncGenerator[X, None], key: Callable[[Any], Any]
) -> AsyncGenerator[Tuple[Any, Iterable[X]], None]:
    """
    Equivalent of Python's itertools.groupby()
    """

    r = await it.__anext__()
    current_bucket = [r]
    current_key = key(r)

    try:
        async for r in it:
            item_key = key(r)
            if current_key != item_key:
                yield current_key, current_bucket
                current_bucket = []
                current_key = item_key
            current_bucket.append(r)
    finally:
        yield current_key, current_bucket


inner_merge_join = inner_merge_join_m2m


async def list(source: Union[AsyncGenerator[X, None], AsyncIterator[X]]) -> List[X]:
    """Convert async generator into a list
    :param source: The async generator to convert."""
    items = []
    async for item in source:
        items.append(item)
    return items


async def count(source: AsyncGenerator[X, None]) -> int:
    count = 0
    async for item in source:
        count += 1
    return count


async def enumerate(
    source: AsyncGenerator[X, None]
) -> AsyncGenerator[Tuple[int, X], None]:
    count = 0
    async for item in source:
        yield count, item
        count += 1


async def from_batches(source: AsyncGenerator[List[X], U]) -> AsyncGenerator[X, U]:
    async for batch in source:
        for item in batch:
            yield item


async def to_batches(
    source: AsyncGenerator[X, U], batch_size: Optional[int] = None
) -> AsyncGenerator[List[X], U]:
    batch_size = batch_size or 25
    chunk = []
    async for item in source:
        chunk.append(item)
        if len(chunk) >= batch_size:
            yield chunk
            chunk = []

    if chunk:
        yield chunk

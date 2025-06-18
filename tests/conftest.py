import random
from typing import Any
from typing import AsyncGenerator
from typing import List

from hypothesis.strategies import booleans
from hypothesis.strategies import composite
from hypothesis.strategies import integers
from hypothesis.strategies import lists


async def random_unique_numbers(a, b, c):
    numbers = list(range(a, b, c))
    random.shuffle(numbers)
    for i in numbers:
        yield i


async def numbers(a, b, c):
    for i in range(a, b, c):
        yield i


async def lists_of_numbers(a, b, c) -> AsyncGenerator[List[int], None]:
    batch = []
    for i in range(a, b, c):
        batch.append(i)
        if len(batch) == random.randint(1, 5):
            yield batch
            batch = []
    if batch:
        yield batch


async def lists_of_numbers_hint_aware(a, b, c) -> AsyncGenerator[List[int], None]:
    hint = None
    batch = []
    for i in range(a, b, c):
        if hint is not None:
            if i < hint.other[0]:
                continue
        batch.append(i)
        if len(batch) == random.randint(1, 8):
            hint = yield batch
            batch = []
    if batch:
        yield batch


async def items(*a):
    for i in a:
        yield i


async def lists_of_items(*a) -> AsyncGenerator[List[Any], None]:
    batch = []
    for i in a:
        batch.append(i)
        if len(batch) == random.randint(1, 5):
            yield batch
            batch = []
    if batch:
        yield batch


async def cmp_func(a, b):
    if a < b:
        return -1
    elif a > b:
        return 1
    return 0


unique_monotonic = lists(integers(min_value=0, max_value=20)).map(set).map(sorted)


@composite
def unique_monotonic_sublists(draw):
    base = draw(unique_monotonic)
    if not base:
        return []

    boundary_bools = draw(
        lists(booleans(), min_size=len(base) - 1, max_size=len(base) - 1)
    )

    sublists = []
    current = [base[0]]

    for x, should_split in zip(base[1:], boundary_bools):
        if should_split:
            sublists.append(current)
            current = [x]
        else:
            current.append(x)

    sublists.append(current)

    return sublists


async def list2asyncgen(x):
    for i in x:
        yield i

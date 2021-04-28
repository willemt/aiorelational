import random

from hypothesis.strategies import integers, lists

import pytest

from aiorelational import limited, list as gen2list


async def random_unique_numbers(a, b, c):
    numbers = list(range(a, b, c))
    random.shuffle(numbers)
    for i in numbers:
        yield i


async def numbers(a, b, c):
    for i in range(a, b, c):
        yield i


async def cmp_func(a, b):
    if a < b:
        return -1
    elif a > b:
        return 1
    return 0


unique_monotonic = lists(integers(min_value=0, max_value=20)).map(set).map(sorted)


async def list2asyncgen(x):
    for i in x:
        yield i

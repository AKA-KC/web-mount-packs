#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 5)
__all__ = [
    "SupportsLT", "grouped_mapping", "grouped_mapping_async", 
    "uniq", "uniq_async", "dups", "dups_async", "iter_keyed_dups", 
    "iter_keyed_dups_async", "iter_dups", "iter_dups_async", 
]

from collections.abc import (
    AsyncIterable, AsyncIterator, Coroutine, Iterable, Iterator, 
    MutableMapping, 
)
from operator import itemgetter
from typing import cast, overload, runtime_checkable, Any, Callable, Protocol 


@runtime_checkable
class SupportsLT(Protocol):
    def __lt__(self, o, /) -> bool:
        ...


@overload
def grouped_mapping[K, V](
    it: Iterable[V], 
    /, 
    key: Callable[[V], K], 
    *, 
    mapping: None = None, 
) -> dict[K, list[V]]:
    ...
@overload
def grouped_mapping[K, V](
    it: Iterable[V], 
    /, 
    key: Callable[[V], K], 
    *, 
    mapping: MutableMapping[K, list[V]], 
) -> MutableMapping[K, list[V]]:
    ...
@overload
def grouped_mapping[K, V](
    it: Iterable[tuple[K, V]], 
    /, 
    key: None = None, 
    *, 
    mapping: None = None, 
) -> dict[K, list[V]]:
    ...
@overload
def grouped_mapping[K, V](
    it: Iterable[tuple[K, V]], 
    /, 
    key: None = None, 
    *, 
    mapping: MutableMapping[K, list[V]], 
) -> MutableMapping[K, list[V]]:
    ...
@overload
def grouped_mapping[K, V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], K], 
    *, 
    mapping: None = None, 
) -> Coroutine[Any, Any, dict[K, list[V]]]:
    ...
@overload
def grouped_mapping[K, V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], K], 
    *, 
    mapping: MutableMapping[K, list[V]], 
) -> Coroutine[Any, Any, MutableMapping[K, list[V]]]:
    ...
@overload
def grouped_mapping[K, V](
    it: AsyncIterable[tuple[K, V]], 
    /, 
    key: None = None, 
    *, 
    mapping: None = None, 
) -> Coroutine[Any, Any, dict[K, list[V]]]:
    ...
@overload
def grouped_mapping[K, V](
    it: AsyncIterable[tuple[K, V]], 
    /, 
    key: None = None, 
    *, 
    mapping: MutableMapping[K, list[V]], 
) -> Coroutine[Any, Any, MutableMapping[K, list[V]]]:
    ...
def grouped_mapping[K, V](
    it: Iterable[V] | Iterable[tuple[K, V]] | AsyncIterable[V] | AsyncIterable[tuple[K, V]], 
    /, 
    key: None | Callable[[V], K] = None, 
    *, 
    mapping: None | MutableMapping[K, list[V]] = None, 
) -> dict[K, list[V]] | MutableMapping[K, list[V]] | Coroutine[Any, Any, dict[K, list[V]]] | Coroutine[Any, Any, MutableMapping[K, list[V]]]:
    """Groups elements from an iterable into a mapping by a specified key or directly from tuples.

    This function supports two modes:
      1. When `it` is an iterable of values and a key function is provided, 
         it groups the values based on the keys generated by the function.
      2. When `it` is an iterable of tuples, it directly uses the first element of each tuple as the key.

    :param it: An iterable of values (V) or an iterable of tuples (K, V).
    :param key: An optional callable that extracts a key from each value.
    :param mapping: An optional mutable mapping to store the results. If None, a new `dict` will be created.

    :return: A mapping where each key corresponds to a list of values associated with that key.
    """
    if not isinstance(it, Iterable):
        return grouped_mapping_async(it, key=key, mapping=mapping) # type: ignore
    if mapping is None:
        mapping = {}
    append = list.append
    if key is not None:
        it = cast(Iterable[V], it)
        it = ((key(v), v) for v in it)
    it = cast(Iterable[tuple[K, V]], it)
    for k, v in it:
        try:
            append(mapping[k], v)
        except KeyError:
            mapping[k] = [v]
    return mapping


@overload
async def grouped_mapping_async[K, V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], K], 
    *, 
    mapping: None = None, 
) -> dict[K, list[V]]:
    ...
@overload
async def grouped_mapping_async[K, V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], K], 
    *, 
    mapping: MutableMapping[K, list[V]], 
) -> MutableMapping[K, list[V]]:
    ...
@overload
async def grouped_mapping_async[K, V](
    it: AsyncIterable[tuple[K, V]], 
    /, 
    key: None = None, 
    *, 
    mapping: None = None, 
) -> dict[K, list[V]]:
    ...
@overload
async def grouped_mapping_async[K, V](
    it: AsyncIterable[tuple[K, V]], 
    /, 
    key: None = None, 
    *, 
    mapping: MutableMapping[K, list[V]], 
) -> MutableMapping[K, list[V]]:
    ...
async def grouped_mapping_async[K, V](
    it: AsyncIterable[V] | AsyncIterable[tuple[K, V]], 
    /, 
    key: None | Callable[[V], K] = None, 
    *, 
    mapping: None | MutableMapping[K, list[V]] = None, 
) -> MutableMapping[K, list[V]]:
    """Groups elements from an async iterable into a mapping by a specified key or directly from tuples.

    This function supports two modes:
      1. When `it` is an async iterable of values and a key function is provided, 
         it groups the values based on the keys generated by the function.
      2. When `it` is an async iterable of tuples, it directly uses the first element of each tuple as the key.

    :param it: An async iterable of values (V) or an async iterable of tuples (K, V).
    :param key: An optional callable that extracts a key from each value.
    :param mapping: An optional mutable mapping to store the results. If None, a new `dict` will be created.

    :return: A mapping where each key corresponds to a list of values associated with that key.
    """
    if mapping is None:
        mapping = {}
    append = list.append
    if key is not None:
        it = cast(AsyncIterable[V], it)
        it = ((key(v), v) async for v in it)
    it = cast(AsyncIterable[tuple[K, V]], it)
    async for k, v in it:
        try:
            append(mapping[k], v)
        except KeyError:
            mapping[k] = [v]
    return mapping


@overload
def uniq[K, V](
    it: Iterable[V], 
    /, 
    key: Callable[[V], K], 
    keep_first: bool | Callable[[V], SupportsLT] = True, 
) -> dict[K, V]:
    ...
@overload
def uniq[K, V](
    it: Iterable[tuple[K, V]], 
    /, 
    key: None = None, 
    keep_first: bool | Callable[[V], SupportsLT] = True, 
) -> dict[K, V]:
    ...
@overload
def uniq[K, V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], K], 
    keep_first: bool | Callable[[V], SupportsLT] = True, 
) -> Coroutine[Any, Any, dict[K, V]]:
    ...
@overload
def uniq[K, V](
    it: AsyncIterable[tuple[K, V]], 
    /, 
    key: None = None, 
    keep_first: bool | Callable[[V], SupportsLT] = True, 
) -> Coroutine[Any, Any, dict[K, V]]:
    ...
def uniq[K, V](
    it: Iterable[V] | Iterable[tuple[K, V]] | AsyncIterable[V] | AsyncIterable[tuple[K, V]], 
    /, 
    key: None | Callable[[V], K] = None, 
    keep_first: bool | Callable[[V], SupportsLT] = True, 
) -> dict[K, V] | Coroutine[Any, Any, dict[K, V]]:
    """Returns a dictionary of unique items from an iterable.
    
    This function supports two modes:
      1. When `it` is an iterable of values and a key function is provided, 
         it determines uniqueness based on the keys generated by the function.
      2. When `it` is an iterable of tuples, it uses the first element of each tuple as the key.

    :param it: An iterable of values (V) or an iterable of tuples (K, V).
    :param key: An optional callable that extracts a key from each value.
    :param keep_first:
        - If a callable, uses its return value for comparison to determine which item to keep (always keep the first lowest).
        - If True, retains the first occurrence of each unique key.
        - If False, retains the last occurrence.

    :return: A dictionary where stores all the unique values (differentiated by their keys).
    """
    if not isinstance(it, Iterable):
        return uniq_async(it, key=key, keep_first=keep_first) # type: ignore
    if key is not None:
        it = cast(Iterable[V], it)
        it = ((key(v), v) for v in it)
    d: dict[K, V] = {}
    setitem: Callable
    if callable(keep_first):
        cache: dict[K, SupportsLT] = {}
        def setitem(k: K, v: V, /):
            comparand = keep_first(v)
            if k in cache:
                prev_comparand = cache[k]
                if comparand < prev_comparand:
                    d[k] = v
                else:
                    return
            cache[k] = comparand
    elif keep_first:
        setitem = d.setdefault
    else:
        setitem = d.__setitem__
    it = cast(Iterable[tuple[K, V]], it)
    for k, v in it:
        setitem(k, v)
    return d


@overload
async def uniq_async[K, V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], K], 
    keep_first: bool | Callable[[V], SupportsLT] = True, 
) -> dict[K, V]:
    ...
@overload
async def uniq_async[K, V](
    it: AsyncIterable[tuple[K, V]], 
    /, 
    key: None = None, 
    keep_first: bool | Callable[[V], SupportsLT] = True, 
) -> dict[K, V]:
    ...
async def uniq_async[K, V](
    it: AsyncIterable[V] | AsyncIterable[tuple[K, V]], 
    /, 
    key: None | Callable[[V], K] = None, 
    keep_first: bool | Callable[[V], SupportsLT] = True, 
) -> dict[K, V]:
    """Returns a dictionary of unique items from an async iterable.
    
    This function supports two modes:
      1. When `it` is an async iterable of values and a key function is provided, 
         it determines uniqueness based on the keys generated by the function.
      2. When `it` is an async iterable of tuples, it uses the first element of each tuple as the key.

    :param it: An async iterable of values (V) or an async iterable of tuples (K, V).
    :param key: An optional callable that extracts a key from each value.
    :param keep_first:
        - If a callable, uses its return value for comparison to determine which item to keep (always keep the first lowest).
        - If True, retains the first occurrence of each unique key.
        - If False, retains the last occurrence.

    :return: A dictionary where stores all the unique values (differentiated by their keys).
    """
    if key is not None:
        it = cast(AsyncIterable[V], it)
        it = ((key(v), v) async for v in it)
    d: dict[K, V] = {}
    setitem: Callable
    if callable(keep_first):
        cache: dict[K, SupportsLT] = {}
        def setitem(k: K, v: V, /):
            comparand = keep_first(v)
            if k in cache:
                prev_comparand = cache[k]
                if comparand < prev_comparand:
                    d[k] = v
                else:
                    return
            cache[k] = comparand
    elif keep_first:
        setitem = d.setdefault
    else:
        setitem = d.__setitem__
    it = cast(AsyncIterable[tuple[K, V]], it)
    async for k, v in it:
        setitem(k, v)
    return d


@overload
def dups[K, V](
    it: Iterable[V], 
    /, 
    key: Callable[[V], K], 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> dict[K, list[V]]:
    ...
@overload
def dups[K, V](
    it: Iterable[tuple[K, V]], 
    /, 
    key: None = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> dict[K, list[V]]:
    ...
@overload
def dups[K, V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], K], 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> Coroutine[Any, Any, dict[K, list[V]]]:
    ...
@overload
def dups[K, V](
    it: AsyncIterable[tuple[K, V]], 
    /, 
    key: None = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> Coroutine[Any, Any, dict[K, list[V]]]:
    ...
def dups[K, V](
    it: Iterable[V] | Iterable[tuple[K, V]] | AsyncIterable[V] | AsyncIterable[tuple[K, V]], 
    /, 
    key: None | Callable[[V], K] = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> dict[K, list[V]] | Coroutine[Any, Any, dict[K, list[V]]]:
    """Finds duplicates in the given iterable and returns them as a dictionary.

    :param it: An iterable of values (V) or an iterable of tuples (K, V).
    :param key: An optional callable that extracts a key from each value.
    :param keep_first:
        - If None, retains each occurrence if shared a common key.
        - If a callable, uses its return value for comparison to determine which item to keep (always keep the first lowest).
        - If True, retains the first occurrence of each unique key.
        - If False, retains the last occurrence.

    :return: A dictionary where keys are derived from the input values and values are lists of duplicates.
    """
    if not isinstance(it, Iterable):
        return dups_async(it, key=key, keep_first=keep_first) # type: ignore
    return grouped_mapping(iter_keyed_dups(it, key=key, keep_first=keep_first)) # type: ignore


@overload
def dups_async[K, V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], K], 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> Coroutine[Any, Any, dict[K, list[V]]]:
    ...
@overload
def dups_async[K, V](
    it: AsyncIterable[tuple[K, V]], 
    /, 
    key: None = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> Coroutine[Any, Any, dict[K, list[V]]]:
    ...
def dups_async[K, V](
    it: AsyncIterable[V] | AsyncIterable[tuple[K, V]], 
    /, 
    key: None | Callable[[V], K] = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> Coroutine[Any, Any, dict[K, list[V]]]:
    """Finds duplicates in the given async iterable and returns them as a dictionary.

    :param it: An async iterable of values (V) or an async iterable of tuples (K, V).
    :param key: An optional callable that extracts a key from each value.
    :param keep_first:
        - If None, retains each occurrence if shared a common key.
        - If a callable, uses its return value for comparison to determine which item to keep (always keep the first lowest).
        - If True, retains the first occurrence of each unique key.
        - If False, retains the last occurrence.

    :return: A dictionary where keys are derived from the input values and values are lists of duplicates.
    """
    return grouped_mapping_async(iter_keyed_dups_async(it, key=key, keep_first=keep_first)) # type: ignore


@overload
def iter_keyed_dups[K, V](
    it: Iterable[V], 
    /, 
    key: Callable[[V], K], 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> Iterator[tuple[K, V]]:
    ...
@overload
def iter_keyed_dups[K, V](
    it: Iterable[tuple[K, V]], 
    /, 
    key: None = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> Iterator[tuple[K, V]]:
    ...
@overload
def iter_keyed_dups[K, V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], K], 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> AsyncIterator[tuple[K, V]]:
    ...
@overload
def iter_keyed_dups[K, V](
    it: AsyncIterable[tuple[K, V]], 
    /, 
    key: None = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> AsyncIterator[tuple[K, V]]:
    ...
def iter_keyed_dups[K, V](
    it: Iterable[V] | Iterable[tuple[K, V]] | AsyncIterable[V] | AsyncIterable[tuple[K, V]], 
    /, 
    key: None | Callable[[V], K] = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> Iterator[tuple[K, V]] | AsyncIterator[tuple[K, V]]:
    """Yields tuples of key and (duplicate) value pairs.

    :param it: An iterable of values (V) or an iterable of tuples (K, V).
    :param key: An optional callable that extracts a key from each value.
    :param keep_first:
        - If None, retains each occurrence if shared a common key.
        - If a callable, uses its return value for comparison to determine which item to keep (always keep the first lowest).
        - If True, retains the first occurrence of each unique key.
        - If False, retains the last occurrence.

    :yield: Iterator of tuples of keys and their corresponding duplicate values.
    """
    if not isinstance(it, Iterable):
        return iter_keyed_dups_async(it, key=key, keep_first=keep_first) # type: ignore
    def call():
        nonlocal it
        if key is not None:
            it = cast(Iterable[V], it)
            it = ((key(v), v) for v in it)
        it = cast(Iterable[tuple[K, V]], it)
        if keep_first is None:
            d: dict[K, V] = {}
            pop = d.pop
            s: set[K] = set()
            add = s.add
            for k, v in it:
                if k in s:
                    yield k, v
                elif k in d:
                    yield k, pop(k)
                    yield k, v
                    add(k)
                else:
                    d[k] = v
        elif callable(keep_first):
            cache: dict[K, tuple[SupportsLT, V]] = {}
            for k, v in it:
                comparand = keep_first(v)
                if k in cache:
                    prev_comparand, prev_v = cache[k]
                    if comparand < prev_comparand:
                        yield k, prev_v
                    else:
                        yield k, v
                        continue
                cache[k] = (comparand, v)
        elif keep_first:
            s = set()
            add = s.add
            for k, v in it:
                if k in s:
                    yield k, v
                else:
                    add(k)
        else:
            d = {}
            for k, v in it:
                if k in d:
                    yield k, d[k]
                d[k] = v
    return call()


@overload
def iter_keyed_dups_async[K, V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], K], 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> AsyncIterator[tuple[K, V]]:
    ...
@overload
def iter_keyed_dups_async[K, V](
    it: AsyncIterable[tuple[K, V]], 
    /, 
    key: None = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> AsyncIterator[tuple[K, V]]:
    ...
async def iter_keyed_dups_async[K, V](
    it: AsyncIterable[V] | AsyncIterable[tuple[K, V]], 
    /, 
    key: None | Callable[[V], K] = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> AsyncIterator[tuple[K, V]]:
    """Yields tuples of key and (duplicate) value pairs.

    :param it: An async iterable of values (V) or an async iterable of tuples (K, V).
    :param key: An optional callable that extracts a key from each value.
    :param keep_first:
        - If None, retains each occurrence if shared a common key.
        - If a callable, uses its return value for comparison to determine which item to keep (always keep the first lowest).
        - If True, retains the first occurrence of each unique key.
        - If False, retains the last occurrence.

    :yield: Iterator of tuples of keys and their corresponding duplicate values.
    """
    if key is not None:
        it = cast(AsyncIterable[V], it)
        it = ((key(v), v) async for v in it)
    it = cast(AsyncIterable[tuple[K, V]], it)
    if keep_first is None:
        d: dict[K, V] = {}
        pop = d.pop
        s: set[K] = set()
        add = s.add
        async for k, v in it:
            if k in s:
                yield k, v
            elif k in d:
                yield k, pop(k)
                yield k, v
                add(k)
            else:
                d[k] = v
    elif callable(keep_first):
        cache: dict[K, tuple[SupportsLT, V]] = {}
        async for k, v in it:
            comparand = keep_first(v)
            if k in cache:
                prev_comparand, prev_v = cache[k]
                if comparand < prev_comparand:
                    yield k, prev_v
                else:
                    yield k, v
                    continue
            cache[k] = (comparand, v)
    elif keep_first:
        s = set()
        add = s.add
        async for k, v in it:
            if k in s:
                yield k, v
            else:
                add(k)
    else:
        d = {}
        async for k, v in it:
            if k in d:
                yield k, d[k]
            d[k] = v


@overload
def iter_dups[V](
    it: Iterable[V], 
    /, 
    key: Callable[[V], Any], 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> Iterator[V]:
    ...
@overload
def iter_dups[V](
    it: Iterable[tuple[Any, V]], 
    /, 
    key: None = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> Iterator[V]:
    ...
@overload
def iter_dups[V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], Any], 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> AsyncIterator[V]:
    ...
@overload
def iter_dups[V](
    it: AsyncIterable[tuple[Any, V]], 
    /, 
    key: None = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> AsyncIterator[V]:
    ...
def iter_dups[V](
    it: Iterable[V] | Iterable[tuple[Any, V]] | AsyncIterable[V] | AsyncIterable[tuple[Any, V]], 
    /, 
    key: None | Callable[[V], Any] = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> Iterator[V] | AsyncIterator[V]:
    """Yields duplicate values from the provided iterable.
    
    :param it: An iterable of values (V) or an iterable of tuples (K, V).
    :param key: An optional callable that extracts a key from each value.
    :param keep_first:
        - If None, retains each occurrence if shared a common key.
        - If a callable, uses its return value for comparison to determine which item to keep (always keep the first lowest).
        - If True, retains the first occurrence of each unique key.
        - If False, retains the last occurrence.

    :yield: Iterator of duplicate values from the input iterable.
    """
    if not isinstance(it, Iterable):
        return iter_dups_async(it, key=key, keep_first=keep_first) # type: ignore
    return map(
        itemgetter(1), 
        iter_keyed_dups(it, key=key, keep_first=keep_first), # type: ignore
    )


@overload
def iter_dups_async[V](
    it: AsyncIterable[V], 
    /, 
    key: Callable[[V], Any], 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> AsyncIterator[V]:
    ...
@overload
def iter_dups_async[V](
    it: AsyncIterable[tuple[Any, V]], 
    /, 
    key: None = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> AsyncIterator[V]:
    ...
async def iter_dups_async[V](
    it: AsyncIterable[V] | AsyncIterable[tuple[Any, V]], 
    /, 
    key: None | Callable[[V], Any] = None, 
    keep_first: None | bool | Callable[[V], SupportsLT] = None, 
) -> AsyncIterator[V]:
    """Yields duplicate values from the provided async iterable.
    
    :param it: An async iterable of values (V) or an async iterable of tuples (K, V).
    :param key: An optional callable that extracts a key from each value.
    :param keep_first:
        - If None, retains each occurrence if shared a common key.
        - If a callable, uses its return value for comparison to determine which item to keep (always keep the first lowest).
        - If True, retains the first occurrence of each unique key.
        - If False, retains the last occurrence.

    :yield: Iterator of duplicate values from the input async iterable.
    """
    item: V
    async for _, item in iter_keyed_dups_async(it, key=key, keep_first=keep_first): # type: ignore
        yield item


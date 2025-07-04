#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 2, 4)
__all__ = [
    "Yield", "YieldFrom", "iterable", "async_iterable", 
    "run_gen_step", "run_gen_step_iter", "as_gen_step", 
    "as_gen_step_iter", "split_cm, ""with_iter_next", 
    "map", "filter", "reduce", "zip", "chunked", 
    "foreach", "async_foreach", "through", "async_through", 
    "flatten", "async_flatten", "collect", "async_collect", 
    "group_collect", "async_group_collect", "iter_unique", 
    "async_iter_unique", "wrap_iter", "wrap_aiter", "acc_step", 
    "cut_iter", "bfs_gen", "context", "backgroud_loop", 
]

from asyncio import create_task, sleep as async_sleep
from builtins import map as _map, filter as _filter, zip as _zip
from collections import defaultdict, deque
from collections.abc import (
    AsyncIterable, AsyncIterator, Awaitable, Buffer, Callable, 
    Collection, Container, Coroutine, Generator, Iterable, Iterator, 
    Mapping, MutableMapping, MutableSet, MutableSequence, Sequence, 
    ValuesView, 
)
from contextlib import (
    asynccontextmanager, contextmanager, ExitStack, AsyncExitStack, 
    AbstractContextManager, AbstractAsyncContextManager, 
)
from copy import copy
from dataclasses import dataclass
from itertools import batched, pairwise
from inspect import isawaitable, iscoroutinefunction, signature
from sys import _getframe, exc_info
from _thread import start_new_thread
from time import sleep, time
from types import FrameType
from typing import (
    cast, overload, Any, AsyncContextManager, ContextManager, Literal, 
)

from asynctools import (
    async_filter, async_map, async_reduce, async_zip, async_batched, 
    ensure_async, ensure_aiter, collect as async_collect, 
)
from texttools import format_time
from undefined import undefined


@dataclass(slots=True, frozen=True, unsafe_hash=True)
class Yield:
    """专供 `run_gen_step_iter`，说明值需要 yield 给用户
    """
    value: Any


@dataclass(slots=True, frozen=True, unsafe_hash=True)
class YieldFrom:
    """专供 `run_gen_step_iter`，说明值需要解包后逐个 yield 给用户
    """
    value: Any


def iterable(obj, /) -> bool:
    """判断对象是不是 Iterable
    """
    return isinstance(obj, Iterable)


def async_iterable(obj, /) -> bool:
    """判断对象是不是 AsyncIterable
    """
    return isinstance(obj, AsyncIterable)


def _get_async(back: int = 2, /) -> bool:
    """往上查找，从最近的调用栈的命名空间中获取 `async_` 的值
    """
    f: None | FrameType
    f = _getframe(back)
    f_globals = f.f_globals
    f_locals  = f.f_locals
    if f_locals is f_globals:
        return f_locals.get("async_") or False
    while f_locals is not None and f_locals is not f_globals:
        if "async_" in f_locals:
            if f_locals["async_"] is not None:
                return f_locals["async_"]
        f = f.f_back
        if f is None:
            break
        f_locals = f.f_locals
    return False


def _run_gen_step(gen: Generator, /):
    send = gen.send
    try:
        value: Any = send(None)
        while True:
            value = send(value)
    except StopIteration as e:
        return e.value
    finally:
        gen.close()


async def _run_gen_step_async(gen: Generator, /):
    send  = gen.send
    throw = gen.throw
    try:
        ret: Awaitable = send(None)
        while True:
            try:
                value: Any = await ret
            except BaseException as e:
                ret = throw(e)
            else:
                ret = send(value)
    except StopIteration as e:
        if isawaitable(e.value):
            return await e.value
        return e.value
    finally:
        gen.close()


def run_gen_step[**Args](
    gen_step: Generator | Callable[Args, Generator], 
    async_: None | Literal[False, True] = None, 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
):
    """驱动生成器运行，并返回其结果
    """
    if async_ is None:
        async_ = _get_async()
    if not isinstance(gen_step, Generator):
        params = signature(gen_step).parameters
        if ((param := params.get("async_")) and 
            (param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY))
        ):
            kwds.setdefault("async_", async_)
        gen_step = gen_step(*args, **kwds)
    if async_:
        return _run_gen_step_async(gen_step)
    else:
        return _run_gen_step(gen_step)


def _run_gen_step_iter(gen: Generator, /) -> Iterator:
    send = gen.send
    try:
        value: Any = send(None)
        while True:
            if isinstance(value, Yield):
                yield value.value
            elif isinstance(value, YieldFrom):
                yield from value.value
            value = send(value)
    except StopIteration as e:
        value = e.value
        if isinstance(value, Yield):
            yield value.value
        elif isinstance(value, YieldFrom):
            yield from value.value
    finally:
        gen.close()


async def _run_gen_step_async_iter(gen: Generator, /) -> AsyncIterator:
    send  = gen.send
    throw = gen.throw
    try:
        ret: Awaitable | Yield | YieldFrom = send(None)
        while True:
            try:
                if isinstance(ret, Awaitable):
                    value: Any = await ret
                else:
                    value = ret.value
                    if isawaitable(value):
                        value = await value
                    if isinstance(ret, Yield):
                        yield value
                    elif isinstance(value, AsyncIterable):
                        async for e in value:
                            yield e
                    else:
                        for e in value:
                            yield e
            except BaseException as e:
                ret = throw(e)
            else:
                ret = send(value)
    except StopIteration as e:
        val = e.value
        if isinstance(val, (Yield, YieldFrom)):
            value = val.value
            if isawaitable(value):
                value = await value
            if isinstance(ret, Yield):
                yield value
            elif isinstance(value, AsyncIterable):
                async for e in value:
                    yield e
            else:
                for e in value:
                    yield e
        elif isawaitable(val):
            await val
    finally:
        gen.close()


@overload
def run_gen_step_iter[**Args](
    gen_step: Generator | Callable[Args, Generator], 
    async_: None = None, 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> Iterator | AsyncIterator:
    ...
@overload
def run_gen_step_iter[**Args](
    gen_step: Generator | Callable[Args, Generator], 
    async_: Literal[False] = False, 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> Iterator:
    ...
@overload
def run_gen_step_iter[**Args](
    gen_step: Generator | Callable[Args, Generator], 
    async_: Literal[True], 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> AsyncIterator:
    ...
def run_gen_step_iter[**Args](
    gen_step: Generator | Callable[Args, Generator], 
    async_: None | Literal[False, True] = None, 
    /, 
    *args: Args.args, 
    **kwds: Args.kwargs, 
) -> Iterator | AsyncIterator:
    """驱动生成器运行，并从中返回可迭代而出的值
    """
    if async_ is None:
        async_ = _get_async()
    if not isinstance(gen_step, Generator):
        params = signature(gen_step).parameters
        if ((param := params.get("async_")) and 
            (param.kind in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY))
        ):
            kwds.setdefault("async_", async_)
        gen_step = gen_step(*args, **kwds)
    if async_:
        return _run_gen_step_async_iter(gen_step)
    else:
        return _run_gen_step_iter(gen_step)


def as_gen_step[**Args](
    gen_step: Callable[Args, Generator], 
    /, 
) -> Callable[Args, Any]:
    def wrapper(*args: Args.args, **kwds: Args.kwargs):
        return run_gen_step(
            gen_step, 
            cast(None | Literal[False, True], kwds.pop("async_", None)), 
            *args, 
            **kwds, 
        )
    return wrapper


def as_gen_step_iter[**Args](
    gen_step: Callable[Args, Generator], 
    /, 
) -> Callable[Args, Iterable | AsyncIterable]:
    def wrapper(*args: Args.args, **kwds: Args.kwargs):
        return run_gen_step_iter(
            gen_step, 
            cast(None | Literal[False, True], kwds.pop("async_", None)), 
            *args, 
            **kwds, 
        )
    return wrapper


@overload
def split_cm[T](
    cm: AbstractContextManager[T], 
    /, 
) -> tuple[Callable[[], T], Callable[[], Any]]:
    ...
@overload
def split_cm[T](
    cm: AbstractAsyncContextManager[T], 
    /, 
) -> tuple[Callable[[], Coroutine[Any, Any, T]], Callable[[], Coroutine]]:
    ...
def split_cm[T](
    cm: AbstractContextManager[T] | AbstractAsyncContextManager[T], 
    /, 
) -> (
    tuple[Callable[[], T], Callable[[], Any]] | 
    tuple[Callable[[], Coroutine[Any, Any, T]], Callable[[], Coroutine]]
):
    """拆分上下文管理器，以供 `run_gen_step` 和 `run_gen_step_iter` 使用

    .. code:: python

        if async_:
            async def process():
                async with cm as obj:
                    do_what_you_want()
            return process()
        else:
            with cm as obj:
                do_what_you_want()

    大概相当于

    .. code:: python

        def gen_step():
            enter, exit = split_cm(cm)
            obj = yield enter()
            try:
                do_what_you_want()
            finally:
                yield exit()

        run_gen_step(gen_step, async_)
    """
    if isinstance(cm, AbstractAsyncContextManager):
        enter: Callable = cm.__aenter__
        exit: Callable  = cm.__aexit__
    else:
        enter = cm.__enter__
        exit  = cm.__exit__
    return enter, lambda: exit(*exc_info())


@overload
def with_iter_next[T](
    iterable: Iterable[T], 
    /, 
) -> ContextManager[Callable[[], T]]:
    ...
@overload
def with_iter_next[T](
    iterable: AsyncIterable[T], 
    /, 
) -> ContextManager[Callable[[], Awaitable[T]]]:
    ...
@contextmanager
def with_iter_next[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
):
    """包装迭代器，以供 `run_gen_step` 和 `run_gen_step_iter` 使用

    .. code:: python

        if async_:
            async def process():
                async for e in iterable:
                    do_what_you_want()
            return process()
        else:
            for e in iterable:
                do_what_you_want()

    大概相当于

    .. code:: python

        def gen_step():
            with with_iter_next(iterable) as do_next:
                while True:
                    e = yield do_next()
                    do_what_you_want()

        run_gen_step(gen_step, async_)
    """
    if isinstance(iterable, AsyncIterable):
        try:
            yield aiter(iterable).__anext__
        except StopAsyncIteration:
            pass
    else:
        try:
            yield iter(iterable).__next__
        except StopIteration:
            pass


def map(
    function: None | Callable, 
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
):
    """
    """
    if (
        iscoroutinefunction(function) or 
        isinstance(iterable, AsyncIterable) or 
        any(isinstance(i, AsyncIterable) for i in iterables)
    ):
        if function is None:
            if iterables:
                return async_zip(iterable, *iterables)
            else:
                return iterable
        return async_map(function, iterable, *iterables)
    if function is None:
        if iterables:
            return _zip(iterable, *iterables)
        else:
            return iterable
    return _map(function, iterable, *iterables)


def filter(
    function: None | Callable, 
    iterable: Iterable | AsyncIterable, 
    /, 
):
    """
    """
    if iscoroutinefunction(function) or isinstance(iterable, AsyncIterable):
        return async_filter(function, iterable)
    return _filter(function, iterable)


def reduce(
    function: Callable, 
    iterable: Iterable | AsyncIterable, 
    initial: Any = undefined, 
    /, 
):
    """
    """
    if iscoroutinefunction(function) or isinstance(iterable, AsyncIterable):
        return async_reduce(function, iterable, initial)
    from functools import reduce
    if initial is undefined:
        return reduce(function, iterable)
    return reduce(function, iterable, initial)


def zip(
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
):
    """
    """
    if isinstance(iterable, AsyncIterable) or any(isinstance(i, AsyncIterable) for i in iterables):
        return async_zip(iterable, *iterables)
    return _zip(iterable, *iterables)


@overload
def chunked[T](
    iterable: Iterable[T], 
    n: int = 1, 
    /, 
) -> Iterator[Sequence[T]]:
    ...
@overload
def chunked[T](
    iterable: AsyncIterable[T], 
    n: int = 1, 
    /, 
) -> AsyncIterator[Sequence[T]]:
    ...
def chunked[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    n: int = 1, 
    /, 
) -> Iterator[Sequence[T]] | AsyncIterator[Sequence[T]]:
    """
    """
    if n < 0:
        n = 1
    if isinstance(iterable, Sequence):
        if n == 1:
            return ((e,) for e in iterable)
        return (iterable[i:j] for i, j in pairwise(range(0, len(iterable)+n, n)))
    elif isinstance(iterable, Iterable):
        return batched(iterable, n)
    else:
        return async_batched(iterable, n)


def foreach(
    value: Callable, 
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
):
    """
    """
    if not (isinstance(iterable, Iterable) and all(isinstance(it, Iterable) for it in iterables)):
        return async_foreach(value, iterable, *iterables)
    if iterables:
        for args in _zip(iterable, *iterables):
            value(*args)
    else:
        for arg in iterable:
            value(arg)


async def async_foreach(
    value: Callable, 
    iterable: Iterable | AsyncIterable, 
    /, 
    *iterables: Iterable | AsyncIterable, 
    threaded: bool = False, 
):
    """
    """
    value = ensure_async(value, threaded=threaded)
    if iterables:
        async for args in async_zip(iterable, *iterables, threaded=threaded):
            await value(*args)
    else:
        async for arg in ensure_aiter(iterable, threaded=threaded):
            await value(arg)


def through(
    iterable: Iterable | AsyncIterable, 
    /, 
    take_while: None | Callable = None, 
):
    """
    """
    if not isinstance(iterable, Iterable):
        return async_through(iterable, take_while)
    if take_while is None:
        for _ in iterable:
            pass
    else:
        for v in _map(take_while, iterable):
            if not v:
                break


async def async_through(
    iterable: Iterable | AsyncIterable, 
    /, 
    take_while: None | Callable = None, 
    threaded: bool = False, 
):
    """
    """
    iterable = ensure_aiter(iterable, threaded=threaded)
    if take_while is None:
        async for _ in iterable:
            pass
    elif take_while is bool:
        async for v in iterable:
            if not v:
                break
    else:
        async for v in async_map(take_while, iterable):
            if not v:
                break


@overload
def flatten(
    iterable: Iterable, 
    /, 
    exclude_types: type | tuple[type, ...] = (Buffer, str), 
) -> Iterator:
    ...
@overload
def flatten(
    iterable: AsyncIterable, 
    /, 
    exclude_types: type | tuple[type, ...] = (Buffer, str), 
) -> AsyncIterator:
    ...
def flatten(
    iterable: Iterable | AsyncIterable, 
    /, 
    exclude_types: type | tuple[type, ...] = (Buffer, str), 
) -> Iterator | AsyncIterator:
    """
    """
    if not isinstance(iterable, Iterable):
        return async_flatten(iterable, exclude_types)
    def gen(iterable):
        for e in iterable:
            if isinstance(e, (Iterable, AsyncIterable)) and not isinstance(e, exclude_types):
                yield from gen(e)
            else:
                yield e
    return gen(iterable)


async def async_flatten(
    iterable: Iterable | AsyncIterable, 
    /, 
    exclude_types: type | tuple[type, ...] = (Buffer, str), 
    threaded: bool = False, 
) -> AsyncIterator:
    """
    """
    async for e in ensure_aiter(iterable, threaded=threaded):
        if isinstance(e, (Iterable, AsyncIterable)) and not isinstance(e, exclude_types):
            async for e in async_flatten(e, exclude_types, threaded=threaded):
                yield e
        else:
            yield e


@overload
def collect[K, V](
    iterable: Iterable[tuple[K, V]] | Mapping[K, V], 
    /, 
    rettype: Callable[[Iterable[tuple[K, V]]], MutableMapping[K, V]], 
) -> MutableMapping[K, V]:
    ...
@overload
def collect[T](
    iterable: Iterable[T], 
    /, 
    rettype: Callable[[Iterable[T]], Collection[T]] = list, 
) -> Collection[T]:
    ...
@overload
def collect[K, V](
    iterable: AsyncIterable[tuple[K, V]], 
    /, 
    rettype: Callable[[Iterable[tuple[K, V]]], MutableMapping[K, V]], 
) -> Coroutine[Any, Any, MutableMapping[K, V]]:
    ...
@overload
def collect[T](
    iterable: AsyncIterable[T], 
    /, 
    rettype: Callable[[Iterable[T]], Collection[T]] = list, 
) -> Coroutine[Any, Any, Collection[T]]:
    ...
def collect(
    iterable: Iterable | AsyncIterable | Mapping, 
    /, 
    rettype: Callable[[Iterable], Collection] = list, 
) -> Collection | Coroutine[Any, Any, Collection]:
    """
    """
    if not isinstance(iterable, Iterable):
        return async_collect(iterable, rettype)
    return rettype(iterable)


@overload
def group_collect[K, V, C: Container](
    iterable: Iterable[tuple[K, V]], 
    mapping: None = None, 
    factory: None | C | Callable[[], C] = None, 
) -> dict[K, C]:
    ...
@overload
def group_collect[K, V, C: Container, M: MutableMapping](
    iterable: Iterable[tuple[K, V]], 
    mapping: M, 
    factory: None | C | Callable[[], C] = None, 
) -> M:
    ...
@overload
def group_collect[K, V, C: Container](
    iterable: AsyncIterable[tuple[K, V]], 
    mapping: None = None, 
    factory: None | C | Callable[[], C] = None, 
) -> Coroutine[Any, Any, dict[K, C]]:
    ...
@overload
def group_collect[K, V, C: Container, M: MutableMapping](
    iterable: AsyncIterable[tuple[K, V]], 
    mapping: M, 
    factory: None | C | Callable[[], C] = None, 
) -> Coroutine[Any, Any, M]:
    ...
def group_collect[K, V, C: Container, M: MutableMapping](
    iterable: Iterable[tuple[K, V]] | AsyncIterable[tuple[K, V]], 
    mapping: None | M = None, 
    factory: None | C | Callable[[], C] = None, 
) -> dict[K, C] | M | Coroutine[Any, Any, dict[K, C]] | Coroutine[Any, Any, M]:
    """
    """
    if not isinstance(iterable, Iterable):
        return async_group_collect(iterable, mapping, factory)
    if factory is None:
        if isinstance(mapping, defaultdict):
            factory = mapping.default_factory
        elif mapping:
            factory = type(next(iter(ValuesView(mapping))))
        else:
            factory = cast(type[C], list)
    elif callable(factory):
        pass
    elif isinstance(factory, Container):
        factory = cast(Callable[[], C], lambda _obj=factory: copy(_obj))
    else:
        raise ValueError("can't determine factory")
    factory = cast(Callable[[], C], factory)
    if isinstance(factory, type):
        factory_type = factory
    else:
        factory_type = type(factory())
    if issubclass(factory_type, MutableSequence):
        add = getattr(factory_type, "append")
    else:
        add = getattr(factory_type, "add")
    if mapping is None:
        mapping = cast(M, {})
    for k, v in iterable:
        try:
            c = mapping[k]
        except LookupError:
            c = mapping[k] = factory()
        add(c, v)
    return mapping


@overload
async def async_group_collect[K, V, C: Container](
    iterable: Iterable[tuple[K, V]] | AsyncIterable[tuple[K, V]], 
    mapping: None = None, 
    factory: None | C | Callable[[], C] = None, 
    threaded: bool = False, 
) -> dict[K, C]:
    ...
@overload
async def async_group_collect[K, V, C: Container, M: MutableMapping](
    iterable: Iterable[tuple[K, V]] | AsyncIterable[tuple[K, V]], 
    mapping: M, 
    factory: None | C | Callable[[], C] = None, 
    threaded: bool = False, 
) -> M:
    ...
async def async_group_collect[K, V, C: Container, M: MutableMapping](
    iterable: Iterable[tuple[K, V]] | AsyncIterable[tuple[K, V]], 
    mapping: None | M = None, 
    factory: None | C | Callable[[], C] = None, 
    threaded: bool = False, 
) -> dict[K, C] | M:
    """
    """
    iterable = ensure_aiter(iterable, threaded=threaded)
    if factory is None:
        if isinstance(mapping, defaultdict):
            factory = mapping.default_factory
        elif mapping:
            factory = type(next(iter(ValuesView(mapping))))
        else:
            factory = cast(type[C], list)
    elif callable(factory):
        pass
    elif isinstance(factory, Container):
        factory = cast(Callable[[], C], lambda _obj=factory: copy(_obj))
    else:
        raise ValueError("can't determine factory")
    factory = cast(Callable[[], C], factory)
    if isinstance(factory, type):
        factory_type = factory
    else:
        factory_type = type(factory())
    if issubclass(factory_type, MutableSequence):
        add = getattr(factory_type, "append")
    else:
        add = getattr(factory_type, "add")
    if mapping is None:
        mapping = cast(M, {})
    async for k, v in iterable:
        try:
            c = mapping[k]
        except LookupError:
            c = mapping[k] = factory()
        add(c, v)
    return mapping


@overload
def iter_unique[T](
    iterable: Iterable[T], 
    /, 
    seen: None | MutableSet = None, 
) -> Iterator[T]:
    ...
@overload
def iter_unique[T](
    iterable: AsyncIterable[T], 
    /, 
    seen: None | MutableSet = None, 
) -> AsyncIterator[T]:
    ...
def iter_unique[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    seen: None | MutableSet = None, 
) -> Iterator[T] | AsyncIterator[T]:
    """
    """
    if not isinstance(iterable, Iterable):
        return async_iter_unique(iterable, seen)
    if seen is None:
        seen = set()
    def gen(iterable):
        add = seen.add
        for e in iterable:
            if e not in seen:
                yield e
                add(e)
    return gen(iterable)


async def async_iter_unique[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    seen: None | MutableSet = None, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    """
    """
    if seen is None:
        seen = set()
    add = seen.add
    async for e in ensure_aiter(iterable, threaded=threaded):
        if e not in seen:
            yield e
            add(e)


@overload
def wrap_iter[T](
    iterable: Iterable[T], 
    /, 
    callprev: None | Callable[[T], Any] = None, 
    callnext: None | Callable[[T], Any] = None, 
) -> Iterator[T]:
    ...
@overload
def wrap_iter[T](
    iterable: AsyncIterable[T], 
    /, 
    callprev: None | Callable[[T], Any] = None, 
    callnext: None | Callable[[T], Any] = None, 
) -> AsyncIterator[T]:
    ...
def wrap_iter[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    callprev: None | Callable[[T], Any] = None, 
    callnext: None | Callable[[T], Any] = None, 
) -> Iterator[T] | AsyncIterator[T]:
    """
    """
    if not isinstance(iterable, Iterable):
        return wrap_aiter(
            iterable, 
            callprev=callprev, 
            callnext=callnext, 
        )
    if not callable(callprev):
        callprev = None
    if not callable(callnext):
        callnext = None
    def gen():
        for e in iterable:
            callprev and callprev(e)
            yield e
            callnext and callnext(e)
    return gen()


async def wrap_aiter[T](
    iterable: Iterable[T] | AsyncIterable[T], 
    /, 
    callprev: None | Callable[[T], Any] = None, 
    callnext: None | Callable[[T], Any] = None, 
    threaded: bool = False, 
) -> AsyncIterator[T]:
    """
    """
    callprev = ensure_async(callprev, threaded=threaded) if callable(callprev) else None
    callnext = ensure_async(callnext, threaded=threaded) if callable(callnext) else None
    async for e in ensure_aiter(iterable, threaded=threaded):
        callprev and await callprev(e)
        yield e
        callnext and await callnext(e)


def acc_step(
    start: int, 
    stop: None | int = None, 
    step: int = 1, 
) -> Iterator[tuple[int, int, int]]:
    """
    """
    if stop is None:
        start, stop = 0, start
    for i in range(start + step, stop, step):
        yield start, (start := i), step
    if start != stop:
        yield start, stop, stop - start


def cut_iter(
    start: int, 
    stop: None | int = None, 
    step: int = 1, 
) -> Iterator[tuple[int, int]]:
    """
    """
    if stop is None:
        start, stop = 0, start
    for start in range(start + step, stop, step):
        yield start, step
    if start != stop:
        yield stop, stop - start


@overload
def bfs_gen[T](
    initial: T, 
    /, 
    unpack_iterator: Literal[False] = False, 
) -> Generator[T, T | None, None]:
    ...
@overload
def bfs_gen[T](
    initial: T | Iterator[T], 
    /, 
    unpack_iterator: Literal[True], 
) -> Generator[T, T | None, None]:
    ...
def bfs_gen[T](
    initial: T | Iterator[T], 
    /, 
    unpack_iterator: bool = False, 
) -> Generator[T, T | None, None]:
    """辅助函数，返回生成器，用来简化广度优先遍历
    """
    dq: deque[T] = deque()
    push, pushmany, pop = dq.append, dq.extend, dq.popleft
    if isinstance(initial, Iterator) and unpack_iterator:
        pushmany(initial)
    else:
        push(initial) # type: ignore
    while dq:
        args: None | T = yield (val := pop())
        if unpack_iterator:
            while args is not None:
                if isinstance(args, Iterator):
                    pushmany(args)
                else:
                    push(args)
                args = yield val
        else:
            while args is not None:
                push(args)
                args = yield val


@overload
def context[T](
    func: Callable[..., T], 
    *ctxs: ContextManager, 
    async_: Literal[False], 
) -> T:
    ...
@overload
def context[T](
    func: Callable[..., T] | Callable[..., Awaitable[T]], 
    *ctxs: ContextManager | AsyncContextManager, 
    async_: Literal[True], 
) -> Coroutine[Any, Any, T]:
    ...
@overload
def context[T](
    func: Callable[..., T] | Callable[..., Awaitable[T]], 
    *ctxs: ContextManager | AsyncContextManager, 
    async_: None = None, 
) -> T | Coroutine[Any, Any, T]:
    ...
def context[T](
    func: Callable[..., T] | Callable[..., Awaitable[T]], 
    *ctxs: ContextManager | AsyncContextManager, 
    async_: None | Literal[False, True] = None, 
) -> T | Coroutine[Any, Any, T]:
    """
    """
    if async_ is None:
        if iscoroutinefunction(func):
            async_ = True
        else:
            async_ = _get_async()
    if async_:
        async def call():
            args: list = []
            add_arg = args.append
            with ExitStack() as stack:
                async with AsyncExitStack() as async_stack:
                    enter = stack.enter_context
                    async_enter = async_stack.enter_async_context
                    for ctx in ctxs:
                        if isinstance(ctx, AsyncContextManager):
                            add_arg(await async_enter(ctx))
                        else:
                            add_arg(enter(ctx))
                    ret = func(*args)
                    if isawaitable(ret):
                        ret = await ret
                    return ret
        return call()
    else:
        with ExitStack() as stack:
            return func(*map(stack.enter_context, ctxs)) # type: ignore


@overload
def backgroud_loop(
    call: None | Callable = None, 
    /, 
    interval: int | float = 0.05, 
    *, 
    async_: Literal[False], 
) -> ContextManager:
    ...
@overload
def backgroud_loop(
    call: None | Callable = None, 
    /, 
    interval: int | float = 0.05, 
    *, 
    async_: Literal[True], 
) -> AsyncContextManager:
    ...
@overload
def backgroud_loop(
    call: None | Callable = None, 
    /, 
    interval: int | float = 0.05, 
    *, 
    async_: None = None, 
) -> ContextManager | AsyncContextManager:
    ...
def backgroud_loop(
    call: None | Callable = None, 
    /, 
    interval: int | float = 0.05, 
    *, 
    async_: None | Literal[False, True] = None, 
) -> ContextManager | AsyncContextManager:
    """
    """
    if async_ is None:
        if iscoroutinefunction(call):
            async_ = True
        else:
            async_ = _get_async()
    use_default_call = not callable(call)
    if use_default_call:
        start = time()
        def call():
            print(f"\r\x1b[K{format_time(time() - start)}", end="")
    def run():
        while running:
            try:
                yield call
            except Exception:
                pass
            if interval > 0:
                if async_:
                    yield async_sleep(interval)
                else:
                    sleep(interval)
    running = True
    if async_:
        @asynccontextmanager
        async def actx():
            nonlocal running
            try:
                task = create_task(run())
                yield task
            finally:
                running = False
                task.cancel()
                if use_default_call:
                    print("\r\x1b[K", end="")
        return actx()
    else:
        @contextmanager
        def ctx():
            nonlocal running
            try:
                yield start_new_thread(run, ())
            finally:
                running = False
                if use_default_call:
                    print("\r\x1b[K", end="")
        return ctx()


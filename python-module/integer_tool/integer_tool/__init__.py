#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 4)
__all__ = [
    "bit_length", "count1", "count0", "int_to_bytes", 
    "int_to_bitarray", "int_from_bytes", "lshift", 
    "rshift", "chain", "chain_bytes", "join", "set", 
    "clear", "reverse", "join_bits", "set_bits", 
    "clear_bits", "reverse_bits", "cut_bits", "is_pow2", 
    "is_odd", "is_diff_sign", "sup_pow2", "inf_pow2", 
    "floordiv", "ceildiv", "flip", "shift", "clear_start_ones", 
    "set_start_zeros", "cut_start_ones", "get_first_one", 
    "set_first_zero", 
]

from collections.abc import Buffer
from typing import cast, Final, Literal

from bitarray import bitarray


_to_bytes: Final   = int.to_bytes
_from_bytes: Final = int.from_bytes
bit_length: Final  = int.bit_length
count1: Final      = int.bit_count


def count0(n: int, /) -> int:
    "统计最高位右边 0 的个数"
    return bit_length(n) - count1(n)


def int_to_bytes(
    n: int, 
    /, 
    byteorder: Literal["little", "big"] = "big", 
    signed: bool = False, 
) -> bytes:
    "整数转换为字节数组"
    return _to_bytes(
        n, 
        length=(bit_length(n) + 0b111) >> 3, 
        byteorder=byteorder, 
        signed=signed
    )


def int_to_bitarray(
    n: int, 
    /, 
    byteorder: Literal["little", "big"] = "big", 
    signed: bool = False, 
) -> bitarray:
    "整数转换为位数组"
    return bitarray(int_to_bytes(n, byteorder, signed))


def int_from_bytes(
    b: Buffer, 
    /, 
    byteorder: Literal["little", "big"] = "big", 
    signed: bool = False, 
) -> int:
    "字节数组转换为整数"
    return _from_bytes(b, byteorder=byteorder, signed=signed)


def lshift(n: int, /, offset: int = 1) -> int:
    """按位左移
    """
    if offset < 0:
        return n >> (-offset)
    return n << offset


def rshift(n: int, /, offset: int = 1) -> int:
    """按位右移
    """
    if offset < 0:
        return n << (-offset)
    return n >> offset


def chain(x: int, /, *nums: int) -> int:
    """数字以比特位（二进制）形式串联
    """
    for n in nums:
        x = (x << bit_length(n)) | n
    return x


def chain_bytes(x: int, /, *nums: int) -> int:
    """数字以字节形式形式串联
    """
    if nums:
        b = bytearray(int_to_bytes(x))
        extend = b.extend
        for n in map(int_to_bytes, nums):
            extend(n)
        x = int_from_bytes(b)
    return x


def join(x: int, y: int, /) -> int:
    "按位交"
    return x & y


def set(x: int, y: int, /) -> int:
    "按位或"
    return x | y


def clear(x: int, y: int, /) -> int:
    "按位置 0"
    return x & ~y


def reverse(x: int, y: int, /) -> int:
    "按位异或"
    return x ^ y


def join_bits(x: int, /, offset: int = 0, length: int = 1) -> int:
    "截取指定范围的二进制，并保留其位置"
    if not x:
        return 0
    if length == 1:
        y = 1 << offset
    elif length <= 0:
        y = (1 << bit_length(x)) - (1 << offset)
    else:
        y = (1 << (offset + length)) - (1 << offset)
    return x & y


def set_bits(x: int, /, offset: int = 0, length: int = 1) -> int:
    "置 1 指定范围的二进制"
    if length == 1:
        y = 1 << offset
    elif length <= 0:
        y = (1 << bit_length(x)) - (1 << offset)
    else:
        y = (1 << (offset + length)) - (1 << offset)
    return x | y


def clear_bits(x: int, /, offset: int = 0, length: int = 1) -> int:
    "置 0 指定范围的二进制"
    if not x:
        return 0
    if length == 1:
        y = 1 << offset
    elif length <= 0:
        y = (1 << bit_length(x)) - (1 << offset)
    else:
        y = (1 << (offset + length)) - (1 << offset)
    return x & ~y


def reverse_bits(x: int, /, offset: int = 0, length: int = 1) -> int:
    "置反指定范围的二进制"
    if length == 1:
        y = 1 << offset
    elif length <= 0:
        y = (1 << bit_length(x)) - (1 << offset)
    else:
        y = (1 << (offset + length)) - (1 << offset)
    return x ^ y


def cut_bits(x: int, /, offset: int = 0, length: int = 1) -> int:
    "截取指定范围的二进制"
    if not x:
        return 0
    if length == 1:
        return (x >> offset) & 1
    elif length <= 0:
        return x >> offset
    else:
        return (x >> offset) & ((1 << length) - 1)


def is_pow2(n: int, /) -> bool:
    "是否 2 的自然数次幂"
    return n > 0 and n & (n - 1) == 0


def sup_pow2(n: int, /) -> int:
    "不大于 x 的最大的 2 的自然数次幂"
    if n < 1:
        raise ValueError(f"{n!r} < 1")
    return 1 << bit_length(n - 1)


def inf_pow2(n: int, /) -> int:
    "不小于 x 的最小的 2 的自然数次幂"
    if n <= 1:
        return 1
    return 1 << (bit_length(n) - 1)


def is_odd(x: int, /) -> Literal[0, 1]:
    "是否奇数"
    return x & 1 # type: ignore


def is_diff_sign(x: int, y: int, /) -> bool:
    "是否正负号不同"
    return x ^ y < 0


def floordiv(x: int, y: int, /) -> int:
    "向下整除的除法"
    return x // y


def ceildiv(x: int, y: int, /) -> int:
    "向上整除的除法"
    return -(-x // y)


def flip(x: int, /) -> int:
    """把数字的高低位进行翻转

    实现方法 2

        .. code:: python

            def flip(x: int, /) -> int:
                return int(format(x, "b")[::-1], 2)

    实现方法 3

        .. code:: python

            def flip(x: int, /, min_len: int = 0) -> int:
                bln = bit_length(x)
                max_idx = max(min_len, bln) - 1
                t = 0
                for i in range(bln):
                    if x & (1 << i):
                        t |= 1 << (max_idx - i)
                return t

    实现方法 4: 蝶式法

        .. code:: python

            def flip(x: int, /) -> int:
                if not (0 <= x < 1 << 16):
                    raise ValueError("only accept integer: 0 <= x < 65536")
                x = ((x & 0xAAAA) >> 1) | ((x & 0x5555) << 1)
                x = ((x & 0xCCCC) >> 2) | ((x & 0x3333) << 2)
                x = ((x & 0xF0F0) >> 4) | ((x & 0x0F0F) << 4)
                return ((x & 0xFF00) >> 8) | ((x & 0x00FF) << 8)

        .. code:: python

            def flip(x: int, /) -> int:
                if not (0 <= x < 1 << 16):
                    raise ValueError("only accept integer: 0 <= x < 65536")
                x = (x << 8) | (x >> 8)
                x = ((x << 4) & 0xF0F0) | ((x >> 4) & 0x0F0F)
                x = ((x << 2) & 0xCCCC) | ((x >> 2) & 0x3333)
                return ((x << 1) & 0xAAAA) | ((x >> 1) & 0x5555)
    """
    b = bitarray(int_to_bytes(x))
    b.reverse()
    return int_from_bytes(cast(Buffer, b)) >> (len(b) - bit_length(x))


def shift(x: int, /, offset: int = 1) -> int:
    "首位相接（环形）移动"
    ln = bit_length(x)
    offset = offset % ln
    return ((x << offset) | x >> (ln - offset)) & ((1 << ln) - 1)


def clear_start_ones(x: int, /) -> int:
    "把右起连续的 1 变成 0"
    return x & (x + 1)


def set_start_zeros(x: int, /) -> int:
    "把右起连续的 0 变成 1"
    return x | (x - 1)


def cut_start_ones(x: int, /) -> int:
    "取右起连续的 1"
    return (x ^ (x + 1)) >> 1


def get_first_one(x: int, /) -> int:
    "取右边第 1 个 1，也即去掉右起第 1 个 1 的左边"
    # 等于 x & (~x + 1)
    return x & -x


def set_first_zero(x: int, /) -> int:
    "把右起第 1 个 0 变成 1"
    return x | (x + 1)


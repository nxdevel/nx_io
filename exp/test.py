#!/usr/bin/env python3
"""Combining wrapper"""
import io
from itertools import islice, repeat, takewhile, filterfalse
from math import ceil
from unicodedata import combining, normalize
# import csv


def buffer_fill_to_size(initial, fobj, size, eof, block_size=512):
    fobj_read = fobj.read
    more = max(size - len(initial), 0)
    while not eof and more:
        to_read = ceil(more / block_size) * block_size
        data = fobj_read(to_read)
        if data:
            initial += data
            more = max(size - len(initial), 0)
        else:
            eof = True
    return initial, eof


def buffer_fill_to_pred(initial, fobj, pred, eof, block_size=512):
    fobj_read = fobj.read
    result = next(filter(lambda x: pred(x[1]), enumerate(initial)), None)
    while result is None:
        data = fobj_read(block_size)
        if not data:
            return initial, len(initial), True
        initial += data
        result = next(filter(lambda x: pred(x[1]), enumerate(initial)), None)
    return initial, result[0], eof


class CombiningWrapper:
    """Combining wrapper"""
    def readall(self):
        fobj_read = self._fobj.read
        buf = self._buf
        self._buf = ''
        data = fobj_read()
        while data:
            buf += data
            data = fobj_read()
        form = self.form
        if form is not None:
            return normalize(form, buf)
        return buf

    def read(self, size=-1):
        if size is None or size < 0:
            return self.readall()

        if size == 0:
            return ''

        fobj_read = self._fobj.read
        block_size = self._block_size

        buf, eof = self._buf, self._eof

        more = max(size - len(buf), 0)

        while not eof and more:

            to_read = ceil(more // block_size) * block_size

            data = fobj_read(to_read)
            if data:
                buf += data

                more = max(size - len(buf), 0)

            else:
                self._eof = eof = True

        extra = buf[size:]
        if not extra:
            extra = fobj_read(block_size)
        buf = buf[:size]
        while extra:
            result = next(filterfalse(lambda x: combining(x[1]),
                                      enumerate(extra)), None)
            if result is not None:
                idx = result[0]
                buf += extra[:idx]
                self._buf = extra[idx:]
                break
            buf += extra
            extra = fobj_read(block_size)
        else:
            self._buf = ''
        return buf

    def __init__(self, fobj, *, form=None, block_size=512):
        self._fobj = fobj
        self._form = form
        self._block_size = block_size
        buf = fobj.read(1)
        if isinstance(buf, bytes):
            raise ValueError('stream must be in text mode')
        self._buf, self._eof = buf, not buf


# a = io.StringIO('oo\u0308\u0308\u0308o\u0308\u0308')
# o = CombiningWrapper(a)
# form = 'NFD'

# s1 = ''.join(map(lambda x: normalize(form, x), takewhile(lambda x: x, map(o.read, repeat(1)))))
# print(s1, len(s1))
# a.seek(0)
# s2 = normalize(form, a.read())
# print(s2, len(s2))
# print(s1 == s2)


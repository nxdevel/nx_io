# -*- coding: utf-8 -*-
"""Open function supporting different path specifications"""
import io
import pathlib


__all__ = ['open_file']


def open_file(file_name, *args, **kwargs):
    """Open a file

    Arguments
    ---------
    file_name : str, bytes, pathlib, or fspath
                The file to open
    args      : variable
    kwargs    : variable

    Returns
    -------
    A stream object

    The positional and keyword arguments are the same as those used by
    io.open() and are passed directly to it.

    Originally intended to combine open() and pathlib's open() method, PEP 519
    suggests open() will be updated to additionally handle objects implementing
    a new protocol, __fspath__, and pathlib will be updated to implement
    __fspath__ making this a shim for the future.
    """
    if not isinstance(file_name, (str, bytes, int)):
        if isinstance(file_name, pathlib.Path):
            file_name = str(file_name)
        elif hasattr(file_name, '__fspath__'):
            file_name = file_name.__fspath__()
        else:
            raise TypeError('invalid file: {}'.format(repr(file_name)))
    return io.open(file_name, *args, **kwargs)

"Open function supporting different path specifications"
import io
import pathlib


__all__ = ['open_file']


def open_file(file_name, mode='rt', **kwargs):
    """Open a file.

    Arguments
    ---------
    file_name : str, bytes, pathlib, fspath
    mode      : str
                The mode in which the file is opened
    kwargs    : variable
                The keyword arguments are the same as open() and are passed
                directly to it

    Returns
    -------
    A stream object

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
            raise TypeError('unknown file name type: {}'
                            .format(file_name.__class__.__name__))
    return io.open(file_name, mode=mode, **kwargs)

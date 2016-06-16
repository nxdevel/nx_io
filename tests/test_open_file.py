"""Tests for nx_io.open_file"""
import os
import tempfile
import contextlib
import pathlib
import pytest
from nx_io import open_file


class File519:                # pylint: disable=too-few-public-methods
    """Emulate a PEP 519 conforming object."""
    def __fspath__(self):
        return self._path

    def __init__(self, path):
        self._path = path


@contextlib.contextmanager
def make_file(data, convert=lambda x: x):
    """Context manager to create a file containing *data* and delete it.

    NamedTemporaryFile with delete=True won't work as it cannot be portably
    opened as a regular file.
    """
    (fileno, file_name) = tempfile.mkstemp()
    with open(fileno, 'wt' if isinstance(data, str) else 'wb') as fobj:
        fobj.write(data)
    try:
        yield convert(file_name)
    finally:
        os.unlink(file_name)


def test_file_name_str():
    """Test open_file with a string file name."""
    data = 'abc'
    with make_file(data, str) as file_name:
        fobj = open_file(file_name)
        assert fobj.read() == data
        fobj.close()


def test_file_name_bin():
    """Test open_file with a bytes file name."""
    data = 'abc'
    with make_file(data, lambda x: x.encode()) as file_name:
        fobj = open_file(file_name)
        assert fobj.read() == data
        fobj.close()


def test_file_name_path():
    """Test open_file with a pathlib file name."""
    data = 'abc'
    with make_file(data, pathlib.Path) as file_name:
        fobj = open_file(file_name)
        assert fobj.read() == data
        fobj.close()


def test_file_name_519():
    """Test open_file with a PEP 519 file name."""
    data = 'abc'
    with make_file(data, File519) as file_name:
        fobj = open_file(file_name)
        assert fobj.read() == data
        fobj.close()


def test_file_name_descriptor():
    """Test open_file with a file descriptor."""
    data = 'abc'
    with make_file(data) as file_name, open(file_name) as bobj:
        fobj = open_file(bobj.fileno(), closefd=False)
        assert fobj.read() == data
        fobj.close()
        assert bobj.closed is False


def test_file_name_obj():
    """Test open_file with a file object."""
    data = 'abc'
    with make_file(data) as file_name, open(file_name) as bobj:
        with pytest.raises(TypeError):
            open_file(bobj)

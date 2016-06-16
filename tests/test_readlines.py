"Tests for nx_io.readlines"
import io
import re
from unittest import mock
import pytest
from nx_io import ReadLines


def test_newline_text():
    """Test ReadLines with the default delimiter and text data."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj)
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('\n')
    rdr = ReadLines(fobj)
    assert next(rdr) == '\n'
    assert list(rdr) == []

    fobj = io.StringIO('a\nb\n\nc')
    rdr = ReadLines(fobj)
    assert next(rdr) == 'a\n'
    assert list(rdr) == ['b\n', '\n', 'c']

    fobj = io.StringIO('\na\nb\n\nc\n')
    rdr = ReadLines(fobj)
    assert next(rdr) == '\n'
    assert list(rdr) == ['a\n', 'b\n', '\n', 'c\n']

    fobj = io.StringIO('')
    rdr = ReadLines(fobj)
    assert list(rdr) == []


def test_newline_text_small_buffer():
    """Test ReadLines with the default delimiter, text data, and a small
    buffer."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj, buffer_size=1)
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('\n')
    rdr = ReadLines(fobj, buffer_size=1)
    assert next(rdr) == '\n'
    assert list(rdr) == []

    fobj = io.StringIO('a\nb\n\nc')
    rdr = ReadLines(fobj, buffer_size=1)
    assert next(rdr) == 'a\n'
    assert list(rdr) == ['b\n', '\n', 'c']

    fobj = io.StringIO('\na\nb\n\nc\n')
    rdr = ReadLines(fobj, buffer_size=1)
    assert next(rdr) == '\n'
    assert list(rdr) == ['a\n', 'b\n', '\n', 'c\n']


def test_newline_bin():
    """Test ReadLines with a newline delimiter and binary data."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=b'\n')
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b'\n')
    rdr = ReadLines(fobj, delimiter=b'\n')
    assert next(rdr) == b'\n'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\nb\n\nc')
    rdr = ReadLines(fobj, delimiter=b'\n')
    assert next(rdr) == b'a\n'
    assert list(rdr) == [b'b\n', b'\n', b'c']

    fobj = io.BytesIO(b'\na\nb\n\nc\n')
    rdr = ReadLines(fobj, delimiter=b'\n')
    assert next(rdr) == b'\n'
    assert list(rdr) == [b'a\n', b'b\n', b'\n', b'c\n']

    fobj = io.BytesIO(b'')
    rdr = ReadLines(fobj, delimiter=b'\n')
    assert list(rdr) == []


def test_newline_bin_small_buffer():
    """Test ReadLines with a newline delimiter, binary data, and a small
    buffer."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=b'\n', buffer_size=1)
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b'\n')
    rdr = ReadLines(fobj, delimiter=b'\n', buffer_size=1)
    assert next(rdr) == b'\n'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\nb\n\nc')
    rdr = ReadLines(fobj, delimiter=b'\n', buffer_size=1)
    assert next(rdr) == b'a\n'
    assert list(rdr) == [b'b\n', b'\n', b'c']

    fobj = io.BytesIO(b'\na\nb\n\nc\n')
    rdr = ReadLines(fobj, delimiter=b'\n', buffer_size=1)
    assert next(rdr) == b'\n'
    assert list(rdr) == [b'a\n', b'b\n', b'\n', b'c\n']


def test_single_text():
    """Test ReadLines with a single character delimiter and text data."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('~')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == '~'
    assert list(rdr) == []

    fobj = io.StringIO('a\n~b\n~~c')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'a\n~'
    assert list(rdr) == ['b\n~', '~', 'c']

    fobj = io.StringIO('~\na\n~b\n~~c\n~')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == '~'
    assert list(rdr) == ['\na\n~', 'b\n~', '~', 'c\n~']

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter='~')
    assert list(rdr) == []


def test_single_text_small_buffer():
    """Test ReadLines with a single character delimiter, text data, and a small
    buffer."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj, delimiter='~', buffer_size=1)
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('~')
    rdr = ReadLines(fobj, delimiter='~', buffer_size=1)
    assert next(rdr) == '~'
    assert list(rdr) == []

    fobj = io.StringIO('a\n~b\n~~c')
    rdr = ReadLines(fobj, delimiter='~', buffer_size=1)
    assert next(rdr) == 'a\n~'
    assert list(rdr) == ['b\n~', '~', 'c']

    fobj = io.StringIO('~\na\n~b\n~~c\n~')
    rdr = ReadLines(fobj, delimiter='~', buffer_size=1)
    assert next(rdr) == '~'
    assert list(rdr) == ['\na\n~', 'b\n~', '~', 'c\n~']


def test_single_bin():
    """Test ReadLines with a single character delimiter and binary data."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=b'~')
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b'~')
    rdr = ReadLines(fobj, delimiter=b'~')
    assert next(rdr) == b'~'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\n~b\n~~c')
    rdr = ReadLines(fobj, delimiter=b'~')
    assert next(rdr) == b'a\n~'
    assert list(rdr) == [b'b\n~', b'~', b'c']

    fobj = io.BytesIO(b'~\na\n~b\n~~c\n~')
    rdr = ReadLines(fobj, delimiter=b'~')
    assert next(rdr) == b'~'
    assert list(rdr) == [b'\na\n~', b'b\n~', b'~', b'c\n~']

    fobj = io.BytesIO(b'')
    rdr = ReadLines(fobj, delimiter=b'~')
    assert list(rdr) == []


def test_single_bin_small_buffer():
    """Test ReadLines with a single character delimiter, binary data, and a
    small buffer."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=b'~', buffer_size=1)
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b'~')
    rdr = ReadLines(fobj, delimiter=b'~', buffer_size=1)
    assert next(rdr) == b'~'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\n~b\n~~c')
    rdr = ReadLines(fobj, delimiter=b'~', buffer_size=1)
    assert next(rdr) == b'a\n~'
    assert list(rdr) == [b'b\n~', b'~', b'c']

    fobj = io.BytesIO(b'~\na\n~b\n~~c\n~')
    rdr = ReadLines(fobj, delimiter=b'~', buffer_size=1)
    assert next(rdr) == b'~'
    assert list(rdr) == [b'\na\n~', b'b\n~', b'~', b'c\n~']


def test_multi_text():
    """Test ReadLines with a multi-character delimiter and text data."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj, delimiter='!@')
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('!@')
    rdr = ReadLines(fobj, delimiter='!@')
    assert next(rdr) == '!@'
    assert list(rdr) == []

    fobj = io.StringIO('a\n!@b\n!@!@c')
    rdr = ReadLines(fobj, delimiter='!@')
    assert next(rdr) == 'a\n!@'
    assert list(rdr) == ['b\n!@', '!@', 'c']

    fobj = io.StringIO('!@\na\n!@b\n!@!@c\n!@')
    rdr = ReadLines(fobj, delimiter='!@')
    assert next(rdr) == '!@'
    assert list(rdr) == ['\na\n!@', 'b\n!@', '!@', 'c\n!@']

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter='!@')
    assert list(rdr) == []


def test_multi_text_small_buffer():
    """Test ReadLines with a multi-character delimiter, text data, and a small
    buffer."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj, delimiter='!@', buffer_size=1)
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('!@')
    rdr = ReadLines(fobj, delimiter='!@', buffer_size=1)
    assert next(rdr) == '!@'
    assert list(rdr) == []

    fobj = io.StringIO('a\n!@b\n!@!@c')
    rdr = ReadLines(fobj, delimiter='!@', buffer_size=1)
    assert next(rdr) == 'a\n!@'
    assert list(rdr) == ['b\n!@', '!@', 'c']

    fobj = io.StringIO('!@\na\n!@b\n!@!@c\n!@')
    rdr = ReadLines(fobj, delimiter='!@', buffer_size=1)
    assert next(rdr) == '!@'
    assert list(rdr) == ['\na\n!@', 'b\n!@', '!@', 'c\n!@']


def test_multi_bin():
    """Test ReadLines with a multi-character delimiter and binary data."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=b'!@')
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b'!@')
    rdr = ReadLines(fobj, delimiter=b'!@')
    assert next(rdr) == b'!@'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\n!@b\n!@!@c')
    rdr = ReadLines(fobj, delimiter=b'!@')
    assert next(rdr) == b'a\n!@'
    assert list(rdr) == [b'b\n!@', b'!@', b'c']

    fobj = io.BytesIO(b'!@\na\n!@b\n!@!@c\n!@')
    rdr = ReadLines(fobj, delimiter=b'!@')
    assert next(rdr) == b'!@'
    assert list(rdr) == [b'\na\n!@', b'b\n!@', b'!@', b'c\n!@']

    fobj = io.BytesIO(b'')
    rdr = ReadLines(fobj, delimiter=b'!@')
    assert list(rdr) == []


def test_multi_bin_small_buffer():
    """Test ReadLines with a multi-character delimiter, binary data, and a
    small buffer."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=b'!@', buffer_size=1)
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b'!@')
    rdr = ReadLines(fobj, delimiter=b'!@', buffer_size=1)
    assert next(rdr) == b'!@'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\n!@b\n!@!@c')
    rdr = ReadLines(fobj, delimiter=b'!@', buffer_size=1)
    assert next(rdr) == b'a\n!@'
    assert list(rdr) == [b'b\n!@', b'!@', b'c']

    fobj = io.BytesIO(b'!@\na\n!@b\n!@!@c\n!@')
    rdr = ReadLines(fobj, delimiter=b'!@', buffer_size=1)
    assert next(rdr) == b'!@'
    assert list(rdr) == [b'\na\n!@', b'b\n!@', b'!@', b'c\n!@']


def test_single_regex_text():
    """Test ReadLines with a single character regex and text data."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj, delimiter=re.compile(r'~'))
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('~')
    rdr = ReadLines(fobj, delimiter=re.compile(r'~'))
    assert next(rdr) == '~'
    assert list(rdr) == []

    fobj = io.StringIO('a\n~b\n~~c')
    rdr = ReadLines(fobj, delimiter=re.compile(r'~'))
    assert next(rdr) == 'a\n~'
    assert list(rdr) == ['b\n~', '~', 'c']

    fobj = io.StringIO('~\na\n~b\n~~c\n~')
    rdr = ReadLines(fobj, delimiter=re.compile(r'~'))
    assert next(rdr) == '~'
    assert list(rdr) == ['\na\n~', 'b\n~', '~', 'c\n~']

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter=re.compile(r'~'))
    assert list(rdr) == []


def test_single_regex_text_small_buffer(): # pylint: disable=invalid-name
    """Test ReadLines with a single character regex, text data, and a small
    buffer."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj, delimiter=re.compile(r'~'), buffer_size=1)
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('~')
    rdr = ReadLines(fobj, delimiter=re.compile(r'~'), buffer_size=1)
    assert next(rdr) == '~'
    assert list(rdr) == []

    fobj = io.StringIO('a\n~b\n~~c')
    rdr = ReadLines(fobj, delimiter=re.compile(r'~'), buffer_size=1)
    assert next(rdr) == 'a\n~'
    assert list(rdr) == ['b\n~', '~', 'c']

    fobj = io.StringIO('~\na\n~b\n~~c\n~')
    rdr = ReadLines(fobj, delimiter=re.compile(r'~'), buffer_size=1)
    assert next(rdr) == '~'
    assert list(rdr) == ['\na\n~', 'b\n~', '~', 'c\n~']


def test_single_regex_bin():
    """Test ReadLines with a single character regex and binary data."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=re.compile(b'~'))
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b'~')
    rdr = ReadLines(fobj, delimiter=re.compile(b'~'))
    assert next(rdr) == b'~'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\n~b\n~~c')
    rdr = ReadLines(fobj, delimiter=re.compile(b'~'))
    assert next(rdr) == b'a\n~'
    assert list(rdr) == [b'b\n~', b'~', b'c']

    fobj = io.BytesIO(b'~\na\n~b\n~~c\n~')
    rdr = ReadLines(fobj, delimiter=re.compile(b'~'))
    assert next(rdr) == b'~'
    assert list(rdr) == [b'\na\n~', b'b\n~', b'~', b'c\n~']

    fobj = io.BytesIO(b'')
    rdr = ReadLines(fobj, delimiter=re.compile(b'~'))
    assert list(rdr) == []


def test_single_regex_bin_small_buffer(): # pylint: disable=invalid-name
    """Test ReadLines with a single character regex, binary data, and a small
    buffer."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=re.compile(b'~'), buffer_size=1)
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b'~')
    rdr = ReadLines(fobj, delimiter=re.compile(b'~'), buffer_size=1)
    assert next(rdr) == b'~'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\n~b\n~~c')
    rdr = ReadLines(fobj, delimiter=re.compile(b'~'), buffer_size=1)
    assert next(rdr) == b'a\n~'
    assert list(rdr) == [b'b\n~', b'~', b'c']

    fobj = io.BytesIO(b'~\na\n~b\n~~c\n~')
    rdr = ReadLines(fobj, delimiter=re.compile(b'~'), buffer_size=1)
    assert next(rdr) == b'~'
    assert list(rdr) == [b'\na\n~', b'b\n~', b'~', b'c\n~']


def test_multi_regex_text():
    """Test ReadLines with a multi-character regex and text data."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj, delimiter=re.compile(r'!@'))
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('!@')
    rdr = ReadLines(fobj, delimiter=re.compile(r'!@'))
    assert next(rdr) == '!@'
    assert list(rdr) == []

    fobj = io.StringIO('a\n!@b\n!@!@c')
    rdr = ReadLines(fobj, delimiter=re.compile(r'!@'))
    assert next(rdr) == 'a\n!@'
    assert list(rdr) == ['b\n!@', '!@', 'c']

    fobj = io.StringIO('!@\na\n!@b\n!@!@c\n!@')
    rdr = ReadLines(fobj, delimiter=re.compile(r'!@'))
    assert next(rdr) == '!@'
    assert list(rdr) == ['\na\n!@', 'b\n!@', '!@', 'c\n!@']

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter=re.compile(r'!@'))
    assert list(rdr) == []


def test_multi_regex_text_small_buffer(): # pylint: disable=invalid-name
    """Test ReadLines with a multi-character regex, text data, and a small
    buffer."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj, delimiter=re.compile(r'!@'), buffer_size=1)
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('!@')
    rdr = ReadLines(fobj, delimiter=re.compile(r'!@'), buffer_size=1)
    assert next(rdr) == '!@'
    assert list(rdr) == []

    fobj = io.StringIO('a\n!@b\n!@!@c')
    rdr = ReadLines(fobj, delimiter=re.compile(r'!@'), buffer_size=1)
    assert next(rdr) == 'a\n!@'
    assert list(rdr) == ['b\n!@', '!@', 'c']

    fobj = io.StringIO('!@\na\n!@b\n!@!@c\n!@')
    rdr = ReadLines(fobj, delimiter=re.compile(r'!@'), buffer_size=1)
    assert next(rdr) == '!@'
    assert list(rdr) == ['\na\n!@', 'b\n!@', '!@', 'c\n!@']


def test_multi_regex_bin():
    """Test ReadLines with a multi-character regex and binary data."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=re.compile(b'!@'))
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b'!@')
    rdr = ReadLines(fobj, delimiter=re.compile(b'!@'))
    assert next(rdr) == b'!@'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\n!@b\n!@!@c')
    rdr = ReadLines(fobj, delimiter=re.compile(b'!@'))
    assert next(rdr) == b'a\n!@'
    assert list(rdr) == [b'b\n!@', b'!@', b'c']

    fobj = io.BytesIO(b'!@\na\n!@b\n!@!@c\n!@')
    rdr = ReadLines(fobj, delimiter=re.compile(b'!@'))
    assert next(rdr) == b'!@'
    assert list(rdr) == [b'\na\n!@', b'b\n!@', b'!@', b'c\n!@']

    fobj = io.BytesIO(b'')
    rdr = ReadLines(fobj, delimiter=re.compile(b'!@'), buffer_size=1)
    assert list(rdr) == []


def test_multi_regex_bin_small_buffer(): # pylint: disable=invalid-name
    """Test ReadLines with a multi-character regex, binary data, and a small
    buffer."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=re.compile(b'!@'), buffer_size=1)
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b'!@')
    rdr = ReadLines(fobj, delimiter=re.compile(b'!@'))
    assert next(rdr) == b'!@'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\n!@b\n!@!@c')
    rdr = ReadLines(fobj, delimiter=re.compile(b'!@'), buffer_size=1)
    assert next(rdr) == b'a\n!@'
    assert list(rdr) == [b'b\n!@', b'!@', b'c']

    fobj = io.BytesIO(b'!@\na\n!@b\n!@!@c\n!@')
    rdr = ReadLines(fobj, delimiter=re.compile(b'!@'), buffer_size=1)
    assert next(rdr) == b'!@'
    assert list(rdr) == [b'\na\n!@', b'b\n!@', b'!@', b'c\n!@']


def test_greedy_regex_text():
    """Test ReadLines with a greedy regex and text data."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj, delimiter=re.compile(r't+'))
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('t')
    rdr = ReadLines(fobj, delimiter=re.compile(r't+'))
    assert next(rdr) == 't'
    assert list(rdr) == []

    fobj = io.StringIO('a\nttb\nttttc')
    rdr = ReadLines(fobj, delimiter=re.compile(r't+'))
    assert next(rdr) == 'a\ntt'
    assert list(rdr) == ['b\ntttt', 'c']

    fobj = io.StringIO('tttta\nttb\nttttctt')
    rdr = ReadLines(fobj, delimiter=re.compile(r't+'))
    assert next(rdr) == 'tttt'
    assert list(rdr) == ['a\ntt', 'b\ntttt', 'ctt']

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter=re.compile(r't+'))
    assert list(rdr) == []


def test_greedy_regex_text_small_buffer(): # pylint: disable=invalid-name
    """Test ReadLines with a greedy regex, text data, and a small buffer."""
    fobj = io.StringIO('a')
    rdr = ReadLines(fobj, delimiter=re.compile(r't+'), buffer_size=1)
    assert next(rdr) == 'a'
    assert list(rdr) == []

    fobj = io.StringIO('t')
    rdr = ReadLines(fobj, delimiter=re.compile(r't+'), buffer_size=1)
    assert next(rdr) == 't'
    assert list(rdr) == []

    fobj = io.StringIO('a\nttb\nttttc')
    rdr = ReadLines(fobj, delimiter=re.compile(r't+'), buffer_size=1)
    assert next(rdr) == 'a\ntt'
    assert list(rdr) == ['b\ntttt', 'c']

    fobj = io.StringIO('tttta\nttb\nttttctt')
    rdr = ReadLines(fobj, delimiter=re.compile(r't+'), buffer_size=1)
    assert next(rdr) == 'tttt'
    assert list(rdr) == ['a\ntt', 'b\ntttt', 'ctt']


def test_greedy_regex_bin():
    """Test ReadLines with a greedy regex and binary data."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=re.compile(b't+'))
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b't')
    rdr = ReadLines(fobj, delimiter=re.compile(b't+'))
    assert next(rdr) == b't'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\nttb\nttttc')
    rdr = ReadLines(fobj, delimiter=re.compile(b't+'))
    assert next(rdr) == b'a\ntt'
    assert list(rdr) == [b'b\ntttt', b'c']

    fobj = io.BytesIO(b'tttta\nttb\nttttctt')
    rdr = ReadLines(fobj, delimiter=re.compile(b't+'))
    assert next(rdr) == b'tttt'
    assert list(rdr) == [b'a\ntt', b'b\ntttt', b'ctt']

    fobj = io.BytesIO(b'')
    rdr = ReadLines(fobj, delimiter=re.compile(b't+'))
    assert list(rdr) == []


def test_greedy_regex_bin_small_buffer(): # pylint: disable=invalid-name
    """Test ReadLines with a greedy regex, binary data, and a small buffer."""
    fobj = io.BytesIO(b'a')
    rdr = ReadLines(fobj, delimiter=re.compile(b't+'), buffer_size=1)
    assert next(rdr) == b'a'
    assert list(rdr) == []

    fobj = io.BytesIO(b't')
    rdr = ReadLines(fobj, delimiter=re.compile(b't+'), buffer_size=1)
    assert next(rdr) == b't'
    assert list(rdr) == []

    fobj = io.BytesIO(b'a\nttb\nttttc')
    rdr = ReadLines(fobj, delimiter=re.compile(b't+'), buffer_size=1)
    assert next(rdr) == b'a\ntt'
    assert list(rdr) == [b'b\ntttt', b'c']

    fobj = io.BytesIO(b'tttta\nttb\nttttctt')
    rdr = ReadLines(fobj, delimiter=re.compile(b't+'), buffer_size=1)
    assert next(rdr) == b'tttt'
    assert list(rdr) == [b'a\ntt', b'b\ntttt', b'ctt']


def test_peek():
    """Test ReadLines peek."""
    fobj = io.StringIO('abc~def~ghi~jkl~')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'abc~'
    assert rdr.peek(0) == ''
    assert rdr.peek(2) == 'de'
    assert rdr.peek() == 'def~'
    assert next(rdr) == 'def~'
    assert rdr.peek(3) == 'ghi'
    assert rdr.peek(20) == 'ghi~jkl~'
    assert next(rdr) == 'ghi~'
    assert next(rdr) == 'jkl~'
    assert rdr.peek() == ''
    assert rdr.peek(10) == ''

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter='~')
    assert rdr.peek(0) == ''
    assert rdr.peek(2) == ''
    assert rdr.peek() == ''


def test_peek_small_buffer():
    """Test ReadLines peek with small buffer."""
    fobj = io.StringIO('abc~def~ghi~jkl~')
    rdr = ReadLines(fobj, delimiter='~', buffer_size=1)
    assert next(rdr) == 'abc~'
    assert rdr.peek(0) == ''
    assert rdr.peek(2) == 'de'
    assert rdr.peek() == 'def~'
    assert next(rdr) == 'def~'
    assert rdr.peek(3) == 'ghi'
    assert rdr.peek(20) == 'ghi~jkl~'
    assert next(rdr) == 'ghi~'
    assert next(rdr) == 'jkl~'
    assert rdr.peek() == ''
    assert rdr.peek(10) == ''


def test_reset():
    """Test ReadLines reset with no specified delimiter."""
    fobj = io.StringIO('abc~def~g')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    rdr.reset()
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    assert fobj.seek(2) == 2
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'c~'
    assert next(rdr) == 'def~'
    rdr.reset()
    assert next(rdr) == 'c~'
    assert next(rdr) == 'def~'

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter='~')
    assert rdr.peek() == ''
    rdr.reset()
    assert rdr.peek() == ''


def test_reset_with_delimiter():
    """Test ReadLines reset with a specified delimiter."""
    fobj = io.StringIO('abc~def~ghi!j')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    rdr.reset('!')
    assert next(rdr) == 'abc~def~ghi!'
    assert next(rdr) == 'j'
    assert fobj.seek(2) == 2
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'c~'
    assert next(rdr) == 'def~'
    rdr.reset('!')
    assert next(rdr) == 'c~def~ghi!'
    assert next(rdr) == 'j'


def test_with_delimiter():
    """Test ReadLines direct delimmiter override."""
    fobj = io.StringIO('abc~def~ghi!j')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    rdr.delimiter = '!'
    assert next(rdr) == 'ghi!'
    assert next(rdr) == 'j'
    assert fobj.seek(2) == 2
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'c~'
    assert next(rdr) == 'def~'
    rdr.delimiter = '!'
    assert next(rdr) == 'ghi!'
    assert next(rdr) == 'j'

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter='~')
    assert rdr.peek() == ''
    rdr.delimiter = '!'
    assert rdr.peek() == ''


def test_with_delimiter_small_buffer(): # pylint: disable=invalid-name
    """Test ReadLines direct delimmiter override with a small buffer."""
    fobj = io.StringIO('abc~def~ghi!j')
    rdr = ReadLines(fobj, delimiter='~', buffer_size=1)
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    rdr.delimiter = '!'
    assert next(rdr) == 'ghi!'
    assert next(rdr) == 'j'
    assert fobj.seek(2) == 2
    rdr = ReadLines(fobj, delimiter='~', buffer_size=1)
    assert next(rdr) == 'c~'
    assert next(rdr) == 'def~'
    rdr.delimiter = '!'
    assert next(rdr) == 'ghi!'
    assert next(rdr) == 'j'


def test_peek_error():
    """Test ReadLines peek with an invalid length."""
    fobj = io.StringIO('abc~def~g')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'abc~'
    with pytest.raises(ValueError):
        assert rdr.peek(-1)


def test_errors_delimiter_init():
    """Test ReadLines initialization with an invalid delimiter."""
    fobj = io.StringIO('abc~def~ghi!j')
    rdr = ReadLines(fobj, delimiter=1)
    with pytest.raises(AttributeError):
        next(rdr)


def test_errors_delimiter_reset():
    """Test ReadLines reset with an invalid delimiter."""
    fobj = io.StringIO('abc~def~ghi!j')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    rdr.reset(1)
    with pytest.raises(AttributeError):
        next(rdr)
    rdr.reset(b'~')
    with pytest.raises(TypeError):
        next(rdr)
    rdr.reset(re.compile(b'~'))
    with pytest.raises(TypeError):
        next(rdr)
    rdr.reset('~')
    assert next(rdr) == 'abc~'
    rdr.reset(1)
    with pytest.raises(AttributeError):
        rdr.peek()
    assert rdr.peek(1) == 'a'


def test_errors_delimiter_direct():
    """Test ReadLines direct delimiter override with an invalid delimiter."""
    fobj = io.StringIO('abc~def~ghi!j')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    rdr.delimiter = 1
    with pytest.raises(AttributeError):
        next(rdr)
    rdr.delimiter = b'~'     # pylint: disable=redefined-variable-type
    with pytest.raises(TypeError):
        next(rdr)
    rdr.delimiter = re.compile(b'~') # pylint: disable=redefined-variable-type
    with pytest.raises(TypeError):
        next(rdr)
    rdr.reset('~')
    assert next(rdr) == 'abc~'
    rdr.delimiter = 1
    with pytest.raises(AttributeError):
        next(rdr)
    assert rdr.peek(1) == 'd'


def test_errors_seek():
    """Test ReadLines seek with a stream that does not support it."""
    fobj = io.StringIO('abc~def~ghi!j')
    with mock.patch.object(fobj, 'seekable', return_value=False):
        rdr = ReadLines(fobj, delimiter='~')
        with pytest.raises(ValueError):
            rdr.reset()

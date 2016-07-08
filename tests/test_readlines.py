# -*- coding: utf-8 -*-
"Tests for nx_io.readlines"
import io
import re
from collections.abc import Iterator
import pytest
from nx_io import ReadLines


def get_instance(fobj, delimiter, **kwargs):
    """Instantiate the ReadLines instance"""
    if delimiter is None:
        rdr = ReadLines(fobj, **kwargs)
    else:
        rdr = ReadLines(fobj, delimiter=delimiter, **kwargs)
    assert isinstance(rdr, Iterator)
    assert delimiter is None or rdr.delimiter is delimiter
    return rdr


def null_tests(delimiter_txt, delimiter, **kwargs):
    """Run tests with no delimiter in the stream"""
    if isinstance(delimiter_txt, str):
        fobj_ctor = io.StringIO
        data_ctor = lambda x: x
    else:
        fobj_ctor = io.BytesIO
        data_ctor = lambda x: x.encode('utf-8')

    # test with no delimiter in data
    assert delimiter_txt != data_ctor('abc')
    fobj = fobj_ctor(data_ctor('abc'))
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert list(rdr) == [data_ctor('abc')]

    # test with no data
    fobj = fobj_ctor(data_ctor(''))
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert list(rdr) == []


def std_tests(delimiter_txt, delimiter=None, eats_run=False, **kwargs):
    """Run standard delimiter tests"""
    if isinstance(delimiter_txt, str):
        fobj_ctor = io.StringIO
        data_ctor = lambda x: x
    else:
        fobj_ctor = io.BytesIO
        data_ctor = lambda x: x.encode('utf-8')

    null_tests(delimiter_txt, delimiter, **kwargs)

    # test with just the delimiter as data
    fobj = fobj_ctor(delimiter_txt)
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert list(rdr) == [delimiter_txt]

    # test with multiple delimiters only as data
    fobj = fobj_ctor(delimiter_txt * 3)
    if eats_run:
        expected = [delimiter_txt * 3]
    else:
        expected = [delimiter_txt] * 3
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert list(rdr) == expected

    # test with delimiter and text where there is no delimiter at either end
    base_str = data_ctor('abc') + delimiter_txt + data_ctor('def') + \
               (delimiter_txt * 2) + data_ctor('ghi')
    if eats_run:
        expected = [data_ctor('abc') + delimiter_txt, data_ctor('def') + \
                    (delimiter_txt * 2), data_ctor('ghi')]
    else:
        expected = [data_ctor('abc') + delimiter_txt, data_ctor('def') + \
                    delimiter_txt, delimiter_txt, data_ctor('ghi')]
    fobj = fobj_ctor(base_str)
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert list(rdr) == expected

    # test with delimiter and text where the delimiter is at both ends
    base_str = delimiter_txt + base_str + delimiter_txt
    expected = [delimiter_txt] + expected
    expected[-1] = expected[-1] + delimiter_txt
    fobj = fobj_ctor(base_str)
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert list(rdr) == expected


def std_tests_strip(delimiter_txt, delimiter=None, eats_run=False, **kwargs):
    """Run standard delimiter tests with stripped delimiters"""
    if isinstance(delimiter_txt, str):
        fobj_ctor = io.StringIO
        data_ctor = lambda x: x
    else:
        fobj_ctor = io.BytesIO
        data_ctor = lambda x: x.encode('utf-8')

    kwargs['strip_delimiter'] = True

    null_tests(delimiter_txt, delimiter, **kwargs)

    # test with just the delimiter as data
    fobj = fobj_ctor(delimiter_txt)
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert list(rdr) == [data_ctor('')]

    # test with multiple delimiters only as data
    fobj = fobj_ctor(delimiter_txt * 3)
    if eats_run:
        expected = [data_ctor('')]
    else:
        expected = [data_ctor('')] * 3
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert list(rdr) == expected

    # test with delimiter and text where there is no delimiter at either end
    base_str = data_ctor('abc') + delimiter_txt + data_ctor('def') + \
               (delimiter_txt * 2) + data_ctor('ghi')
    if eats_run:
        expected = [data_ctor('abc'), data_ctor('def'), data_ctor('ghi')]
    else:
        expected = [data_ctor('abc'), data_ctor('def'), data_ctor(''),
                    data_ctor('ghi')]
    fobj = fobj_ctor(base_str)
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert list(rdr) == expected

    # test with delimiter and text where the delimiter is at both ends
    base_str = delimiter_txt + base_str + delimiter_txt
    expected = [data_ctor('')] + expected
    fobj = fobj_ctor(base_str)
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert list(rdr) == expected


def test_newline_text():
    """Test with newline delimiter - default and explicit"""
    std_tests('\n')
    std_tests('\n', '\n')
    std_tests('\n', block_size=1)
    std_tests('\n', '\n', block_size=1)


def test_newline_text_strip():
    """Test with newline delimiter - default and explicit"""
    std_tests_strip('\n')
    std_tests_strip('\n', '\n')
    std_tests_strip('\n', block_size=1)
    std_tests_strip('\n', '\n', block_size=1)


def test_single_text():
    """Test with single character delimiter"""
    std_tests('~', '~')
    std_tests('~', '~', block_size=1)


def test_single_text_strip():
    """Test with single character delimiter"""
    std_tests_strip('~', '~')
    std_tests_strip('~', '~', block_size=1)


def test_multi_text():
    """Test with multiple character delimiter"""
    std_tests('!@', '!@')
    std_tests('!@', '!@', block_size=1)


def test_multi_text_strip():
    """Test with multiple character delimiter"""
    std_tests_strip('!@', '!@')
    std_tests_strip('!@', '!@', block_size=1)


def test_newline_bin():
    """Test with newline delimiter"""
    std_tests(b'\n', b'\n')
    std_tests(b'\n', b'\n', block_size=1)


def test_newline_bin_strip():
    """Test with newline delimiter"""
    std_tests_strip(b'\n', b'\n')
    std_tests_strip(b'\n', b'\n', block_size=1)


def test_single_bin():
    """Test with single character delimiter"""
    std_tests(b'~', b'~')
    std_tests(b'~', b'~', block_size=1)


def test_single_bin_strip():
    """Test with single character delimiter"""
    std_tests_strip(b'~', b'~')
    std_tests_strip(b'~', b'~', block_size=1)


def test_multi_bin():
    """Test with multiple character delimiter"""
    std_tests(b'!@', b'!@')
    std_tests(b'!@', b'!@', block_size=1)


def test_multi_bin_strip():
    """Test with multiple character delimiter"""
    std_tests_strip(b'!@', b'!@')
    std_tests_strip(b'!@', b'!@', block_size=1)


def test_single_regex_text():
    """Test with single character regex delimiter"""
    std_tests('~', re.compile(r'~'))
    std_tests('~', re.compile(r'~'), block_size=1)


def test_single_regex_text_strip():
    """Test with single character regex delimiter"""
    std_tests_strip('~', re.compile(r'~'))
    std_tests_strip('~', re.compile(r'~'), block_size=1)


def test_multi_regex_text():
    """Test with multiple character regex delimiter"""
    std_tests('!@', re.compile(r'!@'))
    std_tests('!@', re.compile(r'!@'), block_size=1)


def test_multi_regex_text_strip():
    """Test with multiple character regex delimiter"""
    std_tests_strip('!@', re.compile(r'!@'))
    std_tests_strip('!@', re.compile(r'!@'), block_size=1)


def test_greedy_regex_text():
    """Test with greedy character regex delimiter"""
    std_tests('t', re.compile(r't+'), eats_run=True)
    std_tests('ttt', re.compile(r't+'), eats_run=True)
    std_tests('tttttt', re.compile(r't+'), eats_run=True)
    std_tests('t', re.compile(r't+'), eats_run=True, block_size=1)
    std_tests('ttt', re.compile(r't+'), eats_run=True, block_size=1)
    std_tests('tttttt', re.compile(r't+'), eats_run=True, block_size=1)


def test_greedy_regex_text_strip():
    """Test with greedy character regex delimiter"""
    std_tests_strip('t', re.compile(r't+'), eats_run=True)
    std_tests_strip('ttt', re.compile(r't+'), eats_run=True)
    std_tests_strip('tttttt', re.compile(r't+'), eats_run=True)
    std_tests_strip('t', re.compile(r't+'), eats_run=True, block_size=1)
    std_tests_strip('ttt', re.compile(r't+'), eats_run=True, block_size=1)
    std_tests_strip('tttttt', re.compile(r't+'), eats_run=True, block_size=1)


def test_single_regex_bin():
    """Test with single character regex delimiter"""
    std_tests(b'~', re.compile(b'~'))
    std_tests(b'~', re.compile(b'~'), block_size=1)


def test_single_regex_bin_strip():
    """Test with single character regex delimiter"""
    std_tests_strip(b'~', re.compile(b'~'))
    std_tests_strip(b'~', re.compile(b'~'), block_size=1)


def test_multi_regex_bin():
    """Test with multiple character regex delimiter"""
    std_tests(b'!@', re.compile(b'!@'))
    std_tests(b'!@', re.compile(b'!@'), block_size=1)


def test_multi_regex_bin_strip():
    """Test with multiple character regex delimiter"""
    std_tests_strip(b'!@', re.compile(b'!@'))
    std_tests_strip(b'!@', re.compile(b'!@'), block_size=1)


def test_greedy_regex_bin():
    """Test with greedy character regex delimiter"""
    std_tests(b't', re.compile(b't+'), eats_run=True)
    std_tests(b'ttt', re.compile(b't+'), eats_run=True)
    std_tests(b'tttttt', re.compile(b't+'), eats_run=True)
    std_tests(b't', re.compile(b't+'), eats_run=True, block_size=1)
    std_tests(b'ttt', re.compile(b't+'), eats_run=True, block_size=1)
    std_tests(b'tttttt', re.compile(b't+'), eats_run=True, block_size=1)


def test_greedy_regex_bin_strip():
    """Test with greedy character regex delimiter"""
    std_tests_strip(b't', re.compile(b't+'), eats_run=True)
    std_tests_strip(b'ttt', re.compile(b't+'), eats_run=True)
    std_tests_strip(b'tttttt', re.compile(b't+'), eats_run=True)
    std_tests_strip(b't', re.compile(b't+'), eats_run=True, block_size=1)
    std_tests_strip(b'ttt', re.compile(b't+'), eats_run=True, block_size=1)
    std_tests_strip(b'tttttt', re.compile(b't+'), eats_run=True, block_size=1)


def delimiter_tests(**kwargs):
    """Delimiter override tests"""
    fobj = io.StringIO('abc~def~gh~i!j')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    rdr.delimiter = '!'
    assert rdr.delimiter == '!'
    assert next(rdr) == 'gh~i!'
    assert next(rdr) == 'j'

    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    delimiter = re.compile('!')
    rdr.delimiter = delimiter
    assert rdr.delimiter is delimiter
    assert next(rdr) == 'gh~i!'
    assert next(rdr) == 'j'

    assert fobj.seek(2) == 2
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert next(rdr) == 'c~'
    assert next(rdr) == 'def~'
    rdr.delimiter = '!'
    assert rdr.delimiter == '!'
    assert next(rdr) == 'gh~i!'
    assert next(rdr) == 'j'

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert rdr.peek() == ''
    rdr.delimiter = '!'
    assert rdr.delimiter == '!'
    assert rdr.peek() == ''


def test_delimiter():
    """Test delimiter override"""
    delimiter_tests()
    delimiter_tests(block_size=1)


def delimiter_tests_strip(**kwargs):
    """Delimiter override tests with stripped delimiters"""
    fobj = io.StringIO('abc~def~gh~i!j')
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True, **kwargs)
    assert next(rdr) == 'abc'
    assert next(rdr) == 'def'
    rdr.delimiter = '!'
    assert rdr.delimiter == '!'
    assert next(rdr) == 'gh~i'
    assert next(rdr) == 'j'

    assert fobj.seek(2) == 2
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True, **kwargs)
    assert next(rdr) == 'c'
    assert next(rdr) == 'def'
    rdr.delimiter = '!'
    assert rdr.delimiter == '!'
    assert next(rdr) == 'gh~i'
    assert next(rdr) == 'j'


def test_delimiter_strip():
    """Test delimiter override with stripped delimiters"""
    delimiter_tests_strip()
    delimiter_tests_strip(block_size=1)


def peek_tests(**kwargs):
    """Peek tests"""
    fobj = io.StringIO('abc~def~gh!i~jkl')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert next(rdr) == 'abc~'
    assert rdr.peek(0) == ''
    assert rdr.peek(2) == 'de'
    assert rdr.peek() == 'def~'
    assert rdr.peek() == 'def~'
    assert next(rdr) == 'def~'
    assert rdr.peek() == 'gh!i~'
    rdr.delimiter = '!'
    assert rdr.peek() == 'gh!'
    rdr.delimiter = '~'
    assert rdr.peek() == 'gh!i~'
    assert rdr.peek(4) == 'gh!i'
    assert rdr.peek(20) == 'gh!i~jkl'
    assert next(rdr) == 'gh!i~'
    assert rdr.peek() == 'jkl'
    assert rdr.peek() == 'jkl'
    assert next(rdr) == 'jkl'
    assert rdr.peek() == ''
    assert rdr.peek(10) == ''

    fobj = io.StringIO('abc~def~gh!i~jkl')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert next(rdr) == 'abc~'
    assert rdr.peek(0) == ''
    assert rdr.peek(2) == 'de'
    assert rdr.peek() == 'def~'
    assert rdr.peek() == 'def~'
    assert next(rdr) == 'def~'
    rdr.delimiter = '!'
    assert rdr.peek() == 'gh!'
    rdr.delimiter = '~'
    assert rdr.peek() == 'gh!i~'
    assert rdr.peek(4) == 'gh!i'
    assert rdr.peek(20) == 'gh!i~jkl'
    assert next(rdr) == 'gh!i~'
    assert rdr.peek() == 'jkl'
    assert rdr.peek() == 'jkl'
    assert next(rdr) == 'jkl'
    assert rdr.peek() == ''
    assert rdr.peek(10) == ''

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert rdr.peek(0) == ''
    assert rdr.peek(2) == ''
    assert rdr.peek() == ''


def test_peek():
    """Test peek"""
    peek_tests()
    peek_tests(block_size=1)


def peek_tests_strip(**kwargs):
    """Peek tests with stripped delimiters"""
    fobj = io.StringIO('abc~def~gh!i~jkl~')
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True, **kwargs)
    assert next(rdr) == 'abc'
    assert rdr.peek(0) == ''
    assert rdr.peek(2) == 'de'
    assert rdr.peek() == 'def'
    assert rdr.peek() == 'def'
    assert next(rdr) == 'def'
    assert rdr.peek() == 'gh!i'
    rdr.delimiter = '!'
    assert rdr.peek() == 'gh'
    rdr.delimiter = '~'
    assert rdr.peek() == 'gh!i'
    assert rdr.peek(4) == 'gh!i'
    assert rdr.peek(20) == 'gh!i~jkl~'
    assert next(rdr) == 'gh!i'
    assert rdr.peek() == 'jkl'
    assert rdr.peek() == 'jkl'
    assert next(rdr) == 'jkl'
    assert rdr.peek() == ''
    assert rdr.peek(10) == ''

    fobj = io.StringIO('abc~def~gh!i~jkl~')
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True, **kwargs)
    assert next(rdr) == 'abc'
    assert rdr.peek(0) == ''
    assert rdr.peek(2) == 'de'
    assert rdr.peek() == 'def'
    assert rdr.peek() == 'def'
    assert next(rdr) == 'def'
    rdr.delimiter = '!'
    assert rdr.peek() == 'gh'
    rdr.delimiter = '~'
    assert rdr.peek() == 'gh!i'
    assert rdr.peek(4) == 'gh!i'
    assert rdr.peek(20) == 'gh!i~jkl~'
    assert next(rdr) == 'gh!i'
    assert rdr.peek() == 'jkl'
    assert rdr.peek() == 'jkl'
    assert next(rdr) == 'jkl'
    assert rdr.peek() == ''
    assert rdr.peek(10) == ''


def test_peek_strip():
    """Test peek with stripped delimiters"""
    peek_tests_strip()
    peek_tests_strip(block_size=1)


def test_strip_change():
    """Test changing stripped delimiters"""
    fobj = io.StringIO('abc~def~')
    rdr = ReadLines(fobj, delimiter='~')
    assert rdr.peek() == 'abc~'
    rdr.strip_delimiter = True
    assert rdr.peek() == 'abc'
    assert next(rdr) == 'abc'
    rdr.strip_delimiter = False
    assert next(rdr) == 'def~'


# pylint: disable=redefined-variable-type
def test_errors_delimiter_text():
    """Test delimiter override with an invalid delimiter for a text stream"""
    fobj = io.StringIO('abc~def~')
    rdr = ReadLines(fobj, delimiter='~')
    assert next(rdr) == 'abc~'
    rdr.delimiter = b'~'
    with pytest.raises(TypeError):
        next(rdr)
    rdr.delimiter = 1
    with pytest.raises(TypeError):
        next(rdr)
    rdr.delimiter = re.compile(b'~')
    with pytest.raises(TypeError):
        next(rdr)
    rdr.delimiter = '~'
    assert next(rdr) == 'def~'
# pylint: enable=redefined-variable-type


# pylint: disable=redefined-variable-type
def test_errors_delimiter_bin():
    """Test delimiter override with an invalid delimiter for a text stream"""
    fobj = io.BytesIO(b'abc~def~')
    rdr = ReadLines(fobj, delimiter=b'~')
    assert next(rdr) == b'abc~'
    rdr.delimiter = '~'
    with pytest.raises(TypeError):
        next(rdr)
    rdr.delimiter = 1
    with pytest.raises(TypeError):
        next(rdr)
    rdr.delimiter = re.compile('~')
    with pytest.raises(TypeError):
        next(rdr)
    rdr.delimiter = b'~'
    assert next(rdr) == b'def~'
# pylint: enable=redefined-variable-type


def test_zero_match_delimiter():
    """Test with delimiter that matches on zero-length"""
    fobj = io.StringIO('abc~def~')
    rdr = ReadLines(fobj, delimiter='')
    assert rdr.peek() == ''
    assert next(rdr) == ''
    assert next(rdr) == ''
    assert rdr.peek(3) == 'abc'

    fobj = io.StringIO('abc~def~')
    rdr = ReadLines(fobj, delimiter=re.compile('~*'))
    assert rdr.peek() == ''
    assert next(rdr) == ''
    assert next(rdr) == ''
    assert rdr.peek(3) == 'abc'

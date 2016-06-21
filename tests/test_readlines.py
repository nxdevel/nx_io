# -*- coding: utf-8 -*-
"Tests for nx_io.readlines"
# pylint: disable=too-many-statements
import io
import re
from collections.abc import Iterator
from unittest import mock
import pytest
from nx_io import ReadLines


def get_ctors(delimiter_txt):
    """Get the stream and data constructors based on the delimiter type"""
    if isinstance(delimiter_txt, str):
        return io.StringIO, lambda x: x
    return io.BytesIO, lambda x: x.encode('utf-8')


def get_instance(fobj, delimiter, **kwargs):
    """Instantiate the ReadLines instance"""
    if delimiter is None:
        return ReadLines(fobj, **kwargs)
    return ReadLines(fobj, delimiter=delimiter, **kwargs)


def run_fixed_tests(delimiter_txt, delimiter=None, eats_run=False, **kwargs):
    """Run fixed delimiter tests"""
    fobj_ctor, char_ctor = get_ctors(delimiter_txt)

    # test with no delimiter in data
    assert delimiter_txt != char_ctor('a')
    fobj = fobj_ctor(char_ctor('a'))
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == [char_ctor('a')]

    # test with just the delimiter as data
    fobj = fobj_ctor(delimiter_txt)
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == [delimiter_txt]

    # test with delimiter and text where there is no delimiter at neither end
    base_str = char_ctor('a') + delimiter_txt + char_ctor('b') + \
               (delimiter_txt * 2) + char_ctor('c')
    if eats_run:
        expected = [char_ctor('a') + delimiter_txt, char_ctor('b') + \
                    (delimiter_txt * 2), char_ctor('c')]
    else:
        expected = [char_ctor('a') + delimiter_txt, char_ctor('b') + \
                    delimiter_txt, delimiter_txt, char_ctor('c')]
    fobj = fobj_ctor(base_str)
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == expected

    # test with delimiter and text where the delimiter is at both ends
    base_str = delimiter_txt + base_str + delimiter_txt
    expected = [delimiter_txt] + expected
    expected[-1] = expected[-1] + delimiter_txt
    fobj = fobj_ctor(base_str)
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == expected

    # test with no data
    fobj = fobj_ctor(char_ctor(''))
    rdr = get_instance(fobj, delimiter, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == []


def run_fixed_tests_strip(delimiter_txt, delimiter=None, eats_run=False,
                          **kwargs):
    """Run fixed delimiter tests with stripped delimiters"""
    fobj_ctor, char_ctor = get_ctors(delimiter_txt)

    # test with no delimiter in data
    assert delimiter_txt != char_ctor('a')
    fobj = fobj_ctor(char_ctor('a'))
    rdr = get_instance(fobj, delimiter, strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == [char_ctor('a')]

    # test with just the delimiter as data
    fobj = fobj_ctor(delimiter_txt)
    rdr = get_instance(fobj, delimiter, strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == [char_ctor('')]

    # test with delimiter and text where there is no delimiter at neither end
    base_str = char_ctor('a') + delimiter_txt + char_ctor('b') + \
               (delimiter_txt * 2) + char_ctor('c')
    if eats_run:
        expected = [char_ctor('a'), char_ctor('b'), char_ctor('c')]
    else:
        expected = [char_ctor('a'), char_ctor('b'), char_ctor(''),
                    char_ctor('c')]
    fobj = fobj_ctor(base_str)
    rdr = get_instance(fobj, delimiter, strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == expected

    # test with delimiter and text where the delimiter is at both ends
    base_str = delimiter_txt + base_str + delimiter_txt
    expected = [char_ctor('')] + expected
    fobj = fobj_ctor(base_str)
    rdr = get_instance(fobj, delimiter, strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == expected

    # test with no data
    fobj = fobj_ctor(char_ctor(''))
    rdr = get_instance(fobj, delimiter, strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == []


def test_newline_text():
    """Test with newline delimiter - default and explicit"""
    run_fixed_tests('\n')
    run_fixed_tests('\n', '\n')
    run_fixed_tests('\n', block_size=1)
    run_fixed_tests('\n', '\n', block_size=1)


def test_newline_text_strip():
    """Test with newline delimiter - default and explicit"""
    run_fixed_tests_strip('\n')
    run_fixed_tests_strip('\n', '\n')
    run_fixed_tests_strip('\n', block_size=1)
    run_fixed_tests_strip('\n', '\n', block_size=1)


def test_single_text():
    """Test with single character delimiter"""
    run_fixed_tests('~', '~')
    run_fixed_tests('~', '~', block_size=1)


def test_single_text_strip():
    """Test with single character delimiter"""
    run_fixed_tests_strip('~', '~')
    run_fixed_tests_strip('~', '~', block_size=1)


def test_multi_text():
    """Test with multiple character delimiter"""
    run_fixed_tests('!@', '!@')
    run_fixed_tests('!@', '!@', block_size=1)


def test_multi_text_strip():
    """Test with multiple character delimiter"""
    run_fixed_tests_strip('!@', '!@')
    run_fixed_tests_strip('!@', '!@', block_size=1)


def test_newline_bin():
    """Test with newline delimiter"""
    run_fixed_tests(b'\n', b'\n')
    run_fixed_tests(b'\n', b'\n', block_size=1)


def test_newline_bin_strip():
    """Test with newline delimiter"""
    run_fixed_tests_strip(b'\n', b'\n')
    run_fixed_tests_strip(b'\n', b'\n', block_size=1)


def test_single_regex_text():
    """Test with single character regex delimiter"""
    run_fixed_tests('~', re.compile(r'~'))
    run_fixed_tests('~', re.compile(r'~'), block_size=1)


def test_single_regex_text_strip():
    """Test with single character regex delimiter"""
    run_fixed_tests_strip('~', re.compile(r'~'))
    run_fixed_tests_strip('~', re.compile(r'~'), block_size=1)


def test_multi_regex_text():
    """Test with multiple character regex delimiter"""
    run_fixed_tests('!@', re.compile(r'!@'))
    run_fixed_tests('!@', re.compile(r'!@'), block_size=1)


def test_multi_regex_text_strip():
    """Test with multiple character regex delimiter"""
    run_fixed_tests_strip('!@', re.compile(r'!@'))
    run_fixed_tests_strip('!@', re.compile(r'!@'), block_size=1)


def test_greedy_regex_text():
    """Test with greedy character regex delimiter"""
    run_fixed_tests('t', re.compile(r't+'), eats_run=True)
    run_fixed_tests('ttt', re.compile(r't+'), eats_run=True)
    run_fixed_tests('tttttt', re.compile(r't+'), eats_run=True)
    run_fixed_tests('t', re.compile(r't+'), eats_run=True, block_size=1)
    run_fixed_tests('ttt', re.compile(r't+'), eats_run=True, block_size=1)
    run_fixed_tests('tttttt', re.compile(r't+'), eats_run=True, block_size=1)


def test_greedy_regex_text_strip():
    """Test with greedy character regex delimiter"""
    run_fixed_tests_strip('t', re.compile(r't+'), eats_run=True)
    run_fixed_tests_strip('ttt', re.compile(r't+'), eats_run=True)
    run_fixed_tests_strip('tttttt', re.compile(r't+'), eats_run=True)
    run_fixed_tests_strip('t', re.compile(r't+'), eats_run=True, block_size=1)
    run_fixed_tests_strip('ttt', re.compile(r't+'), eats_run=True,
                          block_size=1)
    run_fixed_tests_strip('tttttt', re.compile(r't+'), eats_run=True,
                          block_size=1)


def test_single_bin():
    """Test with single character delimiter"""
    run_fixed_tests(b'~', b'~')
    run_fixed_tests(b'~', b'~', block_size=1)


def test_single_bin_strip():
    """Test with single character delimiter"""
    run_fixed_tests_strip(b'~', b'~')
    run_fixed_tests_strip(b'~', b'~', block_size=1)


def test_multi_bin():
    """Test with multiple character delimiter"""
    run_fixed_tests(b'!@', b'!@')
    run_fixed_tests(b'!@', b'!@', block_size=1)


def test_multi_bin_strip():
    """Test with multiple character delimiter"""
    run_fixed_tests_strip(b'!@', b'!@')
    run_fixed_tests_strip(b'!@', b'!@', block_size=1)


def test_single_regex_bin():
    """Test with single character regex delimiter"""
    run_fixed_tests(b'~', re.compile(b'~'))
    run_fixed_tests(b'~', re.compile(b'~'), block_size=1)


def test_single_regex_bin_strip():
    """Test with single character regex delimiter"""
    run_fixed_tests_strip(b'~', re.compile(b'~'))
    run_fixed_tests_strip(b'~', re.compile(b'~'), block_size=1)


def test_multi_regex_bin():
    """Test with multiple character regex delimiter"""
    run_fixed_tests(b'!@', re.compile(b'!@'))
    run_fixed_tests(b'!@', re.compile(b'!@'), block_size=1)


def test_multi_regex_bin_strip():
    """Test with multiple character regex delimiter"""
    run_fixed_tests_strip(b'!@', re.compile(b'!@'))
    run_fixed_tests_strip(b'!@', re.compile(b'!@'), block_size=1)


def test_greedy_regex_bin():
    """Test with greedy character regex delimiter"""
    run_fixed_tests(b't', re.compile(b't+'), eats_run=True)
    run_fixed_tests(b'ttt', re.compile(b't+'), eats_run=True)
    run_fixed_tests(b'tttttt', re.compile(b't+'), eats_run=True)
    run_fixed_tests(b't', re.compile(b't+'), eats_run=True, block_size=1)
    run_fixed_tests(b'ttt', re.compile(b't+'), eats_run=True, block_size=1)
    run_fixed_tests(b'tttttt', re.compile(b't+'), eats_run=True, block_size=1)


def test_greedy_regex_bin_strip():
    """Test with greedy character regex delimiter"""
    run_fixed_tests_strip(b't', re.compile(b't+'), eats_run=True)
    run_fixed_tests_strip(b'ttt', re.compile(b't+'), eats_run=True)
    run_fixed_tests_strip(b'tttttt', re.compile(b't+'), eats_run=True)
    run_fixed_tests_strip(b't', re.compile(b't+'), eats_run=True, block_size=1)
    run_fixed_tests_strip(b'ttt', re.compile(b't+'), eats_run=True,
                          block_size=1)
    run_fixed_tests_strip(b'tttttt', re.compile(b't+'), eats_run=True,
                          block_size=1)


def test_bin_normalization():
    """Test normalization on binary data"""
    # normalization is ignored for binary data
    run_fixed_tests(b'~', b'~', form='NFC')
    run_fixed_tests(b'~', b'~', form='NFKC')
    run_fixed_tests(b'~', b'~', form='NFD')
    run_fixed_tests(b'~', b'~', form='NFKD')


def peek_tests(**kwargs):
    """Peek tests"""
    fobj = io.StringIO('abc~def~ghi~jkl~')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert isinstance(rdr, Iterator)
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
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert isinstance(rdr, Iterator)
    assert rdr.peek(0) == ''
    assert rdr.peek(2) == ''
    assert rdr.peek() == ''


def test_peek():
    """Test peek"""
    peek_tests()
    peek_tests(block_size=1)


def peek_tests_strip(**kwargs):
    """Peek tests with stripped delimiters"""
    fobj = io.StringIO('abc~def~ghi~jkl~')
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert next(rdr) == 'abc'
    assert rdr.peek(0) == ''
    assert rdr.peek(2) == 'de'
    assert rdr.peek() == 'def'
    assert next(rdr) == 'def'
    assert rdr.peek(3) == 'ghi'
    assert rdr.peek(20) == 'ghi~jkl~'
    assert next(rdr) == 'ghi'
    assert next(rdr) == 'jkl'
    assert rdr.peek() == ''
    assert rdr.peek(10) == ''


def test_peek_strip():
    """Test peek with stripped delimiters"""
    peek_tests_strip()
    peek_tests_strip(block_size=1)


def test_reset():
    """Test reset"""
    fobj = io.StringIO('abc~def~g')
    rdr = ReadLines(fobj, delimiter='~')
    assert isinstance(rdr, Iterator)
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    rdr.reset()
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'

    assert fobj.seek(2) == 2
    rdr = ReadLines(fobj, delimiter='~')
    assert isinstance(rdr, Iterator)
    assert next(rdr) == 'c~'
    assert next(rdr) == 'def~'
    rdr.reset()
    assert next(rdr) == 'c~'
    assert next(rdr) == 'def~'

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter='~')
    assert isinstance(rdr, Iterator)
    assert rdr.peek() == ''
    rdr.reset()
    assert rdr.peek() == ''


def test_reset_strip():
    """Test reset with stripped delimiters"""
    fobj = io.StringIO('abc~def~g')
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True)
    assert isinstance(rdr, Iterator)
    assert next(rdr) == 'abc'
    assert next(rdr) == 'def'
    rdr.reset()
    assert next(rdr) == 'abc'
    assert next(rdr) == 'def'

    assert fobj.seek(2) == 2
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True)
    assert isinstance(rdr, Iterator)
    assert next(rdr) == 'c'
    assert next(rdr) == 'def'
    rdr.reset()
    assert next(rdr) == 'c'
    assert next(rdr) == 'def'


def delimiter_tests(**kwargs):
    """Delimiter override tests"""
    fobj = io.StringIO('abc~def~gh~i!j')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert isinstance(rdr, Iterator)
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    rdr.delimiter = '!'
    assert next(rdr) == 'gh~i!'
    assert next(rdr) == 'j'

    assert fobj.seek(2) == 2
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert isinstance(rdr, Iterator)
    assert next(rdr) == 'c~'
    assert next(rdr) == 'def~'
    rdr.delimiter = '!'
    assert next(rdr) == 'gh~i!'
    assert next(rdr) == 'j'

    fobj = io.StringIO('')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert isinstance(rdr, Iterator)
    assert rdr.peek() == ''
    rdr.delimiter = '!'
    assert rdr.peek() == ''


def test_delimiter():
    """Test delimiter override"""
    delimiter_tests()
    delimiter_tests(block_size=1)


def delimiter_tests_strip(**kwargs):
    """Delimiter override tests with stripped delimiters"""
    fobj = io.StringIO('abc~def~gh~i!j')
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert next(rdr) == 'abc'
    assert next(rdr) == 'def'
    rdr.delimiter = '!'
    assert next(rdr) == 'gh~i'
    assert next(rdr) == 'j'

    assert fobj.seek(2) == 2
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert next(rdr) == 'c'
    assert next(rdr) == 'def'
    rdr.delimiter = '!'
    assert next(rdr) == 'gh~i'
    assert next(rdr) == 'j'


def test_delimiter_strip():
    """Test delimiter override with stripped delimiters"""
    delimiter_tests_strip()
    delimiter_tests_strip(block_size=1)


def test_strip_change():
    """Test changing stripped delimiters"""
    fobj = io.StringIO('abc~def~')
    rdr = ReadLines(fobj, delimiter='~')
    assert isinstance(rdr, Iterator)
    assert rdr.peek() == 'abc~'
    rdr.strip_delimiter = True
    assert rdr.peek() == 'abc'
    assert next(rdr) == 'abc'
    rdr.strip_delimiter = False
    assert next(rdr) == 'def~'


def test_peek_error():
    """Test peek with an invalid length"""
    fobj = io.StringIO('abc~def~g')
    rdr = ReadLines(fobj, delimiter='~')
    assert isinstance(rdr, Iterator)
    assert next(rdr) == 'abc~'
    with pytest.raises(ValueError):
        assert rdr.peek(-1)


def test_errors_delimiter_init():
    """Test initialization with an invalid delimiter"""
    fobj = io.StringIO('')
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter='')
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=b'')
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=b'~')
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=1)
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=re.compile(''))
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=re.compile(b''))
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=re.compile('~*'))
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=re.compile(b'~'))
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=re.compile(b'~*'))

    fobj = io.BytesIO(b'')   # pylint: disable=redefined-variable-type
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter='')
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=b'')
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter='~')
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=1)
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=re.compile(''))
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=re.compile(b''))
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=re.compile('~*'))
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=re.compile('~'))
    with pytest.raises(ValueError):
        ReadLines(fobj, delimiter=re.compile(b'~*'))


def test_errors_delimiter():
    """Test delimiter override with an invalid delimiter"""
    fobj = io.StringIO('abc~def~ghi!j')
    rdr = ReadLines(fobj, delimiter='~')
    assert isinstance(rdr, Iterator)
    assert next(rdr) == 'abc~'
    assert next(rdr) == 'def~'
    with pytest.raises(ValueError):
        rdr.delimiter = ''
    with pytest.raises(ValueError):
        rdr.delimiter = b''  # pylint: disable=redefined-variable-type
    with pytest.raises(ValueError):
        rdr.delimiter = b'~'
    with pytest.raises(ValueError):
        rdr.delimiter = 1
    with pytest.raises(ValueError):
        rdr.delimiter = re.compile('')
    with pytest.raises(ValueError):
        rdr.delimiter = re.compile(b'')
    with pytest.raises(ValueError):
        rdr.delimiter = re.compile('~*')
    with pytest.raises(ValueError):
        rdr.delimiter = re.compile(b'~')
    with pytest.raises(ValueError):
        rdr.delimiter = re.compile(b'~*')
    rdr.delimiter = '!'
    assert next(rdr) == 'ghi!'

    fobj = io.BytesIO(b'abc~def~ghi!j') # pylint: disable=redefined-variable-type
    rdr = ReadLines(fobj, delimiter=b'~')
    assert isinstance(rdr, Iterator)
    assert next(rdr) == b'abc~'
    assert next(rdr) == b'def~'
    with pytest.raises(ValueError):
        rdr.delimiter = ''
    with pytest.raises(ValueError):
        rdr.delimiter = b''
    with pytest.raises(ValueError):
        rdr.delimiter = '~'
    with pytest.raises(ValueError):
        rdr.delimiter = 1
    with pytest.raises(ValueError):
        rdr.delimiter = re.compile('')
    with pytest.raises(ValueError):
        rdr.delimiter = re.compile(b'')
    with pytest.raises(ValueError):
        rdr.delimiter = re.compile('~*')
    with pytest.raises(ValueError):
        rdr.delimiter = re.compile('~')
    with pytest.raises(ValueError):
        rdr.delimiter = re.compile(b'~*')
    rdr.delimiter = b'!'
    assert next(rdr) == b'ghi!'


def test_errors_seek():
    """Test seek with a stream that does not support it"""
    fobj = io.StringIO('abc~def~ghi!j')
    with mock.patch.object(fobj, 'seekable', return_value=False):
        rdr = ReadLines(fobj, delimiter='~')
        assert isinstance(rdr, Iterator)
        with pytest.raises(ValueError):
            rdr.reset()


def form_tests(**kwargs):
    """Tests for normalization"""
    fobj = io.StringIO('abco\u0308~abc')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6', '~abc']

    fobj = io.StringIO('bco\u0308~abc')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6', '~abc']

    fobj = io.StringIO('abc\u00f6~abc')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6', '~abc']

    fobj = io.StringIO('bc\u00f6~abc')
    rdr = ReadLines(fobj, delimiter='~', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6~', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFD', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFC', **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6', '~abc']


def test_forms():
    """Test form normalization"""
    form_tests()
    form_tests(block_size=1)


def form_tests_strip(**kwargs):
    """Tests for normalization with stripped delimiters"""
    fobj = io.StringIO('abco\u0308~abc')
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFD', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFC', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFD',
                    strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFC',
                    strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFD', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFC', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc', '~abc']

    fobj = io.StringIO('bco\u0308~abc')
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFD', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFC', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFD',
                    strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFC',
                    strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFD', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFC', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc', '~abc']

    fobj = io.StringIO('abc\u00f6~abc')
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFD', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFC', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFD',
                    strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFC',
                    strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFD', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFC', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['abc', '~abc']

    fobj = io.StringIO('bc\u00f6~abc')
    rdr = ReadLines(fobj, delimiter='~', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFD', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='~', form='NFC', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6', 'abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFD',
                    strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='o\u0308', form='NFC',
                    strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc\u00f6~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', strip_delimiter=True, **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc', '~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFD', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bco\u0308~abc']
    assert fobj.seek(0) == 0
    rdr = ReadLines(fobj, delimiter='\u00f6', form='NFC', strip_delimiter=True,
                    **kwargs)
    assert isinstance(rdr, Iterator)
    assert list(rdr) == ['bc', '~abc']


def test_forms_strip():
    """Test form normalization with delimiters stripped"""
    form_tests_strip()
    form_tests_strip(block_size=1)


def test_combining_scan():
    """Test the combining character scanning of streams"""
    # inducing the scanner to execute multiple reads
    fobj = io.StringIO('a\n\u0308')
    with mock.patch('nx_io.readlines.SCAN_SIZE', return_value=1):
        rdr = ReadLines(fobj, form='NFC', block_size=1)
        assert isinstance(rdr, Iterator)
        assert list(rdr) == ['a\n', '\u0308']

    # inducing the scanner to read extra data but not enough to satisfy
    # a subsequent read
    fobj = io.StringIO('abc\n\u0308')
    with mock.patch('nx_io.readlines.SCAN_SIZE', return_value=1):
        rdr = ReadLines(fobj, form='NFC', block_size=3)
        assert isinstance(rdr, Iterator)
        assert list(rdr) == ['abc\n', '\u0308']


def test_form_errors():
    """Test normalization form errors"""
    with pytest.raises(ValueError):
        ReadLines(io.StringIO(''), form='nfc')

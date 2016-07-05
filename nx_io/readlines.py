# -*- coding: utf-8 -*-
"""Object to read lines from a stream using an arbitrary delimiter"""
from math import ceil
from collections import namedtuple


__all__ = ['ReadLines']


# the default amount of data to read on a buffer full
DEFAULT_BLOCK_SIZE = 1048756


Stream = namedtuple('Stream', ('read', 'block_size'))


Buffer = namedtuple('Buffer', ('buf', 'idx', 'eof'))


class ReadLines:
    """Iterator to read lines from a stream using an arbitrary delimiter"""
    def peek(self, size=None):
        """Peek into the stream/buffer without advancing the current state

        Arguments
        ---------
        size : integer
               The amount of data to read

        Returns
        -------
        If size is specified then the returned amount is the same as if the
        stream were being peek()'ed directly, i.e. the amount will include upto
        the amount requested depending on how much data there is.
        If size is omitted or None, the forth-coming delimited line will be
        returned.
        """
        if size is None:                # request to peek at a line
            try:
                return self._get_line(consume=False)
            except StopIteration:
                return ''

        if size >= 0:
            buf, idx, eof = self._buffer

            if not eof and len(buf) - idx < size:
                # the stream is not known to be exhausted and the buffer will
                # not satisfy the request

                read, block_size = self._stream

                # truncate the buffer
                buf, idx = buf[idx:], 0

                amount_needed = size - len(buf)

                while amount_needed > 0:

                    # determine how much data to read(in multiples of the block
                    # size) in order to satisfy the request
                    to_read = ceil(amount_needed / block_size) * block_size

                    data = read(to_read)
                    if not data:
                        eof = True
                        break

                    # more data has been received so it is added to the buffer
                    buf += data

                    amount_needed = max(size - len(buf), 0)

                # buffer was modified
                self._buffer = Buffer(buf, idx, eof)

            return buf[idx:idx + size]

        raise ValueError('invalid size: {}'.format(size))

    def __iter__(self):
        return self

    def __next__(self):
        return self._get_line(consume=True)

    def _get_line(self, consume):
        """Get the next/cached line

        Arguments
        ---------
        consume : boolean
                  Indicator on whether the line is to be consumed

        Returns
        -------
        The next line in the buffer/stream if no line has been cached or the
        cached line from a previous call

        This call will raise a StreamExhausted exception if there are no cached
        lines available and there are no more lines to be read.
        """
        line = self._line
        if line is not None:            # a cached line is available
            if consume:
                # if consume is True then ensure that the next call will get
                # the next line
                self._line = None
            return line[:self._delimiter_pos] if self.strip_delimiter else line

        # get the next line from the buffer/stream
        line, delimiter_pos = self._get_next_line()
        if not consume:
            # cache the line
            self._line, self._delimiter_pos = line, delimiter_pos
        return line[:delimiter_pos] if self.strip_delimiter else line

    def _get_next_line(self):
        """Get the next line

        Returns
        -------
        A two-tuple of the next line in the stream and the index of where,
        within the line, the delimiter starts if it is present or the length of
        the line if it does not.

        This call will raise a StreamExhausted exception if there are no more
        lines to be read.
        """
        read, block_size = self._stream
        buf, idx, eof = self._buffer
        delimiter = self._delimiter

        # searching starts at the idx
        search_idx = idx

        while True:

            # The delimiter is either str/bytes or a regex-like object
            if isinstance(delimiter, (str, bytes)):
                delimiter_start = buf.find(delimiter, search_idx)

                if delimiter_start != -1:
                    # the length of the delimiter is added to where the
                    # delimiter starts to get the index of where it ends and
                    # the index attribute is set to indicate where in the
                    # buffer the next line begins
                    end = delimiter_start + len(delimiter)
                    self._buffer = Buffer(buf, end, eof)
                    return buf[idx:end], delimiter_start - idx

                # a match was not found but if the delimiter is more than one
                # character then the delimiter could have been split so an
                # offset is provided to start the search within the existing
                # buffer
                search_offset = len(delimiter) - 1

            else:
                result = delimiter.search(buf, # pylint: disable=no-member
                                          search_idx)
                if result:
                    delimiter_start = result.start()
                    end = result.end()

                    if end != result.endpos:
                        # if the match is not at the end of the buffer then it
                        # is an exact match regardless of whether the regex is
                        # greedy

                        # the index attribute is set to indicate where in the
                        # buffer the next line begins
                        self._buffer = Buffer(buf, end, eof)
                        return buf[idx:end], delimiter_start - idx

                    # if the match is at the end of the buffer then reading
                    # more could result in a better match if the regex is
                    # greedy

                    # since a match was found, searching can begin at the point
                    # where the match started
                    search_offset = end - delimiter_start

                else:

                    # the delimiter was not found in the buffer
                    delimiter_start = -1

                    # the buffer needs to be scanned from the beginning
                    search_offset = len(buf) - idx

            if eof:                     # no more data is forth-coming
                # ensure that another call will result in no search being
                # performed
                end = len(buf)
                self._buffer = Buffer(buf[end:], 0, eof)
                if idx < end:
                    # there is unconsumed data in the buffer

                    # it is possible that a match exists but an attempt is
                    # being made to find a better match
                    if delimiter_start == -1:
                        # if there was no previous delimiter match then the
                        # final line contains no delimiter
                        delimiter_start = end

                    return buf[idx:end], delimiter_start - idx

                raise StopIteration

            # truncate the buffer
            buf, idx = buf[idx:], 0

            # search should commence at the where the buffer ends minus any
            # offset that was previously provided
            search_idx = len(buf) - search_offset

            if search_idx < 0:
                # ensure the search index does not start on a negative value
                search_idx = 0

            # get more data
            more = read(block_size)
            buf += more
            if not more:
                eof = True

    @property
    def delimiter(self):
        """Get delimiter"""
        return self._delimiter
    _delimiter = None

    @delimiter.setter
    def delimiter(self, value):
        """Set delimiter"""
        if isinstance(value, (str, bytes)):
            if not value:
                raise ValueError('non-zero match delimiter is required')
            if isinstance(value, bytes) != isinstance(self._buffer.buf, bytes):
                raise ValueError('delimiter type must match stream mode')
        elif hasattr(value, 'search'):
            test_text = b'' if isinstance(self._buffer.buf, bytes) else ''
            try:
                result = value.search(test_text)
            except TypeError:
                raise ValueError('delimiter type must match stream mode')
            if result and result.start() == result.end():
                raise ValueError('non-zero match delimiter is required')
        else:
            raise ValueError('unknown type of delimiter: {}'
                             .format(repr(value)))
        self._delimiter = value

    def __init__(self, fobj, *, delimiter='\n', strip_delimiter=False,
                 block_size=DEFAULT_BLOCK_SIZE):
        """
        Arguments
        ----------
        fobj            : stream
                          Stream from which to read
        delimiter       : str, bytes, or regex
                          Criteria for how a line is terminated
        strip_delimiter : boolean
                          Indicator on whether the delimiter should be included
                          in a returned line
        block_size      : integer
                          Size to use for reading from the stream

        Attributes
        ----------
        delimiter       : str, bytes, or regex
                          Criteria for how a line is terminated
        strip_delimiter : boolean
                          Indicator on whether the delimiter should be included
                          in a returned line

        The stream must be opened for reading and must be blocking.

        The *delimiter* type should match the mode of *fobj*. If *delimiter* is
        str/bytes then the find() method of the internal buffer will be used.
        If *delimiter* is regex then its search() method will be used.

        The *delimiter* should match one or more characters.
        """
        read = fobj.read
        buf = read(block_size)
        self._stream = Stream(read, block_size)
        self._buffer = Buffer(buf, 0, not buf)
        self.delimiter = delimiter
        self.strip_delimiter = strip_delimiter
        self._line, self._delimiter_pos = None, None

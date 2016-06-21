# -*- coding: utf-8 -*-
"""Object to read lines from a stream using an arbitrary delimiter"""
import math
from itertools import filterfalse
from unicodedata import combining, normalize


__all__ = ['ReadLines']


# the default amount of data to read on a buffer full
DEFAULT_BLOCK_SIZE = 1048756


# the size to read when scanning for a non-combining character
SCAN_SIZE = 512


class _Reader:
    """Stream reader that accounts for combining characters"""
    def reset(self):
        """Reset to the initialized state"""
        start = self._start
        if start is None:
            raise ValueError('reset on non-seekable steam object')
        fobj = self._fobj
        fobj.seek(start)
        self.__init__(fobj, form=self._form)

    def read(self, size):
        """Read data

        Arguments
        ---------
        size : integer
               The amount of data to read

        Returns
        -------
        The data read from the stream

        As with any stream, the length of the return may be less than the
        amount requested with a blank returned for EOF.

        Due to the nature and handling of combining characters, when a
        normalization form is specified, the return for reads from a text-mode
        stream may have more or less data than requested. Depending on the
        disposition of the stream, it is possible that the results could
        contain the entire contents of the stream regardless of how much data
        was requested.
        """
        form = self._form
        if form is None:
            # no form means a direct read
            return self._fobj.read(size)

        fobj_read = self._fobj.read

        prev_extra = self._prev_extra
        if prev_extra:
            # include the previous extra data with the buffer
            buf = prev_extra
            buf_length = len(buf)
            if buf_length < size:
                # only execute a read if the previous extra data cannot
                # satisify the request
                buf += fobj_read(size - buf_length)
        else:
            buf = fobj_read(size)

        if isinstance(buf, bytes):
            return buf

        scan_size = SCAN_SIZE

        # retrieve extra data to ensure the boundary does not split combined
        # characters
        extra = fobj_read(scan_size)

        while extra:

            # find the first occurrence of a non-combing character in the extra
            # data
            result = next(filterfalse(lambda x: combining(x[1]),
                                      enumerate(extra)), None)

            if result is not None:
                # a non-combining character was found in the extra data
                idx = result[0]

                # add all of the extra data upto the non-combining character
                # to the buffer
                buf += extra[:idx]

                # store all of the extra data from the non-combining character
                # on so it will be added to the buffer on the next read
                self._prev_extra = extra[idx:]

                break

            # if there is no occurrence of a non-combining character in the
            # extra data then add the extra data to the buffer and try again
            buf += extra
            extra = fobj_read(scan_size)

        else:
            self._prev_extra = ''       # no extra data was read or it was
                                        # already added to the buffer

        return normalize(form, buf)

    def __init__(self, fobj, form=None):
        """
        Arguments
        ---------
        fobj      : stream
                    The stream from which to read
        form      : string
                    The normalization form to use

        The stream must be opened for reading and must be in blocking mode.

        If form is specified then the returned data is normalized with that
        form.
        """
        self._fobj = fobj
        if form not in {'NFC', 'NFKC', 'NFD', 'NFKD', None}:
            raise ValueError('invalid normalization form')
        self._form = form
        self._start = fobj.tell() if fobj.seekable() else None
        self._prev_extra = ''


class StreamExhausted(Exception): # pylint: disable=too-few-public-methods
    """Exception indicating the stream has been exhausted of lines"""
    pass


class ReadLines:        # pylint: disable=too-many-instance-attributes
    """Iterator to read lines from a stream using an arbitrary delimiter"""
    def reset(self):
        """Reset to the initialized state"""
        fobj = self._fobj
        fobj.reset()
        self.__init__(fobj, delimiter=self.delimiter, form=None,
                      strip_delimiter=self.strip_delimiter,
                      block_size=self._block_size)

    def peek(self, size=None):
        """Peek into the stream/buffer without advancing the current read
        state

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
            except StreamExhausted:
                return ''

        if size < 0:
            raise ValueError('invalid size: {}'.format(size))

        if size == 0:
            return ''

        # truncate the buffer
        buf, eof = self._buf[self._idx:], self._eof

        fobj_read = self._fobj.read
        block_size = self._block_size

        # determine if more data is needed to satisfy the request
        extra_needed = size - len(buf)

        # while the steam has not been exhausted and more data is needed...
        while not eof and extra_needed:

            # determine how much data to read(in multiples of the block
            # size) in order to satisfy the request
            to_read = math.ceil(extra_needed / block_size) * block_size

            tmp_buf = fobj_read(to_read)
            if tmp_buf:
                # more data has been received so it is added to the buffer
                buf += tmp_buf

                # determine if the read has satisfied the request
                extra_needed = size - len(buf)

            else:
                self._eof = eof = True # no data has been received so EOF

        # buffer was truncated
        self._buf, self._idx = buf, 0
        return buf[:size]

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self._get_line(consume=True)
        except StreamExhausted:
            raise StopIteration

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
        if line is not None:
            # a cached line is available
            delimiter_pos = self._delimiter_pos

            if consume:
                # if consume is True then ensure that the next call will get
                # the next line
                self._line, self._delimiter_pos = None, None

            if self.strip_delimiter:
                return line[:delimiter_pos]
            return line

        # get the next line from the buffer/stream
        line, delimiter_pos = self._get_next_line()

        if consume:
            # if consume is True then this line will not be cached
            self._line, self._delimiter_pos = None, None
        else:
            # cache the line
            self._line, self._delimiter_pos = line, delimiter_pos

        if self.strip_delimiter:
            return line[:delimiter_pos]
        return line

    def _get_next_line(self):           # pylint: disable=too-many-branches
        """Get the next line

        Returns
        -------
        A two-tuple of the next line in the stream and the index of where,
        within the line, the delimiter starts if it is present or the length of
        the line if it does not.

        This call will raise a StreamExhausted exception if there are no more
        lines to be read.
        """
        fobj_read = self._fobj.read
        block_size = self._block_size
        delimiter = self.delimiter
        buf, idx, eof = self._buf, self._idx, self._eof

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
                    self._idx = end = delimiter_start + len(delimiter)
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
                        self._idx = end
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
                self._buf, self._idx = self._empty_buf, 0

                end = len(buf)
                if idx < end:
                    # there is unconsumed data in the buffer

                    # it is possible that a match exists but an attempt is
                    # being made to find a better match
                    if delimiter_start == -1:
                        # if there was no previous delimiter match then the
                        # final line contains no delimiter
                        delimiter_start = end

                    return buf[idx:end], delimiter_start - idx

                raise StreamExhausted

            # truncate the buffer
            buf, idx = buf[idx:], 0

            # search should commence at the where the buffer ends minus any
            # offset that was previously provided
            search_idx = len(buf) - search_offset

            if search_idx < 0:
                # ensure the search index does not start on a negative value
                search_idx = 0

            # get more data
            more = fobj_read(block_size)
            buf += more
            self._buf = buf
            if not more:
                self._eof = eof = True

    @property
    def delimiter(self):
        """Delimiter getter"""
        return self._delimiter
    _delimiter = None

    @delimiter.setter
    def delimiter(self, value):
        """Delimiter setter"""
        if isinstance(value, (str, bytes)):
            if not value:
                raise ValueError('non-zero match delimiter is required')
            if isinstance(value, bytes) != self._binary:
                raise ValueError('delimiter type must match stream mode')
        elif hasattr(value, 'search'):
            test_text = b'test' if self._binary else 'test'
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

    def __init__(self, fobj, *, delimiter='\n', form=None,
                 strip_delimiter=False, block_size=DEFAULT_BLOCK_SIZE):
        """
        Arguments
        ----------
        fobj            : stream
                          Stream from which to read
        delimiter       : str, bytes, or regex
                          Criteria for how a line is terminated
        form            : str
                          The normalizion form to use
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

        The stream must be opened for reading and must be in blocking mode.

        If form is specified then that form is used for all lines.

        The *delimiter* type should match the mode of *fobj*. If *delimiter* is
        str/bytes then the find() method of the internal buffer will be used.
        If *delimiter* is regex then its search() method will be used.

        The *delimiter* should match one or more characters.
        """
        if not isinstance(fobj, _Reader):
            fobj = _Reader(fobj, form)
        buf = fobj.read(block_size)
        if isinstance(buf, bytes):
            self._binary = True
            self._empty_buf = b''
        else:
            self._binary = False
            self._empty_buf = '' # pylint: disable=redefined-variable-type
        self._fobj = fobj
        self.strip_delimiter = strip_delimiter
        self._block_size = block_size
        self.delimiter = delimiter
        self._buf, self._idx, self._eof = buf, 0, not buf
        self._line, self._delimiter_pos = None, None

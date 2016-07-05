# -*- coding: utf-8 -*-
"""Object to read lines from a stream using an arbitrary delimiter"""
from math import ceil


__all__ = ['ReadLines']


# the default amount of data to read on a buffer full
DEFAULT_BLOCK_SIZE = 1048756


class ReadLines:              # pylint: disable=too-few-public-methods
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
        the amount requested depending on how much data there is. If size is
        omitted or None, the forth-coming delimited line will be returned.
        """
        if size is None:                # request to peek at a line
            try:
                line, delimiter_pos = self._get_line(consume=False)
                return line[:delimiter_pos] if self.strip_delimiter else line
            except StopIteration:
                return ''

        buf = self._buf
        idx = self._idx

        if not self._eof and len(buf) - idx < size:
            # the stream is not known to be exhausted and the buffer will not
            # satisfy the request

            read = self._read
            block_size = self._block_size

            # truncate the buffer
            buf = buf[idx:]
            self._idx = idx = 0

            amount_needed = size - len(buf)

            while amount_needed > 0:

                # read in the number of blocks necessary to fulfill the request
                data = read(ceil(amount_needed / block_size) * block_size)
                if not data:
                    self._eof = True
                    break

                # more data has been received so it is added to the buffer
                buf += data

                amount_needed = max(size - len(buf), 0)

            # buffer was modified
            self._buf = buf

        return buf[idx:idx + size]

    def __iter__(self):
        return self

    def __next__(self):
        line, delimiter_pos = self._get_line(consume=True)
        return line[:delimiter_pos] if self.strip_delimiter else line

    def _get_line(self, consume):  # pylint: disable=too-many-branches
        """Get the next line

        Arguments
        ---------
        consume : boolean
                  Indicator on whether the line is to be consumed

        Returns
        -------
        A two-tuple of the next line in the stream and the index of where,
        within the line, the delimiter starts if it is present or the length of
        the line if it does not.

        This call will raise a StreamExhausted exception if there are no more
        lines to be read.
        """
        read = self._read
        block_size = self._block_size
        buf, idx, eof = self._buf, self._idx, self._eof
        delimiter = self.delimiter

        # searching starts at the idx
        search_idx = idx

        while True:

            # The delimiter is either str/bytes or a regex-like object
            if isinstance(delimiter, (str, bytes)):
                delimiter_start = buf.find(delimiter, search_idx)

                if delimiter_start != -1:
                    # the length of the delimiter is added to where the
                    # delimiter starts to get the index of where it ends
                    end = delimiter_start + len(delimiter)
                    if consume:
                        # the index attribute is set to indicate where in the
                        # buffer the next line begins
                        self._buf, self._idx, self._eof = buf, end, eof
                    else:
                        self._buf, self._idx, self._eof = buf, idx, eof
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
                        # is treated as an exact match

                        if consume:
                            # the index attribute is set to indicate where in
                            # the buffer the next line begins
                            self._buf, self._idx, self._eof = buf, end, eof
                        else:
                            self._buf, self._idx, self._eof = buf, idx, eof
                        return buf[idx:end], delimiter_start - idx

                    # if the match is at the end of the buffer then reading
                    # more could result in a better match

                    # since a match was found, searching can begin at the point
                    # where the match started
                    search_offset = end - delimiter_start

                else:

                    # the delimiter was not found in the buffer
                    delimiter_start = -1

                    # the buffer needs to be scanned from the beginning
                    search_offset = len(buf) - idx

            if eof:                     # no more data is forth-coming
                end = len(buf)

                if consume:
                    # ensure that another call will result in no search being
                    # performed
                    self._buf, self._idx, self._eof = buf[end:], 0, True
                else:
                    self._buf, self._idx, self._eof = buf, idx, True

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
            if not more:
                eof = True
            buf += more

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

        The stream must be opened for reading and should be blocking.

        The *delimiter* type should match the mode of *fobj*. If *delimiter* is
        str/bytes then the find() method of the internal buffer will be used.
        If *delimiter* is regex then its search() method will be used.

        The *delimiter* should match one or more characters.

        Searching is performed incrementally against blocks read from the
        stream. While accommodations are made to allow regex delimiters to
        match as much as possible, as would be necessary if matching text is
        split between blocks, caution is advised in using regular expressions
        that assume all of the text is present during a search.
        """
        self._read = fobj.read
        self.delimiter = delimiter
        self.strip_delimiter = strip_delimiter
        self._block_size = block_size
        buf = fobj.read(block_size)
        self._buf, self._idx, self._eof = buf, 0, not buf

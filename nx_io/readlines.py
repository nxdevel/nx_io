# -*- coding: utf-8 -*-
"""Object to read lines from a stream using an arbitrary delimiter"""
from math import ceil


__all__ = ['ReadLines']


# the default amount of data to read on a buffer fill
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
        omitted or None, the next delimited line will be returned but it will
        not be consumed.
        """
        if size is None:                # request to peek at a line
            try:
                return self.__next__(consume=False)
            except StopIteration:
                return ''

        buf = self._buf
        idx = self._idx

        if not self._eof and len(buf) - idx < size:
            # the stream is not known to be exhausted and the existing buffer
            # will not satisfy the request

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

    # pylint: disable=too-many-branches,too-many-locals
    def __next__(self, consume=True):
        """Get the next line

        Arguments
        ---------
        consume : boolean
                  Indicator on whether the line is to be consumed

        Returns
        -------
        The next line in the stream
        """
        buf = self._buf
        eof = self._eof
        delimiter = self.delimiter
        read = self._read
        block_size = self._block_size

        # searching starts at the idx
        search_idx = idx = self._idx

        delimiter_is_fixed = isinstance(delimiter, (str, bytes))
        if delimiter_is_fixed:
            delimiter_length = len(delimiter)
            # if a match is not found then the buffer is expanded and another
            # search is performed starting with the new data
            # if the delimiter has multiple characters then the possibility
            # exists that the delimiter has been split between reads so an
            # offset is used to start the search within the buffer that was
            # already searched
            search_offset = delimiter_length - 1
        elif not hasattr(delimiter, 'search'):
            raise TypeError('invalid delimiter type: {}'
                            .format(delimiter.__class__))

        while True:
            if delimiter_is_fixed:
                delimiter_start = buf.find(delimiter, search_idx)

                if delimiter_start != -1:
                    # the length of the delimiter is added to where the
                    # delimiter starts to get the index of where it ends
                    end = delimiter_start + delimiter_length
                    break

            else:
                result = delimiter.search(buf, # pylint: disable=no-member
                                          search_idx)
                if result:
                    delimiter_start = result.start()
                    end = result.end()

                    if end != result.endpos:
                        # if the match is not at the end of the buffer then it
                        # is treated as an exact match
                        break

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

                if idx < end:
                    # there is data in the buffer to return

                    # it is possible that a match exists but an attempt is
                    # being made to find a better match
                    if delimiter_start == -1:
                        # if there was no previous delimiter match then the
                        # final line contains no delimiter
                        delimiter_start = end

                    break

                raise StopIteration

            # truncate the buffer
            buf = buf[idx:]
            idx = 0

            # searching should commence with the new data that will be added
            # to the buffer minus any offset that was previously provided
            search_idx = max(len(buf) - search_offset, 0)

            # get more data
            more = read(block_size)
            if not more:
                self._eof = eof = True
            buf += more
            self._buf = buf

        # set the _idx attribute to the end of the line being returned if it is
        # being consumed otherwise set it to the local idx, which may have been
        # updated if data was added to the buffer
        self._idx = end if consume else idx

        if self.strip_delimiter:
            return buf[idx:delimiter_start]
        return buf[idx:end]
    # pylint: enable=too-many-branches,too-many-locals

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
        self._buf = buf
        self._idx = 0
        self._eof = not buf

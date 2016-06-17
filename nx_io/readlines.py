"Object to read lines from a file using an arbitrary delimiter"
import math


__all__ = ['ReadLines']


BUFFER_SIZE = 1048756


class ReadLines:        # pylint: disable=too-many-instance-attributes
    """Read lines from a stream using an arbitrary delimiter."""
    def reset(self):
        """Reset the object to its initial state, changing the delimiter if
        specified.
        """
        start = self._start
        if start is None:
            raise ValueError('reset on non-seekable file object')
        fobj = self.fobj
        fobj.seek(start)
        buf = fobj.read(self._buffer_size)
        self._buf, self._idx, self._eof = buf, 0, not buf

    def peek(self, size=None):
        """Peek into the stream/buffer."""
        if size is None:                # request to peek at a line
            line_start, line_end, delimiter_start = self._next()

            # _next might read more into the buffer so the index attribute
            # needs to be updated and setting it to the start of the line
            # means the line is not consumed
            self._idx = line_start

            end = delimiter_start if self.strip_delimiter else line_end
            return self._buf[line_start:end]

        if size < 0:
            raise ValueError('invalid size: {}'.format(size))

        if size == 0:
            return ''

        # truncate the buffer
        buf, eof = self._buf[self._idx:], self._eof

        fobj_read, buffer_size = self.fobj.read, self._buffer_size

        # determine if more data is needed to satisfy the request
        extra_needed = size - len(buf)

        # while the file has not been exhausted and more data is need...
        while not eof and extra_needed:

            # determine how much data to read(in multiples of the buffer
            # size) in order to satisfy the request
            to_read = math.ceil(extra_needed / buffer_size) * buffer_size

            tmp_buf = fobj_read(to_read)
            if len(tmp_buf):
                # more data has been received so it is added to the buffer
                buf += tmp_buf

                # determine if the read has satisfied the request
                extra_needed = size - len(buf)

            else:
                eof = True          # no data has been received so EOF

        self._buf, self._eof = buf, eof

        # buffer was truncated so the index attribute needs to be updated
        self._idx = 0

        return buf[:size]

    def __iter__(self):
        return self

    def __next__(self):
        line_start, line_end, delimiter_start = self._next()

        # _next might read more into the buffer so the index attribute needs to
        # be updated and setting it to the end of the line means the line is
        # consumed
        self._idx = line_end

        # a line end of 0 means the buffer and file are exhausted
        if line_end == 0:
            raise StopIteration

        end = delimiter_start if self.strip_delimiter else line_end
        return self._buf[line_start:end]

    def _next(self):
        """Read the next line.

        The buffer may be modified so it is the responsibility of the caller
        to update the _idx attribute.
        """
        delimiter = self.delimiter
        buf, idx = self._buf, self._idx

        # searching starts at the idx
        search_idx = idx

        while True:

            # The delimiter is either str/bytes or a regex-like object
            if isinstance(delimiter, (str, bytes)):
                delimiter_start = buf.find(delimiter, search_idx)

                if delimiter_start != -1:
                    # find() returns the index of the match so the length of
                    # the delimiter is added to it to get where the match ends
                    end = delimiter_start + len(delimiter)
                    return idx, end, delimiter_start

                # a match was not found but if the delimiter is more than one
                # character then the delimiter could have been split so an
                # offest is provided to start the search within the existing
                # buffer
                search_offset = len(delimiter) - 1

            else:
                result = delimiter.search(buf, search_idx) # pylint: disable=no-member
                if result:
                    delimiter_start = result.start()
                    delimiter_end = result.end()

                    if delimiter_end != result.endpos:
                        # if the match is not at the end of the buffer then it
                        # is exact
                        return idx, delimiter_end, delimiter_start

                    # if the match is at the end of the buffer then reading
                    # more into the buffer could result in more being matched
                    # if the regex ends with a greedy pattern
                    # since a match was found, searching can being at the point
                    # where the match started
                    search_offset = delimiter_end - delimiter_start

                else:

                    # the delimiter was not found in the buffer
                    delimiter_start = -1

                    # the buffer needs to be scanned from the beginning
                    search_offset = len(buf) - idx

            if self._eof:
                # no more data is forth-coming

                end = len(buf)
                if idx < end:
                    # there is unconsumed data in the buffer

                    if delimiter_start == -1:
                        # if there was no previous delimiter match then the
                        # final line contains no delimiter
                        delimiter_start = end

                    return idx, end, delimiter_start

                # everything is exhausted so the buffer is cleared
                self._buf = ''

                # a line_end of 0 indicates the file and buffer are exhausted
                return 0, 0, 0

            # truncate the buffer
            buf, idx = buf[idx:], 0

            # search should commence at the where the buffer ends minus any
            # offset that was previously provided
            search_idx = len(buf) - search_offset

            if search_idx < 0:
                # ensure search index does not start before the buffer
                search_idx = 0

            # get more data
            more = self.fobj.read(self._buffer_size)
            buf += more
            if not more:
                self._eof = True

            self._buf = buf

    def __init__(self, fobj, *, delimiter='\n', strip_delimiter=False,
                 buffer_size=BUFFER_SIZE):
        """
        Arguments
        ----------
        fobj            : stream
                          The stream from which to read
        delimiter       : str, bytes, regex
                          Criteria for how a line is terminated
        strip_delimiter : boolean
                          Indicator on whether the delimiter should be included
                          in a returned line
        buffer_size :     integer
                          Size to use for the internal buffer

        Returns
        -------
        The generator produces a line on each iteration

        Attributes
        ----------
        delimiter       : str, bytes, regex
                          Criteria for how a line is terminated
        strip_delimiter : boolean
                          Indicator on whether the delimiter should be included
                          in a returned line

        The mode of *fobj* should match the type of *delimiter*.

        If *delimiter* is str/bytes then the find() method of the internal
        buffer will be used. If *delimiter* is regex then its search() method
        will be used.
        """
        self.fobj = fobj
        self.delimiter = delimiter
        self.strip_delimiter = strip_delimiter
        self._buffer_size = buffer_size
        self._start = fobj.tell() if fobj.seekable() else None
        buf = fobj.read(buffer_size)
        self._buf, self._idx, self._eof = buf, 0, not buf

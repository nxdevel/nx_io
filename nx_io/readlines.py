"Object to read lines from a file using an arbitrary delimiter"
import math


__all__ = ['ReadLines']


BUFFER_SIZE = 1048756


class ReadLines:
    """Read lines from a stream using an arbitrary delimiter."""
    def reset(self, delimiter=None):
        """Reset the object to its initial state, changing the delimiter if
        specified.
        """
        start = self._start
        if start is None:
            raise ValueError('reset on non-seekable file object')
        if delimiter is not None:
            self.delimiter = delimiter
        fobj = self.fobj
        fobj.seek(start)
        buf = fobj.read(self.buffer_size)
        self._buf, self._idx, self._eof = buf, 0, not buf

    def peek(self, size=None):
        """Peek into the stream/buffer."""
        if size is not None:            # explicit count request

            if size < 0:
                raise ValueError('invalid size: {}'.format(size))

            if size == 0:
                return ''

            # truncate the buffer
            buf, eof = self._buf[self._idx:], self._eof

            fobj_read, buffer_size = self.fobj.read, self.buffer_size

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

            # buffer was truncated
            self._idx = 0

            return buf[:size]

        buf, match_start, match_end = self._next()

        # _next reads more into the buffer so the buffer and index attribute
        # need to be updated
        # setting the index instance attribute to the start of the match
        # means the match is not consumed
        self._buf, self._idx = buf, match_start

        return buf[match_start:match_end]

    def __iter__(self):
        return self

    def __next__(self):
        buf, match_start, match_end = self._next()

        # _next reads more into the buffer so the instance attribute needs to
        # be updated and setting the index to the end consumes the matched
        # portion of the buffer
        self._buf, self._idx = buf, match_end

        # an end of 0 means the buffer is exhausted
        if match_end == 0:
            raise StopIteration

        return buf[match_start:match_end]

    def _next(self):
        """Read the next line.

        The only instance attribute modified while scanning for a line is EOF.
        It is the responsibility of the caller to update the instance
        attributes with the returned buffer and index parameters.
        """
        delimiter = self.delimiter
        buf, idx = self._buf, self._idx

        # searching starts at the idx
        search_idx = idx

        while True:

            # The delimiter is either str/bytes(which should match the mode of
            # the file object) or a regex-like object
            if isinstance(delimiter, (str, bytes)):
                result = buf.find(delimiter, search_idx)

                if result != -1:
                    # find() returns the index of the match so the length of
                    # the delimiter is added to it to get where the match ends
                    end = result + len(delimiter)
                    return buf, idx, end

                # no match was found in the buffer but if the delimiter is more
                # than one character then the delimiter could have been split
                # so an offest is provided to start the search within the
                # existing buffer
                search_offset = len(delimiter) - 1

            else:
                result = delimiter.search(buf, search_idx) # pylint: disable=no-member
                if result:

                    # if the match is not at the end of the buffer then it is
                    # exact
                    end = result.end()
                    if result.endpos != end:
                        return buf, idx, end

                    # if the match is at the end of the buffer then reading
                    # more into the buffer could result in more being matched
                    # if the regex ends with a greedy pattern

                    # since a match was found, searching can being at the point
                    # where the match started
                    search_offset = end - result.start()
                else:

                    # no match means the buffer needs to be scanned from the
                    # beginning
                    search_offset = len(buf) - idx

            if self._eof:
                # at this point, no more data is forth-coming so the rest of
                # unconsumed buffer is returned
                end = len(buf)
                if idx < end:
                    return buf, idx, end

                # if an end of 0 is returned then the file is exhausted
                return '', 0, 0

            # truncate the buffer
            buf, idx = buf[idx:], 0

            # search should commence at the where the buffer ends minus any
            # offset that was previously provided
            search_idx = len(buf) - search_offset

            if search_idx < 0:
                # ensure search index does not start before the buffer
                search_idx = 0

            # get more data
            more = self.fobj.read(self.buffer_size)
            buf += more
            if not more:
                self._eof = True

    def __init__(self, fobj, *, delimiter='\n', buffer_size=BUFFER_SIZE):
        """
        Arguments
        ----------
        fobj        : stream
                      The stream from which to read
        delimiter   : str, bytes, regex
                      Indicator of how each line is terminated
        buffer_size : integer
                      Size to use for the internal buffer

        Returns
        -------
        The generator produces a line on each iteration

        Attributes
        ----------
        A delimiter attribute is available to be set to a different value

        The mode of *fobj* should match the type of *delimiter*.

        If *delimiter* is str/bytes then the find() method of the internal
        buffer will be used. If *delimiter* is regex then its search() method
        will be used.
        """
        self.fobj = fobj
        self.delimiter = delimiter
        self.buffer_size = buffer_size
        self._start = fobj.tell() if fobj.seekable() else None
        buf = fobj.read(buffer_size)
        self._buf, self._idx, self._eof = buf, 0, not buf

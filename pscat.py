#!/usr/bin/env python3

# socat in one python file
# The goal is to be able to replace socat in kubctl port-forward

import sys
import argparse
import logging
from collections import namedtuple
import selectors

log = logging.getLogger(__name__)
logging.basicConfig(filename='pscat.log', encoding='utf-8', level=logging.DEBUG)

Socket = namedtuple('Socket', ['rfd', 'wfd'])

import io
class ReaderWrapper(object):
    def __init__(self, io):
        super(ReaderWrapper, self).__init__()
        self.io = io
        self.eof = False

    def next_batches(self, batch_size):
        """non-blocking fds would consume as much as we can by batches.

            this function assumes fd's ready for reading.
        """

        if self.eof:
            return None

        if isinstance(self.io, io.TextIOWrapper):
            if self.io.isatty():
                return self._read_stdin(batch_size)
            else:
                return self._read_file_io(batch_size)
        elif isinstance(self.io, socket.socket):
            return self._read_socket(batch_size)

    def eof_met(self):
        return self.eof

    def _read_stdin(self, batch_size):
        # for stdin, we could only fetch one batch, cause there is no safe way
        # to know whether there is more data without blocking

        # stdin should use raw for reading
        bytes = self.io.buffer.raw.read(batch_size)
        if len(bytes) == 0:
            log.debug('ala')
            self.eof = True
        else:
            log.debug('asdf')
            yield bytes

    def _read_file_io(self, batch_size):
        # File IO could be read till the end
        bytes = self.io.read(batch_size)
        while len(bytes) > 0:
            yield bytes
            bytes = self.io.read(batch_size)
        self.eof = True

    def _read_socket(self, batch_size):
        bytes = self.io.recv(batch_size)
        while len(bytes) >= batch_size and not self.io.getblocking():
            # enters only in non-blocking mode
            yield bytes
            try:
                bytes = self.io.recv(batch_size)
            except BlockingIOError:
                # no more data, just returns
                break
        else:
            # last bits of bytes
            if len(bytes) == 0:
                self.eof = True
            else:
                yield bytes

    def close(self):
        self.io.close()

class WriterWrapper(object):
    def __init__(self, io):
        super(WriterWrapper, self).__init__()
        self.io = io
    def write(self, bytes):
        if isinstance(self.io, io.TextIOWrapper):
            self.io.buffer.raw.write(bytes)
        elif isinstance(self.io, io.IOBase):
            self.io.write(bytes)
        elif isinstance(self.io, socket.socket):
            self.io.sendall(bytes)
    def close(self):
        self.io.close()

sel = selectors.DefaultSelector()

def usage():
    pass

def parse_args():
    parser = argparse.ArgumentParser(description='pscat')
    parser.add_argument('addr1', type=str, help='The first address')
    parser.add_argument('addr2', type=str, help='The first address')
    return parser.parse_args()

def pscat_open(address):
    log.info(f'opening address {address}')

    if address == '-':
        return Socket(rfd = sys.stdin, wfd = sys.stdout)
    else:
        raise Exception(f"address type not supported: {address}")

class Pipe(object):
    def __init__(self, rfd, wfd_fd):
        super(Pipe, self).__init__()
        self.input = ReaderWrapper(rfd)
        self.output = WriterWrapper(wfd_fd)

    def transmit(self) -> bool:
        # returns true on EOF
        for batch in self.input.next_batches(4096):
            log.debug(f'TRX batch: {batch}')
            self.output.write(batch)
        return self.input.eof_met()

    def close(self):
        self.input.close()
        self.output.close()

registerred_fds = set()
def register_event(fileobj, event, data):
    if fileobj in registerred_fds:
        return
    sel.register(fileobj, event, data)
    registerred_fds.add(fileobj)
def unregister(fileobj):
    sel.unregister(fileobj)
    registerred_fds.remove(fileobj)

def pscat_connect(sock1, sock2):
    global registerred_fds
    if sock1.rfd is not None and sock2.wfd is not None:
        pipe = Pipe(sock1.rfd, sock2.wfd)
        register_event(sock1.rfd, selectors.EVENT_READ, pipe)

    if sock2.rfd is not None and sock1.wfd is not None:
        pipe = Pipe(sock2.rfd, sock1.wfd)
        register_event(sock2.rfd, selectors.EVENT_READ, pipe)

    while len(sel.get_map()) > 0:
        events = sel.select()
        for key, mask in events:
            pipe = key.data
            eof = pipe.transmit()
            if eof:
                unregister(key.fileobj)

def pscat(args, address1, address2):
    sock1 = pscat_open(address1)
    sock2 = pscat_open(address2)
    return pscat_connect(sock1, sock2)

def main():
    args = parse_args()
    result = pscat(args, args.addr1, args.addr2)
    sys.exit(result)

if __name__ == '__main__':
    main()

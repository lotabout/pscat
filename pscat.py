#!/usr/bin/env python3

# socat in one python file
# The goal is to be able to replace socat in kubctl port-forward

import os
import time
import io
import sys
import argparse
import logging
from collections import namedtuple
import selectors
import socket

log = logging.getLogger(__name__)
# logging.basicConfig(filename='pscat.log', encoding='utf-8', level=logging.DEBUG)

Socket = namedtuple('Socket', ['rfd', 'wfd'])

class ReaderWrapper(object):
    def __init__(self, io):
        super(ReaderWrapper, self).__init__()
        self.io = io
        self.eof = io is None
        self.closed = io is None

    def get_inner(self):
        return self.io

    def next_batches(self, batch_size):
        """non-blocking fds would consume as much as we can by batches.

            this function assumes fd's ready for reading.
        """
        if self.eof:
            return None
        if self.closed:
            raise Exception('input was closed')

        if isinstance(self.io, io.TextIOWrapper):
            return self._read_stdin(batch_size)
        elif isinstance(self.io, io.IOBase):
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
            self.eof = True
        else:
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
        if self.closed:
            return
        if isinstance(self.io, socket.socket):
            try:
                self.io.shutdown(socket.SHUT_RD)
            except OSError:
                pass
        else:
            self.io.close()
        self.closed = True

class WriterWrapper(object):
    def __init__(self, io):
        super(WriterWrapper, self).__init__()
        self.io = io
        self.closed = io is None

    def get_inner(self):
        return self.io

    def write(self, bytes):
        if self.closed:
            raise Exception('output was closed')

        if isinstance(self.io, io.TextIOWrapper):
            self.io.buffer.raw.write(bytes)
        elif isinstance(self.io, io.IOBase):
            self.io.write(bytes)
        elif isinstance(self.io, socket.socket):
            self.io.sendall(bytes)

    def close(self):
        if self.closed:
            return
        if isinstance(self.io, socket.socket):
            try:
                self.io.shutdown(socket.SHUT_WR)
            except OSError:
                pass
        else:
            self.io.close()
        self.closed = True

class Pipe(object):
    def __init__(self, rfd, wfd):
        super(Pipe, self).__init__()
        self.input = ReaderWrapper(rfd)
        self.output = WriterWrapper(wfd)

    def get_input(self):
        return self.input
    def get_output(self):
        return self.output

    def transmit(self) -> bool:
        # returns true on EOF
        for batch in self.input.next_batches(4096):
            log.debug(f"TRX: {batch}")
            self.output.write(batch)
        return self.input.eof_met()

    def close(self):
        self.input.close()
        self.output.close()

class PscatConnect(object):
    """docstring for PscatConnect"""
    def __init__(self, sock1, sock2, close_timeout=0.5):
        super(PscatConnect, self).__init__()
        self.sel = selectors.DefaultSelector()
        self.pipes = []
        self.stop_at = None
        self.close_timeout = close_timeout

        if sock1.rfd is not None and sock2.wfd is not None:
            pipe = Pipe(sock1.rfd, sock2.wfd)
            self._register(pipe)

        if sock2.rfd is not None and sock1.wfd is not None:
            pipe = Pipe(sock2.rfd, sock1.wfd)
            self._register(pipe)

    def _register(self, pipe):
        input_fd = pipe.get_input().get_inner();
        if input_fd in self.sel.get_map():
            return
        self.pipes.append(pipe)
        self.sel.register(input_fd, selectors.EVENT_READ, (pipe))

    def _unregister(self, pipe):
        input_fd = pipe.get_input().get_inner();
        if input_fd in self.sel.get_map():
            self.sel.unregister(input_fd)
            self.stop_at = time.time() + self.close_timeout

    def run(self):
        while len(self.sel.get_map()) > 0:
            timeout = self.close_timeout if self.stop_at is not None else None
            events = self.sel.select(timeout)

            now = time.time()
            if self.stop_at is not None and self.stop_at <= now:
                break

            for key, mask in events:
                pipe = key.data
                eof = pipe.transmit()
                if eof:
                    self._unregister(pipe)

        # should not close pipes manually cause the resource might still
        # be used by parent processes. Let OS clean the resource

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
    elif address.startswith('TCP:'): # TCP:host:port
        components = address.split(':')
        host = components[1]
        port = int(components[2])
        s = socket.socket()
        s.connect((host, port))
        return Socket(rfd = s, wfd = s)
    elif address.startswith('TCP-LISTEN:'):
        return open_socket(address)
    elif address.startswith('OPEN:'): # OPEN:<filename>
        components = address.split(':')
        filename = components[1]
        input = open(filename, 'rb')
        return Socket(rfd = input, wfd = None)
    else:
        raise Exception(f"address type not supported: {address}")

def open_socket(address: str) -> Socket:
    # TCP-LISTEN:port
    components = address.split(':')
    options = components[1].split(',') # e.g. <port>,reuseaddr,fork
    port = int(options[0])

    s = socket.socket()
    if 'reuseaddr' in options:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    s.bind(('0.0.0.0', port))
    s.listen()

    dofork = 'fork' in options

    conn, addr = s.accept()
    while dofork and os.fork() != 0:
        # parent would continue to accept new connections
        conn, addr = s.accept()

    return Socket(rfd = conn, wfd = conn)


def pscat(args, address1, address2):
    sock1 = pscat_open(address1)
    sock2 = pscat_open(address2)
    connect = PscatConnect(sock1, sock2)
    return connect.run()

def main():
    args = parse_args()
    result = pscat(args, args.addr1, args.addr2)
    sys.exit(result)

if __name__ == '__main__':
    main()

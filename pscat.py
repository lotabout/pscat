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

sel = selectors.DefaultSelector()

def usage():
    pass

def parse_args():
    parser = argparse.ArgumentParser(description='pscat')
    parser.add_argument('addr1', type=str, help='The first address')
    parser.add_argument('addr2', type=str, help='The first address')
    return parser.parse_args()

def set_input_nonblocking():
    import fcntl
    import os
    orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
    fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)

def pscat_open(address):
    log.info(f'opening address {address}')

    if address == '-':
        set_input_nonblocking()
        return Socket(rfd = sys.stdin, wfd = sys.stdout)
    else:
        raise Exception(f"address type not supported: {address}")

read_buf = bytearray(1024)
def copy_data(src_fd, dst_fd, mask):
    log.debug(f'copy data from {src_fd} -> {dst_fd}')
    bytes_read = src_fd.buffer.readinto(read_buf)
    if not bytes_read:
        log.debug('EOF MET')
        return True # means EOF
    log.debug(f'copy data: {read_buf[:bytes_read]}')
    dst_fd.buffer.write(read_buf[:bytes_read])
    dst_fd.buffer.flush()
    return False

registerred_fds = set()
def register_event(fileobj, event, data):
    if fileobj in registerred_fds:
        return
    sel.register(fileobj, event, data)
    registerred_fds.add(fileobj)

def pscat_connect(sock1, sock2):
    global registerred_fds
    if sock1.rfd is not None and sock2.wfd is not None:
        register_event(sock1.rfd, selectors.EVENT_READ, (copy_data, sock2.wfd))

    if sock2.rfd is not None and sock1.wfd is not None:
        register_event(sock2.rfd, selectors.EVENT_READ, (copy_data, sock1.wfd))

    while True:
        events = sel.select()
        for key, mask in events:
            (callback, dst_fd) = key.data
            eof_met = callback(key.fileobj, dst_fd, mask)
            if eof_met:
                return

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

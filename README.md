# Socat in one python file

The initial goal was replace `socat` in `kubctl port-forward` (which however
is no longer required in newer version), so that in systems that were not able
to install socat this script could be a replacement.

## What's been done

- `TCP-LISTEN:<port>` for creating server
    - only `fork` and `reuseaddr` option were implemented
- `TCP:<host>:<port>` and `TCP4:<host>:<port>` for create connections
- `OPEN:<file-path>` for opening file for reading
- `-` for stdin and stdout, you could use shell's redir for reading from and
    writing to files
- No line ending tranlation at all. (I don't need it)

## None Goal

- Implement all the features of `socat`

## Example

```sh
./pscat.py <addr1> <addr2>
```

Think `pscat` as a tool for creating pipes connecting the read end of `addr1`
and write end of `addr2` and vice versa.

### As an echo server

```sh
./pscat.py - TCP-LISTEN:8080
```

Note that TCP-LISTEN will accept only one connection. If you need to keep the
server running, add `fork` option:


```sh
./pscat.py - TCP-LISTEN:8080,fork
```

### As an echo client

```sh
./pscat.py - TCP:localhost:8080
```

### transfer a file

```sh
# server side for receiving (with an IP for connection)
./pscat.py - TCP-LISTEN:8080 > file
# client side for sending
./pscat.py - TCP:server:8080 < file
```

```sh
# server side for sending (with an IP for connection)
./pscat.py - TCP-LISTEN:8080 < file
# client side for receiving
./pscat.py - TCP:server:8080 > file
```

### As a proxy

Listening on port `8080` and transfer all data to `remote:port`.

```sh
./pscat.py TCP-LISTEN:8080 TCP:remote:port
```

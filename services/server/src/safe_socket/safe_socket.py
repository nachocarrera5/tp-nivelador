import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket, size):

    if size < 0:
        raise ValueError("size must be equal or greater than cero")

    received = bytearray()

    while len(received) < size:

        pending = size - len(received)
        to_recv = socket.recv(pending)

        if to_recv == b"":
            raise ConnectionError("Connection closed by the other side")

        received.extend(to_recv)

    return bytes(received)


def send_all(socket: socket.socket, bytes):

    total = 0

    while total < len(bytes):
        sent = socket.send(bytes[total:])
        total += sent

    return total

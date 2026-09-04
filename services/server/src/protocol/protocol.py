import safe_socket

TYPE_BET = b"B"
TYPE_ACK = b"A"
TYPE_FINISH = b"F"
TYPE_WINNER = b"W"
TYPE_END = b"E"
TYPE_ERROR = b"X"

HEADER_SIZE = 9
MAX_PAYLOAD_SIZE = 99999999

VALID_TYPES = [
    TYPE_BET,
    TYPE_ACK,
    TYPE_FINISH,
    TYPE_WINNER,
    TYPE_END,
    TYPE_ERROR,
]


class Message:
    def __init__(self, message_type: bytes, payload: bytes):
        self.type = message_type
        self.payload = payload


def send_message(socket, message: Message):

    if message.type not in VALID_TYPES:
        raise ValueError(f"Invalid message type {message.type}")

    payload_size = len(message.payload)
    if payload_size > MAX_PAYLOAD_SIZE:
        raise ValueError(
            f"Payload size {payload_size} exceeds maximum {MAX_PAYLOAD_SIZE}"
        )

    header = message.type + str(payload_size).zfill(8).encode()

    safe_socket.send_all(socket, header + message.payload)


def recv_message(socket) -> Message:

    header = safe_socket.recv_all(socket, HEADER_SIZE)

    message_type = header[0:1]  # el slice me da un byte, no un int

    if message_type not in VALID_TYPES:
        raise ValueError(f"Invalid message type {message_type}")

    payload_size = int(header[1:])

    payload = safe_socket.recv_all(socket, payload_size)

    return Message(message_type, payload)

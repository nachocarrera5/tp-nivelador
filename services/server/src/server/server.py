import socket
import logger
from lottery import Bet, Lottery
from protocol import protocol

_LOTTERY_STORAGE_PATH = "/tmp/lottery.csv"

def deserialize_bet(payload: bytes) -> Bet:
    fields = payload.decode().split(",")

    if len(fields) != 6:
        raise ValueError(f"Invalid bet payload {payload}")

    agency_id = int(fields[0])
    first_name = fields[1]
    last_name = fields[2]
    document = int(fields[3])
    birthdate = fields[4]
    number = int(fields[5])

    return Bet(agency_id, first_name, last_name, document, birthdate, number)

def serialize_winner(bet: Bet) -> bytes:
    return f"{bet.first_name},{bet.last_name},{bet.document:08d},{bet.birthdate},{bet.number}".encode()


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

        with open(_LOTTERY_STORAGE_PATH, "w"):
            pass

        self.lottery = Lottery(_LOTTERY_STORAGE_PATH)

    def _handle_bet(self, payload: bytes, current_agency_id):
        bet = deserialize_bet(payload)

        if bet.agency_id != current_agency_id and current_agency_id is not None:
            raise ValueError(
                f"Invalid agency id {bet.agency_id}, expected {current_agency_id}"
            )

        self.lottery.store_bets([bet])

        return bet.agency_id

    def _send_winners(self, client_socket, agency_id: int):
        for bet in self.lottery.load_bets():
            if bet.agency_id != agency_id:
                continue

            if not self.lottery.has_won(bet):
                continue

            payload = serialize_winner(bet)
            message = protocol.Message(protocol.TYPE_WINNER, payload)
            protocol.send_message(client_socket, message)

        message = protocol.Message(protocol.TYPE_END, b"")
        protocol.send_message(client_socket, message)


    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        agency_id = None

        try:
            logger.info(action, logger.LogResult.in_progress)

            while True:
                try:
                    client_message = protocol.recv_message(client_socket)

                    if client_message.type == protocol.TYPE_BET:
                        agency_id = self._handle_bet(client_message.payload, agency_id)
                        message_amount += 1
                        ack_message = protocol.Message(protocol.TYPE_ACK, b"")
                        protocol.send_message(client_socket, ack_message)

                    elif client_message.type == protocol.TYPE_FINISH:
                        finish_agency_id = int(client_message.payload.decode())

                        if finish_agency_id != agency_id and agency_id is not None:
                            raise ValueError(
                                f"Invalid agency id {finish_agency_id}, expected {agency_id}"
                            )

                        self._send_winners(client_socket, finish_agency_id)

                        logger.info(
                            action, logger.LogResult.success, "messages-amount", message_amount
                        )
                        return

                    else:
                        raise ValueError(f"Invalid message type {client_message.type}")

                except ValueError as e:
                    error_message = protocol.Message(protocol.TYPE_ERROR, str(e).encode())
                    protocol.send_message(client_socket, error_message)

                    logger.error(
                        action, logger.LogResult.fail, "messages-amount", message_amount
                    )
                    return

        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)

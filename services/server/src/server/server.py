import socket
import logger
from lottery import Bet, Lottery
from protocol import protocol
import threading

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

def deserialize_bets(payload: bytes) -> list[Bet]:
    bets = []

    bet_payloads = payload.split(b"\n")

    for line in bet_payloads:
        if not line:
            continue

        bet = deserialize_bet(line)
        bets.append(bet)

    return bets

def serialize_winner(bet: Bet) -> bytes:
    return f"{bet.first_name},{bet.last_name},{bet.document:08d},{bet.birthdate},{bet.number}".encode()


class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min

        with open(_LOTTERY_STORAGE_PATH, "w"):
            pass

        self.lottery = Lottery(_LOTTERY_STORAGE_PATH)
        self.finished_agencies = set()
        self.finish_condition = threading.Condition()
        self.lottery_lock = threading.Lock()

    def _handle_bets(self, payload: bytes, current_agency_id):
        bets = deserialize_bets(payload)

        if len(bets) == 0:
            raise ValueError("no bets received")

        agency_id = current_agency_id

        for bet in bets:
            if agency_id is None:
                agency_id = bet.agency_id
            elif bet.agency_id != agency_id:
                raise ValueError(
                    f"Invalid agency id {bet.agency_id}, expected {agency_id}"
                )

        with self.lottery_lock:
            self.lottery.store_bets(bets)

        return bets[0].agency_id

    def _send_winners(self, client_socket, agency_id: int):
        with self.lottery_lock: # el lock aca lo tengo que tomar porque el quorum es minimo, puede no coincidir con la cantidad de agencias que ya terminaron de enviar sus apuestas, y si otra agencia termina antes, puede estar leyendo el archivo mientras yo estoy escribiendo en el mismo
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
                        agency_id = self._handle_bets(client_message.payload, agency_id)
                        message_amount += 1
                        ack_message = protocol.Message(protocol.TYPE_ACK, b"")
                        protocol.send_message(client_socket, ack_message)

                    elif client_message.type == protocol.TYPE_FINISH:
                        finish_agency_id = int(client_message.payload.decode())

                        if finish_agency_id != agency_id and agency_id is not None:
                            raise ValueError(
                                f"Invalid agency id {finish_agency_id}, expected {agency_id}"
                            )

                        self._wait_for_quorum(finish_agency_id)
                        # "Deberá esperar a la notificación de un mínimo de agencias para realizar el sorteo."
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

    def _wait_for_quorum(self, agency_id: int):
        with self.finish_condition:
            self.finished_agencies.add(agency_id)

            if len(self.finished_agencies) == self.agency_quorum_min:
                self.finish_condition.notify_all()

            while len(self.finished_agencies) < self.agency_quorum_min:
                self.finish_condition.wait()

    def _run_thread(self, client_socket):
        with client_socket:
            self._handle_client(client_socket)

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

                thread = threading.Thread(target=self._run_thread, args=(client_socket,))
                thread.start()

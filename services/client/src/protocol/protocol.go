package protocol

import (
	"fmt"
	"io"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	TypeBet    byte = 'B'
	TypeAck    byte = 'A'
	TypeFinish byte = 'F'
	TypeWinner byte = 'W'
	TypeEnd    byte = 'E'
	TypeError  byte = 'X'
)

const HEADER_SIZE = 9
const MAX_PAYLOAD_SIZE = 99999999

// 1 byte de tipo + 8 bytes de longitud + payload
type Message struct {
	Type    byte
	Payload []byte
}

func isValidMessageType(messageType byte) bool {
	switch messageType {
	case TypeBet, TypeAck, TypeFinish, TypeWinner, TypeEnd, TypeError:
		return true
	default:
		return false
	}
}

func SendMessage(socket io.Writer, message Message) error {
	if !isValidMessageType(message.Type) {
		return fmt.Errorf("invalid message type: %c", message.Type)
	}

	if len(message.Payload) > MAX_PAYLOAD_SIZE {
		return fmt.Errorf("payload size exceeds maximum limit: %d", len(message.Payload))
	}

	lengthFill := fmt.Sprintf("%08d", len(message.Payload)) // para convertir a 8 digitos

	preparedMessage := make([]byte, 0, HEADER_SIZE+len(message.Payload))

	preparedMessage = append(preparedMessage, message.Type)
	preparedMessage = append(preparedMessage, []byte(lengthFill)...)
	preparedMessage = append(preparedMessage, message.Payload...)

	return safe_socket.SendAll(socket, preparedMessage)
}

func RecvMessage(socket io.Reader) (Message, error) {
	header, err := safe_socket.RecvAll(socket, HEADER_SIZE)
	if err != nil {
		return Message{}, fmt.Errorf("failed to receive message header: %w", err)
	}

	messageType := header[0]
	if !isValidMessageType(messageType) {
		return Message{}, fmt.Errorf("invalid message type received: %c", messageType)
	}

	payloadSize, err := parsePayloadSize(header[1:])

	if err != nil {
		return Message{}, fmt.Errorf("failed to parse payload size: %w", err)
	}

	payload, err := safe_socket.RecvAll(socket, payloadSize)
	if err != nil {
		return Message{}, fmt.Errorf("failed to receive message payload: %w", err)
	}

	return Message{
		Type:    messageType,
		Payload: payload,
	}, nil
}

func parsePayloadSize(lengthBytes []byte) (int, error) {
	payloadSize := 0

	for _, char := range lengthBytes {
		if char < '0' || char > '9' {
			return 0, fmt.Errorf("invalid character in payload size: %c", char)
		}
		payloadSize = payloadSize*10 + int(char-'0') // (char-'0') convierte el byte a su valor numérico
	}

	return payloadSize, nil
}

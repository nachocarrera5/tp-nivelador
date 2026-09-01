package client

import (
	"net"
	"time"
	"bufio"
	"os"
	"errors"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile string
	OutputFile string
	BatchSize int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) sendBets(bets []string) error {

	betsPayload := make([]string, 0, len(bets))
	for _, bet := range bets {
		singleBetPayload := client.config.AgencyId + "," + bet
		betsPayload = append(betsPayload, singleBetPayload)
	}

	payloadString := strings.Join(betsPayload, "\n")
	payload := []byte(payloadString)

	message := protocol.Message{
		Type:    protocol.TypeBet,
		Payload: payload,
	}

	err := protocol.SendMessage(client.conn, message)
	if err != nil {
		return err
	}

	response, err := protocol.RecvMessage(client.conn)
	if err != nil {
		return err
	}
	
	if response.Type == protocol.TypeError {
		return errors.New(string(response.Payload))
	}

	if response.Type != protocol.TypeAck {
		return errors.New("unexpected response type: " + string(response.Type))
	}

	return nil
}

func (client *Client) sendFinish() error {
	message := protocol.Message{
		Type:    protocol.TypeFinish,
		Payload: []byte(client.config.AgencyId),
	}

	return protocol.SendMessage(client.conn, message)

}

func (client *Client) recvWinners(writer *bufio.Writer) error{
	finished := false

	for !finished {
		message, err := protocol.RecvMessage(client.conn)
		if err != nil {
			logger.Error("recv-winners", logger.Fail, "err", err)
			return err
		}

		switch message.Type {

		case protocol.TypeWinner:
			winner := string(message.Payload) + "\n"
			written, err := writer.WriteString(winner)
			if err != nil {
				return err
			}
			if written != len(winner) {
				return errors.New("winner message missing bytes")
			}

		case protocol.TypeEnd:
			finished = true

		case protocol.TypeError:
			return errors.New(string(message.Payload))
			
		default:
			return errors.New("unexpected message type: " + string(message.Type))
		}
	}

	return nil
}

func (client *Client) Run() error {
	const mainAction = "bets-and-winners"
	defer client.conn.Close()

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		return err
	}

	// Con defer me aseguro que el archivo se cierre cuando termine Run
	defer inputFile.Close()

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil{
		return err
	}
	defer outputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	writer := bufio.NewWriter(outputFile)
	batch := make([]string, 0, client.config.BatchSize)
	messageId := 0

	// loop de lectura

	for scanner.Scan() {

		bet := scanner.Text()

		if bet == "" {
			continue
		}

		batch = append(batch, bet)

		if len(batch) < client.config.BatchSize {
			continue
		}

		logger.Info(
			mainAction,
			logger.InProgress,
			"agency-id", client.config.AgencyId,
			"message-id", messageId,
		)

		err := client.sendBets(batch)
		if err != nil {
			return err
		}
		batch = batch[:0] // limpio el batch

		messageId++
	}

	scanErr := scanner.Err()
	if scanErr != nil {
		return scanErr
	}

	// si el numero de apuestas no es multiplo del batch size, envio el batch restante
	if len(batch) > 0 {
		logger.Info(
			mainAction,
			logger.InProgress,
			"agency-id", client.config.AgencyId,
			"message-id", messageId,
		)

		err := client.sendBets(batch)
		if err != nil {
			return err
		}

		messageId++
	}

	finishErr := client.sendFinish()
	if finishErr != nil {
		return finishErr
	}

	recvWinnErr := client.recvWinners(writer)
	if recvWinnErr != nil {
		return recvWinnErr
	}

	flushErr := writer.Flush()
	if flushErr != nil {
		return flushErr
	}

	logger.Info(
		mainAction,
		logger.Success,
		"agency-id", client.config.AgencyId,
		"batches-amount", messageId,
	)

	return nil
}

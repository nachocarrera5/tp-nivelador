package client

import (
	"net"
	"time"
	"bufio"
	"os"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const ECHO_CLIENT_BUFFER_SIZE = 512

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile string
	OutputFile string
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

func (client *Client) Run() error {
	const mainAction = "input-read-and-output-write"
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
	messageId := 0

	// loop de lectura

	for scanner.Scan() {

		clientMessage := scanner.Text()

		if clientMessage == "" {
			continue
		}

		logger.Info(
			mainAction,
			logger.InProgress,
			"agency-id", client.config.AgencyId,
			"message-id", messageId,
		)

		sendErr := safe_socket.SendAll(client.conn, []byte(clientMessage))
		if sendErr != nil {
			return sendErr
		}

		response, recvErr := safe_socket.RecvAll(client.conn,ECHO_CLIENT_BUFFER_SIZE)
		if recvErr != nil {
			return recvErr
		}

		// check echo server
		if string(response) != clientMessage {
			return errors.New("server response does not match sent message")
		}

		responseLine := string(response) + "\n"

		written, writeErr := writer.WriteString(responseLine)
		if writeErr != nil {
			return writeErr
		}

		if written != len(responseLine) {
			return errors.New("written output missing some bytes")
		}

		messageId++
	}

	scanErr := scanner.Err()
	if scanErr != nil {
		return scanErr
	}

	flushErr := writer.Flush()
	if flushErr != nil {
		return flushErr
	}

	logger.Info(
		mainAction,
		logger.Success,
		"agency-id", client.config.AgencyId,
		"messages-amount", messageId,
	)

	return nil
}

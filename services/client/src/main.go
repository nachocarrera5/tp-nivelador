package main

import (
	"errors"
	"os"
	"strconv"
	"os/signal"
	"syscall"

	client "github.com/7574-sistemas-distribuidos/tp-nivelador/src/client"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

func loadConfig() (client.ClientConfig, error) {
	agencyId := os.Getenv("AGENCY_ID")
	if agencyId == "" {
		return client.ClientConfig{}, errors.New("AGENCY_ID environment variable is required")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE environment variable is required")
	}

	outputFile := os.Getenv("OUTPUT_FILE")
	if outputFile == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_FILE environment variable is required")
	}

	batchSizeStr := os.Getenv("BATCH_SIZE")
	if batchSizeStr == "" {
		return client.ClientConfig{}, errors.New("BATCH_SIZE environment variable is required")
	}

	batchSize, err := strconv.Atoi(batchSizeStr) // convertir a int
	if err != nil {
		return client.ClientConfig{}, errors.New("BATCH_SIZE must be a valid integer")
	}

	if batchSize <= 0 {
		return client.ClientConfig{}, errors.New("BATCH_SIZE must be a positive integer")
	}

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		AgencyId:   agencyId,
		InputFile:	inputFile,
		OutputFile:	outputFile,
		BatchSize: batchSize,
	}, nil
}

func run() int {

	sigChan := make(chan os.Signal, 1) // canal para recibir señales del sistema operativo
	signal.Notify(sigChan, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	config, err := loadConfig()
	if err != nil {
		logger.Error("load-config", logger.Fail, "err", err)
		return 1
	}

	client, err := client.NewClient(config)
	if err != nil {
		logger.Error("client-new", logger.Fail, "err", err)
		return 1
	}

	clientErrChan := make(chan error, 1)
	go func() {
		clientErrChan <- client.Run()
	}()

	select {

	case err := <-clientErrChan:
		if err != nil {
			logger.Error("client-run", logger.Fail, "err", err)
			return 1
		}
		return 0

	case <-sigChan:
		closeErr := client.Close()
		if closeErr != nil {
			logger.Error("client-close", logger.Fail, "err", closeErr)
		}
		<-clientErrChan // esperar a que termine el cliente y ejecutar el defer de cerrar la conexion
		return 0
	}

}

func main() {
	os.Exit(run())
}

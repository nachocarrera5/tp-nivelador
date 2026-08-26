package safe_socket

import "io"

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	writtenCount := 0

	for writtenCount < len(bytes) {
		n, err := socket.Write(bytes[writtenCount:])

		writtenCount += n

		if err != nil {
			return err
		}
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)

	readCount := 0
	
	// leo hasta alcanzar los bytes que quiere la funcion
	for readCount < size {

		n, err := socket.Read(buff[readCount:])
		
		readCount += n
 
		if err != nil {
			if err == io.EOF && readCount == size { // termine de leer
				return buff, nil
			}
			return nil, err
		}
	
	}
	return buff, nil
}

package utils

import (
	"bufio"
	log "github.com/sirupsen/logrus"
)

func ReadInput(reader *bufio.Reader) string {
	text, err := reader.ReadString('\n')
	if err != nil {
		log.Fatalf("Error reading from STDIN.", err)
	}

    if text[len(text)-1:] == "\n" {
        return text[:len(text)-1]
    } else {
        return text
    }
}

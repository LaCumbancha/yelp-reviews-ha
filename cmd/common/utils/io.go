package utils

import (
	"fmt"
	"bufio"
	"strconv"
	log "github.com/sirupsen/logrus"
)

func ReadInput(reader *bufio.Reader) string {
	text, err := reader.ReadString('\n')
	if err != nil {
		log.Fatalf("Error reading from STDIN. Err: %s", err)
	}

	if text[len(text)-1:] == "\n" {
		return text[:len(text)-1]
	} else {
		return text
	}
}

func ReadInputInt(reader *bufio.Reader) int {
	for true {
		input := ReadInput(reader)
		intInput, err := strconv.Atoi(input)
		if err != nil {
			fmt.Print("Wrong value. Must be an integer: ")
		} else {
			return intInput
		}
	}

	return 0
}

func ReadInputFloat32(reader *bufio.Reader) float32 {
	for true {
		input := ReadInput(reader)
		value, err := strconv.ParseFloat(input, 32)
		if err != nil {
			fmt.Print("Wrong value. Must be an integer: ")
		} else {
			return float32(value)
		}
	}

	return 0.0
}

package communication

import (
	"strings"
	"strconv"
)

// Protocol special messages.
const endMessage = "FINISH-MESSAGE"
const closeMessage = "CLOSE-MESSAGE"

// Retries finish attemps.
const retries = 25

// Defining custom Retries
func EndSignals(outputs int) int {
	return retries * outputs
}

// Defining custom End-Message
func EndMessage(instance string, datasetNumber int) []byte {
	return []byte(endMessage + instance + "|" + strconv.Itoa(datasetNumber))
}

// Defining custom Close-Message
func CloseMessage(instance string) string {
	return closeMessage + instance
}

// Detect all possible end messages (could be like 'FINISH1|1').
func IsEndMessage(message string) bool {
	return strings.HasPrefix(message, endMessage)
}

// Detect all possible close messages (could be like 'CLOSE1|1').
func IsCloseMessage(message string) bool {
	return strings.HasPrefix(message, closeMessage)
}

func specialMessageDatasetNumber(message string) int {
	separatorIdx := strings.Index(message, "|")
	datasetNumber, err := strconv.Atoi(message[separatorIdx+1:len(message)])

	if err != nil {
		return 0
	} else {
		return datasetNumber
	}
}

// Detect if all end signals were received
func LastEndMessage(message string, datasetNumber int, receivedSignals map[string]int, expectedSignals int) (bool, bool) {
	if specialMessageDatasetNumber(message) != datasetNumber {
		return false, false
	}

	receivedSignals[message] = receivedSignals[message] + 1
	newSignal := receivedSignals[message] == 1
	distinctSignals := len(receivedSignals)

	return newSignal, (distinctSignals == expectedSignals) && newSignal
}

// Detect if all close signals were received
func LastCloseMessage(message string, receivedSignals map[string]int, expectedSignals int) (bool, bool) {
	receivedSignals[message] = receivedSignals[message] + 1
	newSignal := receivedSignals[message] == 1
	distinctSignals := len(receivedSignals)

	return newSignal, (distinctSignals == expectedSignals) && newSignal
}

package communication

// Protocol special messages.
const endMessage = "END-MESSAGE"
const closeMessage = "CLOSE-CONNECTION"

// Retries finish attemps.
const retries = 25

// Defining custom Retries
func EndSignals(outputs int) int {
	return retries * outputs
}

// Defining custom End-Message
func EndMessage(instance string) []byte {
	return []byte(endMessage + instance)
}

// Defining custom Close-Message
func CloseMessage(instance string) string {
	return closeMessage + instance
}

// Detect all possible end messages (could be like 'END-MESSAGE1').
func IsEndMessage(message string) bool {
	return (len(message) > 10) && (message[0:11] == endMessage)
}

// Detect all possible close messages (could be like 'CLOSE-CONNECTION1').
func IsCloseMessage(message string) bool {
	return (len(message) > 15) && (message[0:16] == closeMessage)
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

func specialMessageDatasetNumber(message string) int {
	return 0
}

// Detect if all close signals were received
func LastCloseMessage(message string, receivedSignals map[string]int, expectedSignals int) (bool, bool) {
	receivedSignals[message] = receivedSignals[message] + 1
	newSignal := receivedSignals[message] == 1
	distinctSignals := len(receivedSignals)

	return newSignal, (distinctSignals == expectedSignals) && newSignal
}

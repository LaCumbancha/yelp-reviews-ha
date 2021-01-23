package communication

// Protocol special messages.
const endMessage = "END-MESSAGE"

// Retries finish attemps.
const retries = 25

// Defining custom Retries
func EndSignals(outputs int) int {
	return retries * outputs
}

// Defining custom End-Message
func EndMessage(instance string) string {
	return endMessage + instance
}

// Detect all possible end messages (could be like 'END-MESSAGE1').
func IsEndMessage(message string) bool {
	return (len(message) > 10) && (message[0:11] == endMessage)
}

// Detect if all end signals were received
func LastEndMessage(message string, receivedSignals map[string]int, expectedSignals int) (bool, bool) {
	receivedSignals[message] = receivedSignals[message] + 1
	newSignal := receivedSignals[message] == 1
	distinctSignals := len(receivedSignals)

	return newSignal, (distinctSignals == expectedSignals) && newSignal
}

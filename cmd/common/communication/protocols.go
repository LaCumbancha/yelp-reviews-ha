package communication

// Protocol special messages.
const endMessage = "FINISH-MESSAGE"
const startMessage = "START-MESSAGE"
const closeMessage = "CLOSE-MESSAGE"

// Retries finish attemps.
const retries = 25

// Defining custom Retries
func EndSignals(outputs int) int {
	return retries * outputs
}

// Defining custom Start-Message
func StartMessageSigned(nodeCode string, dataset int, instance string) []byte {
	return []byte(SignMessage(nodeCode, dataset, instance, 0, startMessage))
}

// Defining custom End-Message
func FinishMessageSigned(nodeCode string, dataset int, instance string) []byte {
	return []byte(SignMessage(nodeCode, dataset, instance, 0, endMessage))
}

// Defining custom Close-Message
func CloseMessageSigned(nodeCode string, instance string) []byte {
	return []byte(SignMessage(nodeCode, 0, instance, 0, closeMessage))
}

// Detect all start messages.
func IsStartMessage(message string) bool {
	return message == startMessage
}

// Detect all end messages.
func IsFinishMessage(message string) bool {
	return message == endMessage
}

// Detect all close messages.
func IsCloseMessage(message string) bool {
	return message == closeMessage
}

// Signaling control, for closing and finishin.
func signalsControl(signaledInstance string, storedSignals map[string]int, expectedSignals int) (bool, bool, bool) {
	firstSignal := len(storedSignals) == 0
	storedSignals[signaledInstance] = storedSignals[signaledInstance] + 1
	distinctSignals := len(storedSignals)

	newSignal := storedSignals[signaledInstance] == 1
	allSignals := (distinctSignals == expectedSignals) && newSignal
	return firstSignal, newSignal, allSignals
}

// Detect if all signals from multiple datasets were received
func MultiDatasetControl(dataset int, instance string, receivedSignals map[int]map[string]int, expectedSignals int) (bool, bool, bool) {
	if _, found := receivedSignals[dataset]; !found {
		receivedSignals[dataset] = make(map[string]int)
	}

	return signalsControl(instance, receivedSignals[dataset], expectedSignals)
}

// Detect if all signals from a single dataset were received
func SingleControl(instance string, receivedSignals map[string]int, expectedSignals int) (bool, bool, bool) {
	return signalsControl(instance, receivedSignals, expectedSignals)
}

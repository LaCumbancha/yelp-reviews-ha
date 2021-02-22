package communication

import (
	"github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
)


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

// Dataset signals control.
func signalsControl(
	signaledInput string, 
	signaledInstance string, 
	signalsReceivedByInput map[string]map[string]int, 
	signalsNeededByInput map[string]int,
	savedInputs []string,
) (bool, bool, bool, bool) {
	totalInputs := len(signalsNeededByInput)

	if _, found := signalsReceivedByInput[signaledInput]; !found {
		signalsReceivedByInput[signaledInput] = make(map[string]int)
	}
	inputStoredSignals := signalsReceivedByInput[signaledInput]
	inputSignalsNeeded := signalsNeededByInput[signaledInput]
	
	inputFirstSignal := len(inputStoredSignals) == 0
	inputStoredSignals[signaledInstance]++
	inputDistinctSignals := len(inputStoredSignals)

	inputNewSignal := inputStoredSignals[signaledInstance] == 1
	inputAllSignals := (inputDistinctSignals == inputSignalsNeeded) && inputNewSignal

	everyInputAllSignals := true
	if inputNewSignal {
		if len(signalsReceivedByInput) + len(savedInputs) >= totalInputs {
			for inputCode, signalsNeeded := range signalsNeededByInput {
				if !utils.StringInSlice(inputCode, savedInputs) {
					if signalsReceivedByInstance, found := signalsReceivedByInput[inputCode]; found {
						if signalsNeeded != len(signalsReceivedByInstance) {
							everyInputAllSignals = false
						}
					} else {
						everyInputAllSignals = false
					}
				}
			}
		} else {
			everyInputAllSignals = false
		}
	} else {
		everyInputAllSignals = false
	}

	return inputFirstSignal, inputNewSignal, inputAllSignals, everyInputAllSignals
}

// Signals control for multiple datasets.
func MultiDatasetSignalsControl(
	signaledDataset int, 
	signaledInput string, 
	signaledInstance string, 
	signalsByDataset map[int]map[string]map[string]int, 
	signalsNeededByInput map[string]int,
	savedInputs []string,
) (bool, bool, bool, bool) {
	if _, found := signalsByDataset[signaledDataset]; !found {
		signalsByDataset[signaledDataset] = make(map[string]map[string]int)
	}

	if signaledDataset > 0 {
		return signalsControl(signaledInput, signaledInstance, signalsByDataset[signaledDataset], signalsNeededByInput, savedInputs)
	} else {
		return signalsControl(signaledInput, signaledInstance, signalsByDataset[signaledDataset], signalsNeededByInput, make([]string, 0))
	}
}

// Signals control for single dataset.
func SingleDatasetSignalsControl(
	signaledInput string, 
	signaledInstance string, 
	signalsByInput map[string]map[string]int, 
	signalsNeededByInput map[string]int,
) (bool, bool, bool, bool) {
	return signalsControl(signaledInput, signaledInstance, signalsByInput, signalsNeededByInput, make([]string, 0))
}

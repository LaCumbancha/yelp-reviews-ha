package processing

import (
	"sync"
	"strconv"
	log "github.com/sirupsen/logrus"
)

// Defining message storage code
func MessageStorageId(nodeCode string, instance string, bulk int) string {
	return nodeCode + "." + instance + "." + strconv.Itoa(bulk)
}

// Common save process for Aggregators, Joiners and Prettiers.
func ValidateDataSaving(
	datasetNumber int,
	messageId string,
	rawData string,
	savedDataset *int,
	messagesReceived map[string]bool,
	messagesReceivedMutex *sync.Mutex,
	clearCallback func(),
	saveCallback func(string),
) {
	if datasetNumber != *savedDataset {
		log.Infof("Clearing storage due to new dataset received (old: #%d; new: #%d).", *savedDataset, datasetNumber)
		clearCallback()
		*savedDataset = datasetNumber
	}

	messagesReceivedMutex.Lock()

	if _, found := messagesReceived[messageId]; found {
		log.Warnf("Bulk #%s was already received and processed.", messageId)
	} else {
		saveCallback(rawData)
		messagesReceived[messageId] = true
	}
	
	messagesReceivedMutex.Unlock()
}

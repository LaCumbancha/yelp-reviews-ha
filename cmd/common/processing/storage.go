package processing

import (
	"sync"
	"strconv"
	log "github.com/sirupsen/logrus"
)

// Defining message storage code
func MessageSavingId(nodeCode string, dataset int, bulk int) string {
	return nodeCode + "." + strconv.Itoa(dataset) + "." + strconv.Itoa(bulk)
}

// Common save process for Aggregators, Joiners and Prettiers.
func ValidateDataSaving(
	dataset int,
	messageId string,
	rawData string,
	savedDataset *int,
	dataMutex *sync.Mutex,
	messagesReceived map[string]bool,
	messagesReceivedMutex *sync.Mutex,
	storeCallback func(string),
) {
	if dataset != *savedDataset {
		log.Warnf("Data from wrong dataset received (working with #%d but received from #%d).", *savedDataset, dataset)
		*savedDataset = dataset
	}

	messagesReceivedMutex.Lock()
	dataMutex.Lock()

	if _, found := messagesReceived[messageId]; found {
		log.Warnf("Bulk #%s was already received and processed.", messageId)
	} else {
		messagesReceived[messageId] = true
		storeCallback(rawData)
	}

	dataMutex.Unlock()
	messagesReceivedMutex.Unlock()
}

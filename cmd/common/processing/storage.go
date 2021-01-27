package processing

import (
	"sync"
	log "github.com/sirupsen/logrus"
)

// Common save process for Aggregators, Joiners and Prettiers.
func ValidateDataSaving(
	datasetNumber int,
	bulkNumber int,
	rawData string,
	savedDataset *int,
	bulksReceived map[int]bool,
	bulksReceivedMutex *sync.Mutex,
	clearCallback func(),
	saveCallback func(string),
) {
	if datasetNumber != *savedDataset {
		log.Infof("Clearing storage due to new dataset received (old: #%d; new: #%d).", *savedDataset, datasetNumber)
		clearCallback()
		*savedDataset = datasetNumber
	}

	bulksReceivedMutex.Lock()

	if _, found := bulksReceived[bulkNumber]; found {
		log.Warnf("Bulk #%d was already received and processed.", bulkNumber)
	} else {
		saveCallback(rawData)
		bulksReceived[bulkNumber] = true
	}
	
	bulksReceivedMutex.Unlock()
}

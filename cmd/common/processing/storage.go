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
	rawData string,
	savedDataset *int,
	dataMutex *sync.Mutex,
	storeCallback func(string),
) {
	if dataset != *savedDataset {
		log.Warnf("Data from wrong dataset received (working with #%d but received from #%d).", *savedDataset, dataset)
	} else {
		dataMutex.Lock()
		storeCallback(rawData)
		dataMutex.Unlock()
	}
}

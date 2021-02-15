package core

import (
	"fmt"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type backupData struct {
	Data				map[string]map[string]int
	Received			map[string]bool
	Dataset				int
}

type Calculator struct {
	data 				map[string]map[string]int
	dataMutex 			*sync.Mutex
	received			map[string]bool
	receivedMutex 		*sync.Mutex
	dataset				int
	bulkSize			int
}

func loadBackup() (map[string]map[string]int, map[string]bool, int) {
	var backup backupData
	data := make(map[string]map[string]int)
	received := make(map[string]bool)
	dataset	:= proc.DefaultDataset

	backupBytes := proc.LoadBackup(proc.DataBkp)
	if backupBytes != nil {
		json.Unmarshal([]byte(backupBytes), &backup)
		data = backup.Data
		received = backup.Received
		dataset = backup.Dataset
		log.Infof("Aggregator data restored from backup file. Users with texts loaded: %d (%d messages).", len(data), len(received))
	}

	return data, received, dataset
}

func NewCalculator(bulkSize int) *Calculator {
	data, received, dataset := loadBackup()
	
	calculator := &Calculator {
		data:				data,
		dataMutex:			&sync.Mutex{},
		received:			received,
		receivedMutex:		&sync.Mutex{},
		dataset:			dataset,
		bulkSize:			bulkSize,
	}

	return calculator
}

func (calculator *Calculator) Clear(newDataset int) {
	calculator.dataMutex.Lock()
	calculator.data = make(map[string]map[string]int)
	calculator.dataMutex.Unlock()

	calculator.receivedMutex.Lock()
	calculator.received = make(map[string]bool)
	calculator.receivedMutex.Unlock()

	calculator.dataset = newDataset

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) Save(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		proc.MessageSavingId(inputNode, instance, bulk),
		rawData,
		&calculator.dataset,
		calculator.dataMutex,
		calculator.received,
		calculator.receivedMutex,
		calculator.Clear,
		calculator.saveData,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Aggregator: %d users with texts stored.", dataset, bulk, len(calculator.data)), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveData(rawData string) {
	var hashedDataList []comms.HashedTextData
	json.Unmarshal([]byte(rawData), &hashedDataList)

	// Storing data
	for _, hashedData := range hashedDataList {
		if userTexts, found := calculator.data[hashedData.UserId]; found {
			if _, found := userTexts[hashedData.HashedText]; !found { 
				userTexts[hashedData.HashedText] = 1
			}
		} else {
			calculator.data[hashedData.UserId] = make(map[string]int)
			calculator.data[hashedData.UserId][hashedData.HashedText] = 1
		}
	}

	// Updating backup
	backup := &backupData { Data: calculator.data, Received: calculator.received, Dataset: calculator.dataset }
	backupBytes, err := json.Marshal(backup)

	if err != nil {
		log.Errorf("Error serializing Aggregator backup. Err: %s", err)
	} else {
		proc.StoreBackup(proc.DataBkp, backupBytes)
	}
}

func (calculator *Calculator) AggregateData(dataset int) [][]comms.HashedTextData {
	if dataset != calculator.dataset {
		log.Warnf("Aggregating data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, dataset)
		return make([][]comms.HashedTextData, 0)
	}
	
	bulk := make([]comms.HashedTextData, 0)
	bulkedList := make([][]comms.HashedTextData, 0)

	actualBulk := 0
	for userId, hashes := range calculator.data {
		for hash, _ := range hashes {
			actualBulk++
			aggregatedData := comms.HashedTextData { UserId: userId, HashedText: hash }
			bulk = append(bulk, aggregatedData)

			if actualBulk == calculator.bulkSize {
				bulkedList = append(bulkedList, bulk)
				bulk = make([]comms.HashedTextData, 0)
				actualBulk = 0
			}
		}
	}

	if len(bulk) != 0 {
		bulkedList = append(bulkedList, bulk)
	}

	return bulkedList
}

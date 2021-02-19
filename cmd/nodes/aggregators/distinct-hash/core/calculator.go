package core

import (
	"fmt"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type DistinctHash struct {
	Hash 				string
	Unique				bool
	Reviews 			int
}

type Calculator struct {
	data 				map[string]DistinctHash
	dataMutex 			*sync.Mutex
	dataset				int
	bulkSize			int
}

func NewCalculator(bulkSize int) *Calculator {
	calculator := &Calculator {
		data:				make(map[string]DistinctHash),
		dataMutex:			&sync.Mutex{},
		dataset:			proc.DefaultDataset,
		bulkSize:			bulkSize,
	}

	for _, backupData := range bkp.LoadSingleFlowDataBackup() {
		calculator.dataMutex.Lock()
		calculator.saveData(backupData)
		calculator.dataMutex.Unlock()
	}

	return calculator
}

func (calculator *Calculator) Clear(newDataset int) {
	calculator.dataMutex.Lock()
	calculator.data = make(map[string]DistinctHash)
	calculator.dataMutex.Unlock()

	calculator.dataset = newDataset

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) Save(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		rawData,
		&calculator.dataset,
		calculator.dataMutex,
		calculator.saveData,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Aggregator: %d users stored.", dataset, bulk, len(calculator.data)), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveData(rawData string) {
	var hashedDataList []comms.HashedTextData
	json.Unmarshal([]byte(rawData), &hashedDataList)

	// Storing data
	for _, hashedData := range hashedDataList {
		if distinctHashes, found := calculator.data[hashedData.UserId]; found {
			if distinctHashes.Unique && hashedData.HashedText != distinctHashes.Hash {
				calculator.data[hashedData.UserId] = DistinctHash{ Hash: hashedData.HashedText, Unique: false, Reviews: distinctHashes.Reviews + 1 }
			}
		} else {
			calculator.data[hashedData.UserId] = DistinctHash{ Hash: hashedData.HashedText, Unique: true, Reviews: 1 }
		}
	}

	// Updating backup
	bkp.StoreSingleFlowDataBackup(rawData)
}

func (calculator *Calculator) AggregateData(dataset int) [][]comms.UserData {
	if dataset != calculator.dataset {
		log.Warnf("Aggregating data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, dataset)
		return make([][]comms.UserData, 0)
	}

	bulk := make([]comms.UserData, 0)
	bulkedList := make([][]comms.UserData, 0)

	actualBulk := 0
	for userId, distinctHashes := range calculator.data {
		actualBulk++

		if distinctHashes.Unique {
			aggregatedData := comms.UserData { UserId: userId, Reviews: distinctHashes.Reviews }
			bulk = append(bulk, aggregatedData)

			if actualBulk == calculator.bulkSize {
				bulkedList = append(bulkedList, bulk)
				bulk = make([]comms.UserData, 0)
				actualBulk = 0
			}
		}
		
	}

	if len(bulk) != 0 {
		bulkedList = append(bulkedList, bulk)
	}

	return bulkedList
}

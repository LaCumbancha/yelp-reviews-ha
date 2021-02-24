package core

import (
	"fmt"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/yelp-review-ha/cmd/common/logger"
	comms "github.com/LaCumbancha/yelp-review-ha/cmd/common/communication"
)

type CalculatorData map[int]map[string]DistinctHash

type DistinctHash struct {
	Hash 			string
	Unique			bool
	Reviews 		int
}

type Calculator struct {
	data 			CalculatorData
	mutex 			*sync.Mutex
	minReviews		int
}

func NewCalculator(minReviews int) *Calculator {
	calculator := &Calculator {
		data:			make(CalculatorData),
		mutex:			&sync.Mutex{},
		minReviews:		minReviews,
	}

	return calculator
}

// This function doesn't need concurrency control because it will be runned just once at the beggining of the execution, when there's just one goroutine.
func (calculator *Calculator) LoadBackup(dataset int, backups []string) {
	if _, found := calculator.data[dataset]; !found {
		calculator.data[dataset] = make(map[string]DistinctHash)
	}

	for _, backup := range backups {
		calculator.saveData(dataset, backup)
	}
	
	for dataset, datasetData := range calculator.data {
		log.Infof("Dataset #%d retrieved from backup, with %d users.", dataset, len(datasetData))
	}
}

func (calculator *Calculator) Clear(dataset int) {
	calculator.mutex.Lock()

	if _, found := calculator.data[dataset]; found {
		delete(calculator.data, dataset)
		log.Infof("Dataset #%d removed from Calculator storage.", dataset)
	} else {
		log.Infof("Attempting to remove dataset #%d from Calculator storage but it wasn't registered.", dataset)
	}
	
	
	calculator.mutex.Unlock()
}

func (calculator *Calculator) RegisterDataset(dataset int) {
	calculator.mutex.Lock()

	if _, found := calculator.data[dataset]; !found {
		calculator.data[dataset] = make(map[string]DistinctHash)
		log.Infof("Dataset %d initialized in Calculator.", dataset)
	} else {
		log.Warnf("Dataset %d was already initialized in Calculator.", dataset)
	}
	
	calculator.mutex.Unlock()
}

func (calculator *Calculator) Save(dataset int, bulk int, rawData string) {
	calculator.mutex.Lock()
	datasetDataLength := calculator.saveData(dataset, rawData)
	calculator.mutex.Unlock()

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Aggregator: %d users stored.", dataset, bulk, datasetDataLength), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveData(dataset int, rawData string) int {
	var hashedDataList []comms.HashedTextData
	json.Unmarshal([]byte(rawData), &hashedDataList)

	// Retrieving dataset
	datasetData, found := calculator.data[dataset]
	if !found {
		calculator.data[dataset] = make(map[string]DistinctHash)
		log.Warnf("Data received from a dataset not initialized: %d.", dataset)
		datasetData = calculator.data[dataset]
	}

	// Storing data
	for _, hashedData := range hashedDataList {
		if distinctHashes, found := datasetData[hashedData.UserId]; found {
			if distinctHashes.Unique && hashedData.HashedText != distinctHashes.Hash {
				datasetData[hashedData.UserId] = DistinctHash{ Hash: hashedData.HashedText, Unique: false, Reviews: distinctHashes.Reviews + 1 }
			} else {
				datasetData[hashedData.UserId] = DistinctHash{ Hash: hashedData.HashedText, Unique: distinctHashes.Unique, Reviews: distinctHashes.Reviews + 1 }
			}
		} else {
			datasetData[hashedData.UserId] = DistinctHash{ Hash: hashedData.HashedText, Unique: true, Reviews: 1 }
		}
	}

	return len(datasetData)
}

func (calculator *Calculator) AggregateData(dataset int) []comms.UserData {
	calculator.mutex.Lock()

	datasetData, found := calculator.data[dataset]
	if !found {
		log.Warnf("Aggregating data for a dataset not stored (#%d).", dataset)
		return make([]comms.UserData, 0)
	}

	var list []comms.UserData
	for userId, distinctHashes := range datasetData {
		if distinctHashes.Unique && distinctHashes.Reviews >= calculator.minReviews {
			aggregatedData := comms.UserData { UserId: userId, Reviews: distinctHashes.Reviews }
			list = append(list, aggregatedData)
		}	
	}

	calculator.mutex.Unlock()
	log.Infof("Bot users detected: %d.", len(list))
	return list
}

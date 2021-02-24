package core

import (
	"fmt"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type CalculatorData map[int]map[string]int

type Calculator struct {
	data 			CalculatorData
	mutex 			*sync.Mutex
	bulkSize		int
}

func NewCalculator(bulkSize int) *Calculator {
	calculator := &Calculator {
		data:			make(CalculatorData),
		mutex:			&sync.Mutex{},
		bulkSize:		bulkSize,
	}

	return calculator
}

// This function doesn't need concurrency control because it will be runned just once at the beggining of the execution, when there's just one goroutine.
func (calculator *Calculator) LoadBackup(dataset int, backups []string) {
	if _, found := calculator.data[dataset]; !found {
		calculator.data[dataset] = make(map[string]int)
	}

	for _, backup := range backups {
		calculator.saveData(dataset, backup)
	}
	
	for dataset, datasetData := range calculator.data {
		log.Infof("Dataset #%d retrieved from backup, with %d businesses.", dataset, len(datasetData))
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
		calculator.data[dataset] = make(map[string]int)
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

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Aggregator: %d businesses stored.", dataset, bulk, datasetDataLength), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveData(dataset int, rawData string) int {
	var funbizDataList []comms.FunnyBusinessData
	json.Unmarshal([]byte(rawData), &funbizDataList)

	// Retrieving dataset
	datasetData, found := calculator.data[dataset]
	if !found {
		calculator.data[dataset] = make(map[string]int)
		log.Warnf("Data received from a dataset not initialized: %d.", dataset)
		datasetData = calculator.data[dataset]
	}

	// Storing data
	for _, funbizData := range funbizDataList {
		if value, found := datasetData[funbizData.BusinessId]; found {
			newAmount := value + 1
		    datasetData[funbizData.BusinessId] = newAmount
		} else {
			datasetData[funbizData.BusinessId] = 1
		}
	}

	return len(datasetData)
}

func (calculator *Calculator) AggregateData(dataset int) [][]comms.FunnyBusinessData {
	calculator.mutex.Lock()

	datasetData, found := calculator.data[dataset]
	if !found {
		log.Warnf("Aggregating data for a dataset not stored (#%d).", dataset)
		return make([][]comms.FunnyBusinessData, 0)
	}
	
	bulk := make([]comms.FunnyBusinessData, 0)
	bulkedList := make([][]comms.FunnyBusinessData, 0)

	actualBulk := 0
	for businessId, funny := range datasetData {
		actualBulk++
		aggregatedData := comms.FunnyBusinessData { BusinessId: businessId, Funny: funny }
		bulk = append(bulk, aggregatedData)

		if actualBulk == calculator.bulkSize {
			bulkedList = append(bulkedList, bulk)
			bulk = make([]comms.FunnyBusinessData, 0)
			actualBulk = 0
		}
	}

	if len(bulk) != 0 {
		bulkedList = append(bulkedList, bulk)
	}

	calculator.mutex.Unlock()
	return bulkedList
}

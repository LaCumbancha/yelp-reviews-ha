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
	Data				map[string]int
	Dataset				int
}

type Calculator struct {
	data 				map[string]int
	dataMutex 			*sync.Mutex
	dataset				int
	bulkSize			int
}

func loadBackup() (map[string]int, int) {
	var backup backupData
	data := make(map[string]int)
	dataset	:= proc.DefaultDataset

	backupBytes := proc.LoadBackup(proc.DataBkp)
	if backupBytes != nil {
		json.Unmarshal([]byte(backupBytes), &backup)
		data = backup.Data
		dataset = backup.Dataset
		log.Infof("Aggregator data restored from backup file. Users loaded: %d.", len(data))
	}

	return data, dataset
}

func NewCalculator(bulkSize int) *Calculator {
	data, dataset := loadBackup()

	calculator := &Calculator {
		data:				data,
		dataMutex:			&sync.Mutex{},
		dataset:			dataset,
		bulkSize:			bulkSize,
	}

	return calculator
}

func (calculator *Calculator) Clear(newDataset int) {
	calculator.dataMutex.Lock()
	calculator.data = make(map[string]int)
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
		if value, found := calculator.data[hashedData.UserId]; found {
			newValue := value + 1
	    	calculator.data[hashedData.UserId] = newValue
		} else {
			calculator.data[hashedData.UserId] = 1
		}
	}

	// Updating backup
	backup := &backupData { Data: calculator.data, Dataset: calculator.dataset }
	backupBytes, err := json.Marshal(backup)

	if err != nil {
		log.Errorf("Error serializing Aggregator backup. Err: %s", err)
	} else {
		proc.StoreBackup(proc.DataBkp, backupBytes)
	}
}

func (calculator *Calculator) AggregateData(dataset int) [][]comms.DishashData {
	if dataset != calculator.dataset {
		log.Warnf("Aggregating data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, dataset)
		return make([][]comms.DishashData, 0)
	}

	bulk := make([]comms.DishashData, 0)
	bulkedList := make([][]comms.DishashData, 0)

	actualBulk := 0
	for userId, distinctHashes := range calculator.data {
		actualBulk++
		aggregatedData := comms.DishashData { UserId: userId, Distinct: distinctHashes }
		bulk = append(bulk, aggregatedData)

		if actualBulk == calculator.bulkSize {
			bulkedList = append(bulkedList, bulk)
			bulk = make([]comms.DishashData, 0)
			actualBulk = 0
		}
	}

	if len(bulk) != 0 {
		bulkedList = append(bulkedList, bulk)
	}

	return bulkedList
}

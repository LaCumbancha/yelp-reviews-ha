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
		log.Infof("Aggregator data restored from backup file. Businesses loaded: %d (%d messages).", len(data))
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

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Aggregator: %d businesses stored.", dataset, bulk, len(calculator.data)), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveData(rawData string) {
	var funbizDataList []comms.FunnyBusinessData
	json.Unmarshal([]byte(rawData), &funbizDataList)

	// Storing data
	for _, funbizData := range funbizDataList {
		if value, found := calculator.data[funbizData.BusinessId]; found {
			newAmount := value + 1
		    calculator.data[funbizData.BusinessId] = newAmount
		} else {
			calculator.data[funbizData.BusinessId] = 1
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

func (calculator *Calculator) AggregateData(dataset int) [][]comms.FunnyBusinessData {
	if dataset != calculator.dataset {
		log.Warnf("Aggregating data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, dataset)
		return make([][]comms.FunnyBusinessData, 0)
	}
	
	bulk := make([]comms.FunnyBusinessData, 0)
	bulkedList := make([][]comms.FunnyBusinessData, 0)

	actualBulk := 0
	for businessId, funny := range calculator.data {
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

	return bulkedList
}

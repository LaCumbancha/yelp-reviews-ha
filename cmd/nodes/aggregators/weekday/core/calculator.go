package core

import (
	"fmt"
	"sync"
	"strings"
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
}

func loadBackup() (map[string]int, int) {
	var backup backupData
	data := make(map[string]int)
	dataset	:= proc.DefaultDataset

	backupBytes := proc.LoadBackup(proc.DataBkp)
	if backupBytes != nil {
		json.Unmarshal(backupBytes, &backup)
		data = backup.Data
		dataset = backup.Dataset
		log.Infof("Aggregator data restored from backup file. Weekdays loaded: %d.", len(data))
	}

	return data, dataset
}

func NewCalculator() *Calculator {
	data, dataset := loadBackup()
	
	calculator := &Calculator {
		data:				data,
		dataMutex:			&sync.Mutex{},
		dataset:			dataset,
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

func (calculator *Calculator) status(dataset int, bulk int) string {
	statusResponse := fmt.Sprintf("Status by bulk #%d.%d: ", dataset, bulk)

	calculator.dataMutex.Lock()
	for weekday, reviews := range calculator.data {
		statusResponse += strings.ToUpper(fmt.Sprintf("%s (%d) ; ", weekday[0:3], reviews))
	}
	calculator.dataMutex.Unlock()

	return statusResponse[0:len(statusResponse)-3]
}

func (calculator *Calculator) Save(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		rawData,
		&calculator.dataset,
		calculator.dataMutex,
		calculator.saveData,
	)

	logb.Instance().Infof(calculator.status(dataset, bulk), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveData(rawData string) {
	var weekdayDataList []comms.WeekdayData
	json.Unmarshal([]byte(rawData), &weekdayDataList)

	// Storing data
	for _, weekdayData := range weekdayDataList {
		if value, found := calculator.data[weekdayData.Weekday]; found {
			newAmount := value + 1
		    calculator.data[weekdayData.Weekday] = newAmount
		} else {
			calculator.data[weekdayData.Weekday] = 1
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

func (calculator *Calculator) AggregateData(dataset int) []comms.WeekdayData {
	if dataset != calculator.dataset {
		log.Warnf("Aggregating data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, dataset)
		return make([]comms.WeekdayData, 0)
	}
	
	var list []comms.WeekdayData
	for weekday, reviews := range calculator.data {
		log.Infof("%s reviews aggregated: %d.", weekday, reviews)
		aggregatedData := comms.WeekdayData { Weekday: weekday, Reviews: reviews }
		list = append(list, aggregatedData)
	}

	return list
}

package core

import (
	"fmt"
	"sync"
	"strings"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type CalculatorData map[int]map[string]int

type Calculator struct {
	data 			CalculatorData
	mutex 			*sync.Mutex
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data:			make(CalculatorData),
		mutex:			&sync.Mutex{},
	}

	calculator.loadBackup()

	return calculator
}

// This function doesn't need concurrency control because it will be runned just once at the beggining of the execution, when there's just one goroutine.
func (calculator *Calculator) loadBackup() {
	for _, backupData := range bkp.LoadDataBackup() {
		calculator.saveData(backupData.Dataset, backupData.Data)
	}

	for dataset, _ := range calculator.data {
		log.Infof("Dataset #%d retrieved from backup. Status: %s.", dataset, calculator.status(dataset))
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

func (calculator *Calculator) status(dataset int) string {
	statusResponse := ""

	calculator.mutex.Lock()
	for weekday, reviews := range calculator.data[dataset] {
		statusResponse += strings.ToUpper(fmt.Sprintf("%s (%d) ; ", weekday[0:3], reviews))
	}
	calculator.mutex.Unlock()

	if len(statusResponse) < 3 {
		return "No data"
	} else {
		return statusResponse[0:len(statusResponse)-3]
	}
}

func (calculator *Calculator) Save(dataset int, bulk int, rawData string) {
	calculator.mutex.Lock()
	calculator.saveData(dataset, rawData)
	calculator.mutex.Unlock()

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d: %s", dataset, bulk, calculator.status(dataset)), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveData(dataset int, rawData string) int {
	var weekdayDataList []comms.WeekdayData
	json.Unmarshal([]byte(rawData), &weekdayDataList)

	// Retrieving dataset
	datasetData, found := calculator.data[dataset]
	if !found {
		calculator.data[dataset] = make(map[string]int)
		log.Warnf("Data received from a dataset not initialized: %d.", dataset)
		datasetData = calculator.data[dataset]
	}

	// Storing data
	for _, weekdayData := range weekdayDataList {
		if value, found := datasetData[weekdayData.Weekday]; found {
			newAmount := value + 1
		    datasetData[weekdayData.Weekday] = newAmount
		} else {
			datasetData[weekdayData.Weekday] = 1
		}
	}

	// Updating backup
	bkp.StoreSingleFlowDataBackup(dataset, rawData)

	return len(datasetData)
}

func (calculator *Calculator) AggregateData(dataset int) []comms.WeekdayData {
	calculator.mutex.Lock()

	datasetData, found := calculator.data[dataset]
	if !found {
		log.Warnf("Aggregating data for a dataset not stored (#%d).", dataset)
		return make([]comms.WeekdayData, 0)
	}
	
	var list []comms.WeekdayData
	for weekday, reviews := range datasetData {
		log.Infof("%s reviews aggregated: %d.", weekday, reviews)
		aggregatedData := comms.WeekdayData { Weekday: weekday, Reviews: reviews }
		list = append(list, aggregatedData)
	}

	calculator.mutex.Unlock()
	return list
}

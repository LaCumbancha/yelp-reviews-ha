package core

import (
	"fmt"
	"sync"
	"strings"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Calculator struct {
	data 				map[string]int
	dataMutex 			*sync.Mutex
	dataset				int
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data:				make(map[string]int),
		dataMutex:			&sync.Mutex{},
		dataset:			proc.DefaultDataset,
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
	bkp.StoreSingleFlowDataBackup(rawData)
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

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

type Calculator struct {
	data 				map[string]int
	dataMutex 			*sync.Mutex
	received			map[int]bool
	receivedMutex 		*sync.Mutex
	dataset				int
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data:				make(map[string]int),
		dataMutex:			&sync.Mutex{},
		received:			make(map[int]bool),
		receivedMutex:		&sync.Mutex{},
		dataset:			comms.DefaultDataset,
	}

	return calculator
}

func (calculator *Calculator) Clear() {
	calculator.dataMutex.Lock()
	calculator.data = make(map[string]int)
	calculator.dataMutex.Unlock()

	calculator.receivedMutex.Lock()
	calculator.received = make(map[int]bool)
	calculator.receivedMutex.Unlock()

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) status(datasetNumber int, bulkNumber int) string {
	statusResponse := fmt.Sprintf("Status by bulk #%d.%d: ", datasetNumber, bulkNumber)

	calculator.dataMutex.Lock()
	for weekday, reviews := range calculator.data {
		statusResponse += strings.ToUpper(fmt.Sprintf("%s (%d) ; ", weekday[0:3], reviews))
	}
	calculator.dataMutex.Unlock()

	return statusResponse[0:len(statusResponse)-3]
}

func (calculator *Calculator) Save(datasetNumber int, bulkNumber int, rawData string) {
	proc.ValidateDataSaving(
		datasetNumber,
		bulkNumber,
		rawData,
		&calculator.dataset,
		calculator.received,
		calculator.receivedMutex,
		calculator.Clear,
		calculator.saveData,
	)

	logb.Instance().Infof(calculator.status(datasetNumber, bulkNumber), bulkNumber)
}

func (calculator *Calculator) saveData(rawWeekdayDataBulk string) {
	var weekdayDataList []comms.WeekdayData
	json.Unmarshal([]byte(rawWeekdayDataBulk), &weekdayDataList)

	for _, weekdayData := range weekdayDataList {

		calculator.dataMutex.Lock()
		if value, found := calculator.data[weekdayData.Weekday]; found {
			newAmount := value + 1
		    calculator.data[weekdayData.Weekday] = newAmount
		} else {
			calculator.data[weekdayData.Weekday] = 1
		}
		calculator.dataMutex.Unlock()

	}
}

func (calculator *Calculator) AggregateData(datasetNumber int) []comms.WeekdayData {
	if datasetNumber != calculator.dataset {
		log.Warnf("Aggregating data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, datasetNumber)
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

package common

import (
	"fmt"
	"sync"
	"strings"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Calculator struct {
	data 			map[string]int
	mutex 			*sync.Mutex
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data:		make(map[string]int),
		mutex:		&sync.Mutex{},
	}

	return calculator
}

func (calculator *Calculator) Clear() {
	calculator.mutex.Lock()
	calculator.data = make(map[string]int)
	calculator.mutex.Unlock()

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) status(bulkNumber int) string {
	statusResponse := fmt.Sprintf("Status by bulk #%d: ", bulkNumber)

	calculator.mutex.Lock()
	for weekday, reviews := range calculator.data {
		statusResponse += strings.ToUpper(fmt.Sprintf("%s (%d) ; ", weekday[0:3], reviews))
	}
	calculator.mutex.Unlock()

	return statusResponse[0:len(statusResponse)-3]
}

func (calculator *Calculator) Aggregate(bulkNumber int, rawWeekdayDataBulk string) {
	var weekdayDataList []comms.WeekdayData
	json.Unmarshal([]byte(rawWeekdayDataBulk), &weekdayDataList)

	for _, weekdayData := range weekdayDataList {

		calculator.mutex.Lock()
		if value, found := calculator.data[weekdayData.Weekday]; found {
			newAmount := value + 1
		    calculator.data[weekdayData.Weekday] = newAmount
		} else {
			calculator.data[weekdayData.Weekday] = 1
		}
		calculator.mutex.Unlock()

	}

	logb.Instance().Infof(calculator.status(bulkNumber), bulkNumber)
}

func (calculator *Calculator) RetrieveData() []comms.WeekdayData {
	var list []comms.WeekdayData

	for weekday, reviews := range calculator.data {
		log.Infof("%s reviews aggregated: %d.", weekday, reviews)
		aggregatedData := comms.WeekdayData { Weekday: weekday, Reviews: reviews }
		list = append(list, aggregatedData)
	}

	return list
}

package core

import (
	"fmt"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Calculator struct {
	data 			map[string]int
	mutex 			*sync.Mutex
	bulkSize		int
}

func NewCalculator(bulkSize int) *Calculator {
	calculator := &Calculator {
		data:		make(map[string]int),
		mutex:		&sync.Mutex{},
		bulkSize:	bulkSize,
	}

	return calculator
}

func (calculator *Calculator) Clear() {
	calculator.mutex.Lock()
	calculator.data = make(map[string]int)
	calculator.mutex.Unlock()

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) Aggregate(bulkNumber int, rawFunbizDataList string) {
	var funbizDataList []comms.FunnyBusinessData
	json.Unmarshal([]byte(rawFunbizDataList), &funbizDataList)

	for _, funbizData := range funbizDataList {

		calculator.mutex.Lock()
		if value, found := calculator.data[funbizData.BusinessId]; found {
			newAmount := value + 1
		    calculator.data[funbizData.BusinessId] = newAmount
		} else {
			calculator.data[funbizData.BusinessId] = 1
		}
		calculator.mutex.Unlock()

	}

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d: %d businesses stored.", bulkNumber, len(calculator.data)), bulkNumber)
}

func (calculator *Calculator) RetrieveData() [][]comms.FunnyBusinessData {
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

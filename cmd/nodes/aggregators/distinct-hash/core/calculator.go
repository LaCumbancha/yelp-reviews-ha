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

func (calculator *Calculator) Aggregate(bulkNumber int, rawHashedDataBulk string) {
	var hashedDataList []comms.HashedTextData
	json.Unmarshal([]byte(rawHashedDataBulk), &hashedDataList)

	for _, hashedData := range hashedDataList {

		calculator.mutex.Lock()
		if value, found := calculator.data[hashedData.UserId]; found {
			newValue := value + 1
	    	calculator.data[hashedData.UserId] = newValue
		} else {
			calculator.data[hashedData.UserId] = 1
		}
		calculator.mutex.Unlock()

	}

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d: %d users stored.", bulkNumber, len(calculator.data)), bulkNumber)
}

func (calculator *Calculator) RetrieveData() [][]comms.DishashData {
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

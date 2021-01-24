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

func (calculator *Calculator) Aggregate(bulkNumber int, rawFuncitDataList string) {
	var funcitDataList []comms.FunnyCityData
	json.Unmarshal([]byte(rawFuncitDataList), &funcitDataList)

	for _, funcitData := range funcitDataList {

		calculator.mutex.Lock()
		if value, found := calculator.data[funcitData.City]; found {
			newAmount := value + funcitData.Funny
		    calculator.data[funcitData.City] = newAmount
		} else {
			calculator.data[funcitData.City] = funcitData.Funny
		}
		calculator.mutex.Unlock()

	}

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d: %d funny cities stored.", bulkNumber, len(calculator.data)), bulkNumber)
}

func (calculator *Calculator) RetrieveData() [][]comms.FunnyCityData {
	bulk := make([]comms.FunnyCityData, 0)
	bulkedList := make([][]comms.FunnyCityData, 0)

	actualBulk := 0
	for city, funny := range calculator.data {
		actualBulk++
		aggregatedData := comms.FunnyCityData { City: city, Funny: funny }
		bulk = append(bulk, aggregatedData)

		if actualBulk == calculator.bulkSize {
			bulkedList = append(bulkedList, bulk)
			bulk = make([]comms.FunnyCityData, 0)
			actualBulk = 0
		}
	}

	if len(bulk) != 0 {
		bulkedList = append(bulkedList, bulk)
	}

	return bulkedList
}

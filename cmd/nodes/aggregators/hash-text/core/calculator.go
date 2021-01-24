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
	data 			map[string]map[string]int
	mutex 			*sync.Mutex
	bulkSize		int
}

func NewCalculator(bulkSize int) *Calculator {
	calculator := &Calculator {
		data:		make(map[string]map[string]int),
		mutex:		&sync.Mutex{},
		bulkSize:	bulkSize,
	}

	return calculator
}

func (calculator *Calculator) Clear() {
	calculator.mutex.Lock()
	calculator.data = make(map[string]map[string]int)
	calculator.mutex.Unlock()

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) Aggregate(bulkNumber int, rawHashedDataBulk string) {
	var hashedDataList []comms.HashedTextData
	json.Unmarshal([]byte(rawHashedDataBulk), &hashedDataList)

	for _, hashedData := range hashedDataList {

		calculator.mutex.Lock()
		if userTexts, found := calculator.data[hashedData.UserId]; found {
			if _, found := userTexts[hashedData.HashedText]; !found { 
				userTexts[hashedData.HashedText] = 1
			}
		} else {
			calculator.data[hashedData.UserId] = make(map[string]int)
			calculator.data[hashedData.UserId][hashedData.HashedText] = 1
		}
		calculator.mutex.Unlock()

	}

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d: %d users with texts stored.", bulkNumber, len(calculator.data)), bulkNumber)
}

func (calculator *Calculator) RetrieveData() [][]comms.HashedTextData {
	bulk := make([]comms.HashedTextData, 0)
	bulkedList := make([][]comms.HashedTextData, 0)

	actualBulk := 0
	for userId, hashes := range calculator.data {
		for hash, _ := range hashes {
			actualBulk++
			aggregatedData := comms.HashedTextData { UserId: userId, HashedText: hash }
			bulk = append(bulk, aggregatedData)

			if actualBulk == calculator.bulkSize {
				bulkedList = append(bulkedList, bulk)
				bulk = make([]comms.HashedTextData, 0)
				actualBulk = 0
			}
		}
	}

	if len(bulk) != 0 {
		bulkedList = append(bulkedList, bulk)
	}

	return bulkedList
}

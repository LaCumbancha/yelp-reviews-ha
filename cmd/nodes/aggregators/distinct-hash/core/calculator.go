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

type Calculator struct {
	data 				map[string]int
	dataMutex 			*sync.Mutex
	received			map[int]bool
	receivedMutex 		*sync.Mutex
	dataset				int
	bulkSize			int
}

func NewCalculator(bulkSize int) *Calculator {
	calculator := &Calculator {
		data:				make(map[string]int),
		dataMutex:			&sync.Mutex{},
		received:			make(map[int]bool),
		receivedMutex:		&sync.Mutex{},
		dataset:			comms.DefaultDataset,
		bulkSize:			bulkSize,
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

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Aggregator: %d users stored.", datasetNumber, bulkNumber, len(calculator.data)), bulkNumber)
}

func (calculator *Calculator) saveData(rawHashedDataBulk string) {
	var hashedDataList []comms.HashedTextData
	json.Unmarshal([]byte(rawHashedDataBulk), &hashedDataList)

	for _, hashedData := range hashedDataList {

		calculator.dataMutex.Lock()
		if value, found := calculator.data[hashedData.UserId]; found {
			newValue := value + 1
	    	calculator.data[hashedData.UserId] = newValue
		} else {
			calculator.data[hashedData.UserId] = 1
		}
		calculator.dataMutex.Unlock()

	}
}

func (calculator *Calculator) AggregateData(datasetNumber int) [][]comms.DishashData {
	if datasetNumber != calculator.dataset {
		log.Warnf("Aggregating data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, datasetNumber)
		return make([][]comms.DishashData, 0)
	}

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

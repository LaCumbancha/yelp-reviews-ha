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
	received			map[string]bool
	receivedMutex 		*sync.Mutex
	dataset				int
	bulkSize			int
}

func NewCalculator(bulkSize int) *Calculator {
	calculator := &Calculator {
		data:				make(map[string]int),
		dataMutex:			&sync.Mutex{},
		received:			make(map[string]bool),
		receivedMutex:		&sync.Mutex{},
		dataset:			proc.DefaultDataset,
		bulkSize:			bulkSize,
	}

	return calculator
}

func (calculator *Calculator) Clear(newDataset int) {
	calculator.dataMutex.Lock()
	calculator.data = make(map[string]int)
	calculator.dataMutex.Unlock()

	calculator.receivedMutex.Lock()
	calculator.received = make(map[string]bool)
	calculator.receivedMutex.Unlock()

	calculator.dataset = newDataset

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) Save(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		proc.MessageStorageId(inputNode, instance, bulk),
		rawData,
		&calculator.dataset,
		calculator.received,
		calculator.receivedMutex,
		calculator.Clear,
		calculator.saveData,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Aggregator: %d cities stored.", dataset, bulk, len(calculator.data)), bulk)
}

func (calculator *Calculator) saveData(rawData string) {
	var funcitDataList []comms.FunnyCityData
	json.Unmarshal([]byte(rawData), &funcitDataList)

	for _, funcitData := range funcitDataList {

		calculator.dataMutex.Lock()
		if value, found := calculator.data[funcitData.City]; found {
			newAmount := value + funcitData.Funny
		    calculator.data[funcitData.City] = newAmount
		} else {
			calculator.data[funcitData.City] = funcitData.Funny
		}
		calculator.dataMutex.Unlock()

	}
}

func (calculator *Calculator) AggregateData(dataset int) [][]comms.FunnyCityData {
	if dataset != calculator.dataset {
		log.Warnf("Aggregating data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, dataset)
		return make([][]comms.FunnyCityData, 0)
	}
	
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

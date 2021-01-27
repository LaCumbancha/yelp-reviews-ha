package core

import (
	"fmt"
	"sort"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Calculator struct {
	data 				[]comms.FunnyCityData
	dataMutex 			*sync.Mutex
	received			map[int]bool
	receivedMutex 		*sync.Mutex
	dataset				int
	topSize				int
}

func NewCalculator(topSize int) *Calculator {
	calculator := &Calculator {
		data:				[]comms.FunnyCityData{},
		dataMutex:			&sync.Mutex{},
		received:			make(map[int]bool),
		receivedMutex:		&sync.Mutex{},
		dataset:			comms.DefaultDataset,
		topSize:			topSize,
	}

	return calculator
}

func (calculator *Calculator) Clear() {
	calculator.dataMutex.Lock()
	calculator.data = []comms.FunnyCityData{}
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

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Aggregator: %d funny cities stored.", datasetNumber, bulkNumber, len(calculator.data)), bulkNumber)
}

func (calculator *Calculator) saveData(rawFuncitDataList string) {
	var funcitDataList []comms.FunnyCityData
	json.Unmarshal([]byte(rawFuncitDataList), &funcitDataList)

	for _, funcitData := range funcitDataList {
		calculator.dataMutex.Lock()
		calculator.data = append(calculator.data, funcitData)
		calculator.dataMutex.Unlock()
	}
}

func (calculator *Calculator) AggregateData(datasetNumber int) []comms.FunnyCityData {
	if datasetNumber != calculator.dataset {
		log.Warnf("Aggregating data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, datasetNumber)
		return make([]comms.FunnyCityData, 0)
	}
	
	sort.SliceStable(calculator.data, func(cityIdx1, cityIdx2 int) bool {
	    return calculator.data[cityIdx1].Funny > calculator.data[cityIdx2].Funny
	})

	funnyCities := len(calculator.data)
	if (funnyCities > calculator.topSize) {
		log.Infof("%d cities where discarded due to not being funny enoguh.", funnyCities - calculator.topSize)
		return calculator.data[0:calculator.topSize]
	} else {
		log.Infof("They where just %d cities with a funniness higher than 0!", funnyCities)
		return calculator.data[0:funnyCities]
	}
}

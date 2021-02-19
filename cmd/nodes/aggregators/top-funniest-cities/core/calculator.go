package core

import (
	"fmt"
	"sort"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type CalculatorData map[int]map[string]int

type Calculator struct {
	data 			CalculatorData
	mutex 			*sync.Mutex
	topSize			int
}

func NewCalculator(topSize int) *Calculator {
	calculator := &Calculator {
		data:			make(CalculatorData),
		mutex:			&sync.Mutex{},
		topSize:		topSize,
	}

	calculator.loadBackup()

	return calculator
}

// This function doesn't need concurrency control because it will be runned just once at the beggining of the execution, when there's just one goroutine.
func (calculator *Calculator) loadBackup() {
	for _, backupData := range bkp.LoadDataBackup() {
		calculator.saveData(backupData.Dataset, backupData.Data)
	}

	for dataset, datasetData := range calculator.data {
		log.Infof("Dataset #%d retrieved from backup, with %d cities.", dataset, len(datasetData))
	}
}

func (calculator *Calculator) Clear(dataset int) {
	calculator.mutex.Lock()
	if _, found := calculator.data[dataset]; found {
		delete(calculator.data, dataset)
		log.Infof("Dataset #%d removed from Calculator storage.", dataset)
	} else {
		log.Infof("Attempting to remove dataset #%d from Calculator storage but it wasn't registered.", dataset)
	}
	calculator.mutex.Unlock()
}

func (calculator *Calculator) RegisterDataset(dataset int) {
	calculator.mutex.Lock()
	if _, found := calculator.data[dataset]; !found {
		calculator.data[dataset] = make(map[string]int)
		log.Infof("Dataset %d initialized in Calculator.", dataset)
	} else {
		log.Warnf("Dataset %d was already initialized in Calculator.", dataset)
	}
	calculator.mutex.Unlock()
}

func (calculator *Calculator) Save(dataset int, bulk int, rawData string) {
	calculator.mutex.Lock()
	datasetDataLength := calculator.saveData(dataset, rawData)
	calculator.mutex.Unlock()

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Aggregator: %d cities stored.", dataset, bulk, datasetDataLength), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveData(dataset int, rawData string) int {
	var funcitDataList []comms.FunnyCityData
	json.Unmarshal([]byte(rawData), &funcitDataList)

	// Retrieving dataset
	datasetData, found := calculator.data[dataset]
	if !found {
		calculator.data[dataset] = make(map[string]int)
		log.Warnf("Data received from a dataset not initialized: %d.", dataset)
		datasetData = calculator.data[dataset]
	}

	// Storing data
	for _, funcitData := range funcitDataList {
		if value, found := datasetData[funcitData.City]; found {
			newAmount := value + funcitData.Funny
		    datasetData[funcitData.City] = newAmount
		} else {
			datasetData[funcitData.City] = funcitData.Funny
		}
	}

	// Updating backup
	bkp.StoreSingleFlowDataBackup(dataset, rawData)

	return len(datasetData)
}

func (calculator *Calculator) AggregateData(dataset int) []comms.FunnyCityData {
	calculator.mutex.Lock()

	datasetData, found := calculator.data[dataset]
	if !found {
		log.Warnf("Aggregating data for a dataset not stored (#%d).", dataset)
		return make([]comms.FunnyCityData, 0)
	}

	funnyCities := make([]comms.FunnyCityData, 0)
	for city, funny := range datasetData {
		aggregatedData := comms.FunnyCityData { City: city, Funny: funny }
		funnyCities = append(funnyCities, aggregatedData)
	}

	sort.SliceStable(funnyCities, func(cityIdx1, cityIdx2 int) bool {
	    return funnyCities[cityIdx1].Funny > funnyCities[cityIdx2].Funny
	})

	totalFunnyCities := len(funnyCities)
	var maxCityNumber int
	if (totalFunnyCities > calculator.topSize) {
		log.Infof("%d cities where discarded due to not being funny enoguh.", totalFunnyCities - calculator.topSize)
		maxCityNumber = calculator.topSize
	} else {
		log.Infof("They where just %d cities with a funniness higher than 0!", totalFunnyCities)
		maxCityNumber = totalFunnyCities
		
	}

	calculator.mutex.Unlock()
	return funnyCities[0:maxCityNumber]
}

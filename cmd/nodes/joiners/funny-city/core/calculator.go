package core

import (
	"fmt"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/yelp-review-ha/cmd/common/logger"
	comms "github.com/LaCumbancha/yelp-review-ha/cmd/common/communication"
)

type CalculatorData1 map[int]map[string]int
type CalculatorData2 map[string]string

type Calculator struct {
	data1 			CalculatorData1
	data2 			CalculatorData2
	mutex1 			*sync.Mutex
	mutex2 			*sync.Mutex
	maxBulkSize		int
}

func NewCalculator(bulkSize int) *Calculator {
	calculator := &Calculator {
		data1:			make(CalculatorData1),
		data2:			make(CalculatorData2),
		mutex1:			&sync.Mutex{},
		mutex2:			&sync.Mutex{},
		maxBulkSize:	bulkSize,
	}

	return calculator
}

// This function doesn't need concurrency control because it will be runned just once at the beggining of the execution, when there's just one goroutine.
func (calculator *Calculator) LoadFunbizBackup(dataset int, backups []string) {
	if _, found := calculator.data1[dataset]; !found {
		calculator.data1[dataset] = make(map[string]int)
	}

	for _, backup := range backups {
		calculator.saveFunnyBusiness(dataset, backup)
	}
	
	for dataset1, datasetData1 := range calculator.data1 {
		log.Infof("Dataset #%d retrieved from backup, with %d funny businesses.", dataset1, len(datasetData1))
	}
}

// This function doesn't need concurrency control because it will be runned just once at the beggining of the execution, when there's just one goroutine.
func (calculator *Calculator) LoadCitbizBackup(dataset int, backups []string) {
	for _, backup := range backups {
		calculator.saveCityBusiness(backup)
	}
	
	log.Infof("Retrieving %d city businesses from backup.", len(calculator.data2))
}

// Just the funny-businesses information is cleared because city-businesses data is needed for futures datasets.
func (calculator *Calculator) Clear(dataset int) {
	calculator.mutex1.Lock()

	if _, found := calculator.data1[dataset]; found {
		delete(calculator.data1, dataset)
		log.Infof("Dataset #%d removed from Calculator storage #1.", dataset)
	} else {
		log.Infof("Attempting to remove dataset #%d from Calculator storage #1 but it wasn't registered.", dataset)
	}

	calculator.mutex1.Unlock()
}

func (calculator *Calculator) RegisterDataset(dataset int) {
	calculator.mutex1.Lock()

	if _, found := calculator.data1[dataset]; !found {
		calculator.data1[dataset] = make(map[string]int)
		log.Infof("Dataset %d initialized in Calculator storage #1.", dataset)
	} else {
		log.Warnf("Dataset %d was already initialized in Calculator storage #1.", dataset)
	}

	calculator.mutex1.Unlock()
}

func (calculator *Calculator) AddFunnyBusiness(dataset int, bulk int, rawData string) {
	calculator.mutex1.Lock()
	datasetDataLength := calculator.saveFunnyBusiness(dataset, rawData)
	calculator.mutex1.Unlock()

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d funny businesses stored.", dataset, bulk, datasetDataLength), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveFunnyBusiness(dataset int, rawData string) int {
	var funbizDataList []comms.FunnyBusinessData
	json.Unmarshal([]byte(rawData), &funbizDataList)

	// Retrieving dataset
	datasetData, found := calculator.data1[dataset]
	if !found {
		calculator.data1[dataset] = make(map[string]int)
		log.Warnf("Data received from a dataset not initialized: %d.", dataset)
		datasetData = calculator.data1[dataset]
	}

	// Storing data
	for _, funbizData := range funbizDataList {
		datasetData[funbizData.BusinessId] = funbizData.Funny
	}

	return len(datasetData)
}

func (calculator *Calculator) AddCityBusiness(dataset int, bulk int, rawData string) {
	calculator.mutex2.Lock()
	datasetDataLength := calculator.saveCityBusiness(rawData)
	calculator.mutex2.Unlock()

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d city businesses stored.", dataset, bulk, datasetDataLength), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveCityBusiness(rawData string) int {
	var citbizDataList []comms.CityBusinessData
	json.Unmarshal([]byte(rawData), &citbizDataList)

	// Storing data
	for _, citbizData := range citbizDataList {
		calculator.data2[citbizData.BusinessId] = citbizData.City
	}

	return len(calculator.data2)
}

func (calculator *Calculator) RetrieveMatches(dataset int) [][]comms.FunnyCityData {
	calculator.mutex2.Lock()
	calculator.mutex1.Lock()

	datasetData1, found1 := calculator.data1[dataset]
	if !found1 {
		log.Warnf("Joining data for a dataset not stored (#%d) in flow #1.", dataset)
		return make([][]comms.FunnyCityData, 0)
	}

	if len(calculator.data2) == 0 {
		log.Warnf("Joining data with an empty city-businesses collection.")
		return make([][]comms.FunnyCityData, 0)
	}

	var bulkedList [][]comms.FunnyCityData
	var bulk []comms.FunnyCityData

	matches := 0
	actualBulk := 0
	for businessId, funny := range datasetData1 {
		if city, found := calculator.data2[businessId]; found {
			matches++
			joinedData := comms.FunnyCityData { City: city, Funny: funny }

			bulk = append(bulk, joinedData)
			actualBulk++

			if actualBulk == calculator.maxBulkSize {
				bulkedList = append(bulkedList, bulk)
				bulk = make([]comms.FunnyCityData, 0)
				actualBulk = 0
			}
		}
	}

	if len(bulk) != 0 {
		bulkedList = append(bulkedList, bulk)
	}

	calculator.mutex2.Unlock()
	calculator.mutex1.Unlock()

	log.Infof("Join matches to send: %d.", matches)
	return bulkedList
}

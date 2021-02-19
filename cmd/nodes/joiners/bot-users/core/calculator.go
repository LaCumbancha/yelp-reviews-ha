package core

import (
	"fmt"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type CalculatorData1 map[int]map[string]int
type CalculatorData2 map[int]map[string]int

type Calculator struct {
	data1 			CalculatorData1
	data2 			CalculatorData2
	mutex1 			*sync.Mutex
	mutex2 			*sync.Mutex
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data1:			make(CalculatorData1),
		data2:			make(CalculatorData2),
		mutex1:			&sync.Mutex{},
		mutex2:			&sync.Mutex{},
	}

	calculator.loadBackup()

	return calculator
}

// This function doesn't need concurrency control because it will be runned just once at the beggining of the execution, when there's just one goroutine.
func (calculator *Calculator) loadBackup() {
	for _, backupData := range bkp.LoadDataBackup() {
		switch backupData.Flow {
		case 1:
			calculator.mutex1.Lock()
			calculator.saveBotUser(backupData.Dataset, backupData.Data)
			calculator.mutex1.Unlock()
		case 2:
			calculator.mutex2.Lock()
			calculator.saveUser(backupData.Dataset, backupData.Data)
			calculator.mutex2.Unlock()
		}
	}

	for dataset1, datasetData1 := range calculator.data1 {
		log.Infof("Dataset #%d retrieved from backup, with %d bot users.", dataset1, len(datasetData1))
	}

	for dataset2, datasetData2 := range calculator.data2 {
		log.Infof("Dataset #%d retrieved from backup, with %d common users.", dataset2, len(datasetData2))
	}
}

func (calculator *Calculator) Clear(dataset int) {
	calculator.mutex1.Lock()
	if _, found := calculator.data1[dataset]; found {
		delete(calculator.data1, dataset)
		log.Infof("Dataset #%d removed from Calculator storage #1.", dataset)
	} else {
		log.Infof("Attempting to remove dataset #%d from Calculator storage #1 but it wasn't registered.", dataset)
	}
	calculator.mutex1.Unlock()

	calculator.mutex2.Lock()
	if _, found := calculator.data2[dataset]; found {
		delete(calculator.data2, dataset)
		log.Infof("Dataset #%d removed from Calculator storage #2.", dataset)
	} else {
		log.Infof("Attempting to remove dataset #%d from Calculator storage #2 but it wasn't registered.", dataset)
	}
	calculator.mutex2.Unlock()
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

	calculator.mutex2.Lock()
	if _, found := calculator.data2[dataset]; !found {
		calculator.data2[dataset] = make(map[string]int)
		log.Infof("Dataset %d initialized in Calculator storage #2.", dataset)
	} else {
		log.Warnf("Dataset %d was already initialized in Calculator storage #2.", dataset)
	}
	calculator.mutex2.Unlock()
}

func (calculator *Calculator) AddBotUser(dataset int, bulk int, rawData string) {
	calculator.mutex1.Lock()
	datasetDataLength := calculator.saveBotUser(dataset, rawData)
	calculator.mutex1.Unlock()

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d bot users stored.", dataset, bulk, datasetDataLength), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveBotUser(dataset int, rawData string) int {
	var botUserDataList []comms.UserData
	json.Unmarshal([]byte(rawData), &botUserDataList)

	// Retrieving dataset
	datasetData, found := calculator.data1[dataset]
	if !found {
		calculator.data1[dataset] = make(map[string]int)
		log.Warnf("Data received from a dataset not initialized: %d.", dataset)
		datasetData = calculator.data1[dataset]
	}

	// Storing data
	for _, botUserData := range botUserDataList {
		datasetData[botUserData.UserId] = 1
	}

	// Updating backup
	bkp.StoreMultiFlowDataBackup(dataset, 1, rawData)

	return len(datasetData)
}

func (calculator *Calculator) AddUser(dataset int, bulk int, rawData string){
	calculator.mutex2.Lock()
	datasetDataLength := calculator.saveUser(dataset, rawData)
	calculator.mutex2.Unlock()

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d common users stored.", dataset, bulk, datasetDataLength), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveUser(dataset int, rawData string) int {
	var userDataList []comms.UserData
	json.Unmarshal([]byte(rawData), &userDataList)

	// Retrieving dataset
	datasetData, found := calculator.data2[dataset]
	if !found {
		calculator.data2[dataset] = make(map[string]int)
		log.Warnf("Data received from a dataset not initialized: %d.", dataset)
		datasetData = calculator.data2[dataset]
	}

	// Storing data
	for _, userData := range userDataList {
		datasetData[userData.UserId] = userData.Reviews
	}

	// Updating backup
	bkp.StoreMultiFlowDataBackup(dataset, 2, rawData)

	return len(datasetData)
}

func (calculator *Calculator) RetrieveMatches(dataset int) []comms.UserData {
	calculator.mutex2.Lock()
	calculator.mutex1.Lock()

	datasetData1, found1 := calculator.data1[dataset]
	if !found1 {
		log.Warnf("Joining data for a dataset not stored (#%d) in flow #1.", dataset)
		return make([]comms.UserData, 0)
	}

	datasetData2, found2 := calculator.data2[dataset]
	if !found2 {
		log.Warnf("Joining data for a dataset not stored (#%d) in flow #2.", dataset)
		return make([]comms.UserData, 0)
	}

	var list []comms.UserData
	for userId, _ := range datasetData1 {
		if reviews, found := datasetData2[userId]; found {
			log.Infof("User %s has posted %d reviews, all with the same text.", userId, reviews)
			joinedData := comms.UserData { UserId: userId, Reviews: reviews }

			list = append(list, joinedData)
		}
	}

	calculator.mutex2.Unlock()
	calculator.mutex1.Unlock()

	return list
}

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

type backupData struct {
	Data1				map[string]int
	Data2 				map[string]int
	Dataset				int
}

type Calculator struct {
	data1 				map[string]int
	data2 				map[string]int
	dataMutex1 			*sync.Mutex
	dataMutex2 			*sync.Mutex
	dataset				int
}

func loadBackup() (map[string]int, map[string]int, int) {
	var backup backupData
	data1 := make(map[string]int)
	data2 := make(map[string]int)
	dataset	:= proc.DefaultDataset

	backupBytes := proc.LoadBackup(proc.DataBkp)
	if backupBytes != nil {
		json.Unmarshal([]byte(backupBytes), &backup)
		data1 = backup.Data1
		data2 = backup.Data2
		dataset = backup.Dataset
		log.Infof("Joiner data restored from backup file. Best users loaded: %d. Common users loaded: %d.", len(data1), len(data2))
	}

	return data1, data2, dataset
}

func NewCalculator() *Calculator {
	data1, data2, dataset := loadBackup()
	
	calculator := &Calculator {
		data1:				data1,
		data2:				data2,
		dataMutex1:			&sync.Mutex{},
		dataMutex2:			&sync.Mutex{},
		dataset:			dataset,
	}

	return calculator
}

func (calculator *Calculator) Clear(newDataset int) {
	calculator.dataMutex1.Lock()
	calculator.data1 = make(map[string]int)
	calculator.dataMutex1.Unlock()

	calculator.dataMutex2.Lock()
	calculator.data2 = make(map[string]int)
	calculator.dataMutex2.Unlock()

	calculator.dataset = newDataset

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) AddBestUser(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		rawData,
		&calculator.dataset,
		calculator.dataMutex1,
		calculator.saveBestUser,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d best users stored.", dataset, bulk, len(calculator.data1)), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveBestUser(rawData string) {
	var bestUserDataList []comms.UserData
	json.Unmarshal([]byte(rawData), &bestUserDataList)

	// Storing data
	for _, bestUserData := range bestUserDataList {
		calculator.data1[bestUserData.UserId] = bestUserData.Reviews
	}

	// Updating backup
	//backup := &backupData { Data1: calculator.data1, Data2: calculator.data2, Dataset: calculator.dataset }
	//proc.StoreBackup(backup, proc.DataBkp)
}

func (calculator *Calculator) AddUser(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		rawData,
		&calculator.dataset,
		calculator.dataMutex2,
		calculator.saveUser,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d common users stored.", dataset, bulk, len(calculator.data2)), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveUser(rawData string) {
	var userDataList []comms.UserData
	json.Unmarshal([]byte(rawData), &userDataList)

	// Storing data
	for _, userData := range userDataList {
		calculator.data2[userData.UserId] = userData.Reviews
	}

	// Updating backup
	//backup := &backupData { Data1: calculator.data1, Data2: calculator.data2, Dataset: calculator.dataset }
	//proc.StoreBackup(backup, proc.DataBkp)
}

func (calculator *Calculator) RetrieveMatches(dataset int) []comms.UserData {
	if dataset != calculator.dataset {
		log.Warnf("Joining data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, dataset)
		return make([]comms.UserData, 0)
	}

	var list []comms.UserData
	calculator.dataMutex1.Lock()
	for userId, bestReviews := range calculator.data1 {
		calculator.dataMutex1.Unlock()

		calculator.dataMutex2.Lock()
		if totalReviews, found := calculator.data2[userId]; found {
			calculator.dataMutex2.Unlock()

			if bestReviews == totalReviews {
				log.Infof("All user %s reviews where rated with 5 stars.", userId)
				joinedData := comms.UserData {
					UserId:		userId,
					Reviews:	totalReviews,
				}
				list = append(list, joinedData)
			}

			calculator.dataMutex1.Lock()
			delete(calculator.data1, userId);
			calculator.dataMutex1.Unlock()

			calculator.dataMutex2.Lock()
			delete(calculator.data2, userId);
			calculator.dataMutex2.Unlock()
			
		} else {
			calculator.dataMutex2.Unlock()
		}

		calculator.dataMutex1.Lock()
	}

	calculator.dataMutex1.Unlock()
	return list
}

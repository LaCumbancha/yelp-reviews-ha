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
	data1 				map[string]int
	data2 				map[string]int
	dataMutex1 			*sync.Mutex
	dataMutex2 			*sync.Mutex
	received1			map[int]bool
	received2			map[int]bool
	receivedMutex1 		*sync.Mutex
	receivedMutex2 		*sync.Mutex
	dataset				int
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data1:				make(map[string]int),
		data2:				make(map[string]int),
		dataMutex1:			&sync.Mutex{},
		dataMutex2:			&sync.Mutex{},
		received1:			make(map[int]bool),
		received2:			make(map[int]bool),
		receivedMutex1:		&sync.Mutex{},
		receivedMutex2:		&sync.Mutex{},
		dataset:			comms.DefaultDataset,
	}

	return calculator
}

func (calculator *Calculator) Clear() {
	calculator.dataMutex1.Lock()
	calculator.data1 = make(map[string]int)
	calculator.dataMutex1.Unlock()

	calculator.receivedMutex1.Lock()
	calculator.received1 = make(map[int]bool)
	calculator.receivedMutex1.Unlock()

	calculator.dataMutex2.Lock()
	calculator.data2 = make(map[string]int)
	calculator.dataMutex2.Unlock()

	calculator.receivedMutex2.Lock()
	calculator.received2 = make(map[int]bool)
	calculator.receivedMutex2.Unlock()

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) AddBestUser(datasetNumber int, bulkNumber int, rawData string) {
	proc.ValidateDataSaving(
		datasetNumber,
		bulkNumber,
		rawData,
		&calculator.dataset,
		calculator.received1,
		calculator.receivedMutex1,
		calculator.Clear,
		calculator.saveBestUser,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d best users stored.", datasetNumber, len(calculator.data1), bulkNumber), bulkNumber)
}

func (calculator *Calculator) saveBestUser(rawBestUserDataBulk string) {
	var bestUserDataList []comms.UserData
	json.Unmarshal([]byte(rawBestUserDataBulk), &bestUserDataList)

	for _, bestUserData := range bestUserDataList {
		calculator.dataMutex1.Lock()
		calculator.data1[bestUserData.UserId] = bestUserData.Reviews
		calculator.dataMutex1.Unlock()
	}
}

func (calculator *Calculator) AddUser(datasetNumber int, bulkNumber int, rawData string) {
	proc.ValidateDataSaving(
		datasetNumber,
		bulkNumber,
		rawData,
		&calculator.dataset,
		calculator.received2,
		calculator.receivedMutex2,
		calculator.Clear,
		calculator.saveUser,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d common users stored.", datasetNumber, len(calculator.data2), bulkNumber), bulkNumber)
}

func (calculator *Calculator) saveUser(rawUserDataBulk string) {
	var userDataList []comms.UserData
	json.Unmarshal([]byte(rawUserDataBulk), &userDataList)

	for _, userData := range userDataList {
		calculator.dataMutex2.Lock()
		calculator.data2[userData.UserId] = userData.Reviews
		calculator.dataMutex2.Unlock()
	}
}

func (calculator *Calculator) RetrieveMatches(datasetNumber int) []comms.UserData {
	if datasetNumber != calculator.dataset {
		log.Warnf("Joining data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, datasetNumber)
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

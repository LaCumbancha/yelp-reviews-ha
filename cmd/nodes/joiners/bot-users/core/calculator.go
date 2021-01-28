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
	received1			map[string]bool
	received2			map[string]bool
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
		received1:			make(map[string]bool),
		received2:			make(map[string]bool),
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
	calculator.received1 = make(map[string]bool)
	calculator.receivedMutex1.Unlock()

	calculator.dataMutex2.Lock()
	calculator.data2 = make(map[string]int)
	calculator.dataMutex2.Unlock()

	calculator.receivedMutex2.Lock()
	calculator.received2 = make(map[string]bool)
	calculator.receivedMutex2.Unlock()

	calculator.dataset++

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) AddBotUser(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		proc.MessageStorageId(inputNode, instance, bulk),
		rawData,
		&calculator.dataset,
		calculator.received1,
		calculator.receivedMutex1,
		calculator.Clear,
		calculator.saveBotUser,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d bot users stored.", dataset, bulk, len(calculator.data1)), bulk)
}

func (calculator *Calculator) saveBotUser(rawData string) {
	var botUserDataList []comms.UserData
	json.Unmarshal([]byte(rawData), &botUserDataList)

	for _, botUserData := range botUserDataList {
		calculator.dataMutex1.Lock()
		calculator.data1[botUserData.UserId] = 1
		calculator.dataMutex1.Unlock()
	}
}

func (calculator *Calculator) AddUser(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		proc.MessageStorageId(inputNode, instance, bulk),
		rawData,
		&calculator.dataset,
		calculator.received1,
		calculator.receivedMutex1,
		calculator.Clear,
		calculator.saveUser,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d common users stored.", dataset, len(calculator.data2), bulk), bulk)
}

func (calculator *Calculator) saveUser(rawData string) {
	var userDataList []comms.UserData
	json.Unmarshal([]byte(rawData), &userDataList)

	for _, userData := range userDataList {
		calculator.dataMutex2.Lock()
		calculator.data2[userData.UserId] = userData.Reviews
		calculator.dataMutex2.Unlock()
	}
}

func (calculator *Calculator) RetrieveMatches(dataset int) []comms.UserData {
	if dataset != calculator.dataset {
		log.Warnf("Joining data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, dataset)
		return make([]comms.UserData, 0)
	}

	var list []comms.UserData
	calculator.dataMutex1.Lock()
	for userId, _ := range calculator.data1 {
		calculator.dataMutex1.Unlock()

		calculator.dataMutex2.Lock()
		if reviews, found := calculator.data2[userId]; found {
			calculator.dataMutex2.Unlock()

			log.Infof("User %s has posted %d reviews, all with the same text.", userId, reviews)
			joinedData := comms.UserData {
				UserId:		userId,
				Reviews:	reviews,
			}
			list = append(list, joinedData)

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

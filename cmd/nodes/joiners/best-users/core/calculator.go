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
	data1 			map[string]int
	data2 			map[string]int
	mutex1 			*sync.Mutex
	mutex2 			*sync.Mutex
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data1:		make(map[string]int),
		data2:		make(map[string]int),
		mutex1:		&sync.Mutex{},
		mutex2:		&sync.Mutex{},
	}

	return calculator
}

func (calculator *Calculator) Clear() {
	calculator.mutex1.Lock()
	calculator.data1 = make(map[string]int)
	calculator.mutex1.Unlock()

	calculator.mutex2.Lock()
	calculator.data2 = make(map[string]int)
	calculator.mutex2.Unlock()

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) AddBestUser(bulkNumber int, rawBestUserDataBulk string) {
	var bestUserDataList []comms.UserData
	json.Unmarshal([]byte(rawBestUserDataBulk), &bestUserDataList)

	for _, bestUserData := range bestUserDataList {
		calculator.mutex1.Lock()
		calculator.data1[bestUserData.UserId] = bestUserData.Reviews
		calculator.mutex1.Unlock()
	}

	logb.Instance().Infof(fmt.Sprintf("Best users data bulk #%d stored in Joiner", bulkNumber), bulkNumber)
}

func (calculator *Calculator) AddUser(bulkNumber int, rawUserDataBulk string) {
	var userDataList []comms.UserData
	json.Unmarshal([]byte(rawUserDataBulk), &userDataList)

	for _, userData := range userDataList {
		calculator.mutex2.Lock()
		calculator.data2[userData.UserId] = userData.Reviews
		calculator.mutex2.Unlock()
	}

	logb.Instance().Infof(fmt.Sprintf("Common users data bulk #%d stored in Joiner", bulkNumber), bulkNumber)
}

func (calculator *Calculator) RetrieveMatches() []comms.UserData {
	var list []comms.UserData

	calculator.mutex1.Lock()
	for userId, bestReviews := range calculator.data1 {
		calculator.mutex1.Unlock()

		calculator.mutex2.Lock()
		if totalReviews, found := calculator.data2[userId]; found {
			calculator.mutex2.Unlock()

			if bestReviews == totalReviews {
				log.Infof("All user %s reviews where rated with 5 stars.", userId)
				joinedData := comms.UserData {
					UserId:		userId,
					Reviews:	totalReviews,
				}
				list = append(list, joinedData)
			}

			calculator.mutex1.Lock()
			delete(calculator.data1, userId);
			calculator.mutex1.Unlock()

			calculator.mutex2.Lock()
			delete(calculator.data2, userId);
			calculator.mutex2.Unlock()
			
		} else {
			calculator.mutex2.Unlock()
		}

		calculator.mutex1.Lock()
	}

	calculator.mutex1.Unlock()
	return list
}

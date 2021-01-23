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

func (calculator *Calculator) AddBotUser(bulkNumber int, rawBotUserDataBulk string) {
	var botUserDataList []comms.UserData
	json.Unmarshal([]byte(rawBotUserDataBulk), &botUserDataList)

	for _, botUserData := range botUserDataList {
		calculator.mutex1.Lock()
		calculator.data1[botUserData.UserId] = 1
		calculator.mutex1.Unlock()
	}

	logb.Instance().Infof(fmt.Sprintf("Bot users data bulk #%d stored in Joiner", bulkNumber), bulkNumber)
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
	for userId, _ := range calculator.data1 {
		calculator.mutex1.Unlock()

		calculator.mutex2.Lock()
		if reviews, found := calculator.data2[userId]; found {
			calculator.mutex2.Unlock()

			log.Infof("User %s has posted %d reviews, all with the same text.", userId, reviews)
			joinedData := comms.UserData {
				UserId:		userId,
				Reviews:	reviews,
			}
			list = append(list, joinedData)

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

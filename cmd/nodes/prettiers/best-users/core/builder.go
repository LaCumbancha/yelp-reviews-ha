package core

import (
	"fmt"
	"sync"
	"sort"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Builder struct {
	data 				map[string]int
	dataMutex 			*sync.Mutex
	received			map[int]bool
	receivedMutex 		*sync.Mutex
	dataset				int
	reviews				int
}

func NewBuilder(minReviews int) *Builder {
	builder := &Builder {
		data:				make(map[string]int),
		dataMutex:			&sync.Mutex{},
		received:			make(map[int]bool),
		receivedMutex:		&sync.Mutex{},
		dataset:			comms.DefaultDataset,
		reviews:			minReviews,
	}

	return builder
}

func (builder *Builder) Clear() {
	builder.dataMutex.Lock()
	builder.data = make(map[string]int)
	builder.dataMutex.Unlock()

	builder.receivedMutex.Lock()
	builder.received = make(map[int]bool)
	builder.receivedMutex.Unlock()

	log.Infof("Builder storage cleared.")
}

func (builder *Builder) Save(datasetNumber int, bulkNumber int, rawData string) {
	proc.ValidateDataSaving(
		datasetNumber,
		bulkNumber,
		rawData,
		&builder.dataset,
		builder.received,
		builder.receivedMutex,
		builder.Clear,
		builder.storeNewUserData,
	)
}

func (builder *Builder) storeNewUserData(rawData string) {
	var userData comms.UserData
	json.Unmarshal([]byte(rawData), &userData)

	builder.dataMutex.Lock()

	if oldReviews, found := builder.data[userData.UserId]; found {
	    log.Warnf("User %s was already stored with %d 5-stars reviews (new value: %d).", userData.UserId, oldReviews, userData.Reviews)
	} else {
		builder.data[userData.UserId] = userData.Reviews
	}

	builder.dataMutex.Unlock()	
}

func (builder *Builder) BuildData(datasetNumber int) string {
	response := fmt.Sprintf("Users with +%d reviews (only 5-stars): ", builder.reviews)

	if datasetNumber != builder.dataset {
		log.Warnf("Building data for a dataset not stored (stored #%d but requested data from #%d).", builder.dataset, datasetNumber)
		return response + "Error generating data."
	}

	var usersList []comms.UserData
	for userId, reviews := range builder.data {
		user := comms.UserData { UserId: userId, Reviews: reviews }
		usersList = append(usersList, user)
    }

    sort.SliceStable(usersList, func(userIdx1, userIdx2 int) bool {
	    return usersList[userIdx1].Reviews > usersList[userIdx2].Reviews
	})

	for _, user := range usersList {
		response += fmt.Sprintf("%s (%d) ; ", user.UserId, user.Reviews)
    }

    if len(usersList) == 0 {
    	return response + "No users accomplish that requirements."
    } else {
    	return response[0:len(response)-3]
    }
}

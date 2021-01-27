package core

import (
	"fmt"
	"sync"
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
	reviews 			int
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

func (builder *Builder) Save(datasetNumber int, bulkNumber int, rawDataBulk string) {
	proc.ValidateDataSaving(
		datasetNumber,
		bulkNumber,
		rawDataBulk,
		&builder.dataset,
		builder.received,
		builder.receivedMutex,
		builder.Clear,
		builder.storeNewUsersData,
	)
}

func (builder *Builder) storeNewUsersData(rawDataBulk string) {
	var userDataList []comms.UserData
	json.Unmarshal([]byte(rawDataBulk), &userDataList)

	for _, userData := range userDataList {
		builder.dataMutex.Lock()

		if oldReviews, found := builder.data[userData.UserId]; found {
		    log.Warnf("User %s was already stored with %d reviews (new value: %d).", userData.UserId, oldReviews, userData.Reviews)
		} else {
			builder.data[userData.UserId] = userData.Reviews
		}
		
		builder.dataMutex.Unlock()
	}
}

func (builder *Builder) BuildData(datasetNumber int) string {
	response := fmt.Sprintf("Users with +%d reviews: ", builder.reviews)

	if datasetNumber != builder.dataset {
		log.Warnf("Building data for a dataset not stored (stored #%d but requested data from #%d).", builder.dataset, datasetNumber)
		return response + "Error generating data."
	}

	totalReviews := 0
	for _, reviews := range builder.data {
		totalReviews += reviews
    }

    totalUsers := len(builder.data)
    if totalUsers == 0 {
    	return response + "No users accomplish that requirements."
    } else {
    	return response + fmt.Sprintf("%d, with %d total reviews (average: %3.2f per user).", totalUsers, totalReviews, float32(totalReviews)/float32(totalUsers))
    }
}

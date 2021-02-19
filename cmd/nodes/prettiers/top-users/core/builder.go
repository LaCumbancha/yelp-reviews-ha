package core

import (
	"fmt"
	"sync"
	"encoding/json"
	
	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Builder struct {
	data 				map[string]int
	dataMutex 			*sync.Mutex
	dataset				int
	reviews 			int
}

func NewBuilder(minReviews int) *Builder {
	builder := &Builder {
		data:				make(map[string]int),
		dataMutex:			&sync.Mutex{},
		dataset:			proc.DefaultDataset,
		reviews:			minReviews,
	}

	for _, backupData := range bkp.LoadSingleFlowDataBackup() {
		builder.dataMutex.Lock()
		builder.storeNewUsersData(backupData)
		builder.dataMutex.Unlock()
	}

	return builder
}

func (builder *Builder) Clear(newDataset int) {
	builder.dataMutex.Lock()
	builder.data = make(map[string]int)
	builder.dataMutex.Unlock()

	builder.dataset = newDataset

	log.Infof("Builder storage cleared.")
}

func (builder *Builder) Save(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		rawData,
		&builder.dataset,
		builder.dataMutex,
		builder.storeNewUsersData,
	)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (builder *Builder) storeNewUsersData(rawData string) {
	var userDataList []comms.UserData
	json.Unmarshal([]byte(rawData), &userDataList)

	// Storing data
	for _, userData := range userDataList {
		if oldReviews, found := builder.data[userData.UserId]; found {
		    log.Warnf("User %s was already stored with %d reviews (new value: %d).", userData.UserId, oldReviews, userData.Reviews)
		} else {
			builder.data[userData.UserId] = userData.Reviews
		}
	}	

	// Updating backup
	bkp.StoreSingleFlowDataBackup(rawData)
}

func (builder *Builder) BuildData(dataset int) string {
	response := fmt.Sprintf("Users with +%d reviews: ", builder.reviews)

	if dataset != builder.dataset {
		log.Warnf("Building data for a dataset not stored (stored #%d but requested data from #%d).", builder.dataset, dataset)
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

package core

import (
	"fmt"
	"sync"
	"encoding/json"
	
	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type BuilderData map[int]map[string]int

type Builder struct {
	data 			BuilderData
	mutex 			*sync.Mutex
	reviews 		int
}

func NewBuilder(minReviews int) *Builder {
	builder := &Builder {
		data:			make(BuilderData),
		mutex:			&sync.Mutex{},
		reviews:		minReviews,
	}

	builder.loadBackup()

	return builder
}

// This function doesn't need concurrency control because it will be runned just once at the beggining of the execution, when there's just one goroutine.
func (builder *Builder) loadBackup() {
	for _, backupData := range bkp.LoadDataBackup() {
		builder.storeNewUsersData(backupData.Dataset, backupData.Data)
	}

	for dataset, datasetData := range builder.data {
		log.Infof("Dataset #%d retrieved from backup, with %d users.", dataset, len(datasetData))
	}
}

func (builder *Builder) Clear(dataset int) {
	builder.mutex.Lock()
	if _, found := builder.data[dataset]; found {
		delete(builder.data, dataset)
		log.Infof("Dataset #%d removed from Builder storage.", dataset)
	} else {
		log.Infof("Attempting to remove dataset #%d from Builder storage but it wasn't registered.", dataset)
	}
	builder.mutex.Unlock()
}

func (builder *Builder) RegisterDataset(dataset int) {
	builder.mutex.Lock()
	if _, found := builder.data[dataset]; !found {
		builder.data[dataset] = make(map[string]int)
		log.Infof("Dataset %d initialized in Builder.", dataset)
	} else {
		log.Warnf("Dataset %d was already initialized in Builder.", dataset)
	}
	builder.mutex.Unlock()
}

func (builder *Builder) Save(dataset int, bulk int, rawData string) {
	builder.mutex.Lock()
	builder.storeNewUsersData(dataset, rawData)
	builder.mutex.Unlock()
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (builder *Builder) storeNewUsersData(dataset int, rawData string) {
	var userDataList []comms.UserData
	json.Unmarshal([]byte(rawData), &userDataList)

	// Retrieving dataset
	datasetData, found := builder.data[dataset]
	if !found {
		builder.data[dataset] = make(map[string]int)
		log.Warnf("Data received from a dataset not initialized: %d.", dataset)
		datasetData = builder.data[dataset]
	}

	// Storing data
	for _, userData := range userDataList {
		if oldReviews, found := datasetData[userData.UserId]; found {
		    log.Warnf("User %s was already stored with %d reviews (new value: %d).", userData.UserId, oldReviews, userData.Reviews)
		} else {
			datasetData[userData.UserId] = userData.Reviews
		}
	}	

	// Updating backup
	bkp.StoreSingleFlowDataBackup(dataset, rawData)
}

func (builder *Builder) BuildData(dataset int) string {
	builder.mutex.Lock()
	response := fmt.Sprintf("Users with +%d reviews: ", builder.reviews)

	datasetData, found := builder.data[dataset]
	if !found {
		log.Warnf("Building data for a dataset not stored (#%d).", dataset)
		return response + "Error generating data."
	}

	totalReviews := 0
	for _, reviews := range datasetData {
		totalReviews += reviews
    }

    totalUsers := len(datasetData)
    
    builder.mutex.Unlock()
    if totalUsers == 0 {
    	return response + "No users accomplish that requirements."
    } else {
    	return response + fmt.Sprintf("%d, with %d total reviews (average: %3.2f per user).", totalUsers, totalReviews, float32(totalReviews)/float32(totalUsers))
    }
}

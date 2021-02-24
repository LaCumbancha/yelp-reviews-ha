package core

import (
	"fmt"
	"sort"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
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

	return builder
}

// This function doesn't need concurrency control because it will be runned just once at the beggining of the execution, when there's just one goroutine.
func (builder *Builder) LoadBackup(dataset int, backups []string) {
	if _, found := builder.data[dataset]; !found {
		builder.data[dataset] = make(map[string]int)
	}
	
	for _, backup := range backups {
		builder.storeNewUserData(dataset, backup)
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
	builder.storeNewUserData(dataset, rawData)
	builder.mutex.Unlock()
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (builder *Builder) storeNewUserData(dataset int, rawData string) {
	var userData comms.UserData
	json.Unmarshal([]byte(rawData), &userData)

	// Retrieving dataset
	datasetData, found := builder.data[dataset]
	if !found {
		builder.data[dataset] = make(map[string]int)
		log.Warnf("Data received from a dataset not initialized: %d.", dataset)
		datasetData = builder.data[dataset]
	}

	// Storing data
	if oldReviews, found := datasetData[userData.UserId]; found {
	    log.Warnf("User %s was already stored with %d repeated reviews (new value: %d).", userData.UserId, oldReviews, userData.Reviews)
	} else {
		datasetData[userData.UserId] = userData.Reviews
	}
}

func (builder *Builder) BuildData(dataset int) string {
	builder.mutex.Lock()
	response := fmt.Sprintf("Users with +%d reviews (all with same text): ", builder.reviews)

	datasetData, found := builder.data[dataset]
	if !found {
		log.Warnf("Building data for a dataset not stored (#%d).", dataset)
		return response + "Error generating data."
	}

	var usersList []comms.UserData
	for userId, reviews := range datasetData {
		user := comms.UserData { UserId: userId, Reviews: reviews }
		usersList = append(usersList, user)
    }

    sort.SliceStable(usersList, func(userIdx1, userIdx2 int) bool {
	    return usersList[userIdx1].Reviews > usersList[userIdx2].Reviews
	})

	for _, user := range usersList {
		response += fmt.Sprintf("%s (%d) ; ", user.UserId, user.Reviews)
    }

    builder.mutex.Unlock()
    if len(usersList) == 0 {
    	return response + "No users accomplish that requirements."
    } else {
    	return response[0:len(response)-3]
    }
}

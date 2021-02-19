package core

import (
	"fmt"
	"sync"
	"sort"
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
	reviews				int
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
		builder.storeNewUserData(backupData)
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
		builder.storeNewUserData,
	)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (builder *Builder) storeNewUserData(rawData string) {
	var userData comms.UserData
	json.Unmarshal([]byte(rawData), &userData)

	// Storing data
	if oldReviews, found := builder.data[userData.UserId]; found {
	    log.Warnf("User %s was already stored with %d 5-stars reviews (new value: %d).", userData.UserId, oldReviews, userData.Reviews)
	} else {
		builder.data[userData.UserId] = userData.Reviews
	}

	// Updating backup
	bkp.StoreSingleFlowDataBackup(rawData)
}

func (builder *Builder) BuildData(dataset int) string {
	response := fmt.Sprintf("Users with +%d reviews (only 5-stars): ", builder.reviews)

	if dataset != builder.dataset {
		log.Warnf("Building data for a dataset not stored (stored #%d but requested data from #%d).", builder.dataset, dataset)
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

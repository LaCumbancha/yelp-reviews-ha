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

type backupData struct {
	Data				map[string]int
	Dataset				int
}

type Builder struct {
	data 				map[string]int
	dataMutex 			*sync.Mutex
	dataset				int
	reviews				int
}

func loadBackup() (map[string]int, int) {
	var backup backupData
	data := make(map[string]int)
	dataset	:= proc.DefaultDataset

	backupBytes := proc.LoadBackup(proc.DataBkp)
	if backupBytes != nil {
		json.Unmarshal([]byte(backupBytes), &backup)
		data = backup.Data
		dataset = backup.Dataset
		log.Infof("Prettier data restored from backup file. Users with 5-stars reviews loaded: %d .", len(data))
	}

	return data, dataset
}

func NewBuilder(minReviews int) *Builder {
	data, dataset := loadBackup()
	
	builder := &Builder {
		data:				data,
		dataMutex:			&sync.Mutex{},
		dataset:			dataset,
		reviews:			minReviews,
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
	//backup := &backupData { Data: builder.data, Dataset: builder.dataset }
	//proc.StoreBackup(backup, proc.DataBkp)
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

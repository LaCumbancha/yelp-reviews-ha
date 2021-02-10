package core

import (
	"fmt"
	"sync"
	"encoding/json"
	
	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type backupData struct {
	data				map[string]int
	received			map[string]bool
}

type Builder struct {
	data 				map[string]int
	dataMutex 			*sync.Mutex
	received			map[string]bool
	receivedMutex 		*sync.Mutex
	dataset				int
	reviews 			int
}

func loadBackup() (map[string]int, map[string]bool) {
	var backup backupData
	data := make(map[string]int)
	received := make(map[string]bool)

	backupBytes := proc.LoadBackup(proc.DataBkp)
	if backupBytes != nil {
		json.Unmarshal([]byte(backupBytes), &backup)
		data = backup.data
		received = backup.received
		log.Infof("Prettier data restored from backup file. Top users loaded: %d (%d messages).", len(data), len(received))
	}

	return data, received
}

func NewBuilder(minReviews int) *Builder {
	data, received := loadBackup()
	
	builder := &Builder {
		data:				data,
		dataMutex:			&sync.Mutex{},
		received:			received,
		receivedMutex:		&sync.Mutex{},
		dataset:			proc.DefaultDataset,
		reviews:			minReviews,
	}

	return builder
}

func (builder *Builder) Clear(newDataset int) {
	builder.dataMutex.Lock()
	builder.data = make(map[string]int)
	builder.dataMutex.Unlock()

	builder.receivedMutex.Lock()
	builder.received = make(map[string]bool)
	builder.receivedMutex.Unlock()

	builder.dataset = newDataset

	log.Infof("Builder storage cleared.")
}

func (builder *Builder) Save(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		proc.MessageSavingId(inputNode, instance, bulk),
		rawData,
		&builder.dataset,
		builder.dataMutex,
		builder.received,
		builder.receivedMutex,
		builder.Clear,
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
	backup := &backupData { data: builder.data, received: builder.received }
	backupBytes, err := json.Marshal(backup)

	if err != nil {
		log.Errorf("Error serializing Prettier backup. Err: %s", err)
	} else {
		proc.StoreBackup(proc.DataBkp, backupBytes)
	}
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

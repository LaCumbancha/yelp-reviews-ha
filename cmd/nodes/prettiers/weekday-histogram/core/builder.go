package core

import (
	"fmt"
	"sync"
	"encoding/json"
	
	log "github.com/sirupsen/logrus"
	comms "github.com/LaCumbancha/yelp-review-ha/cmd/common/communication"
)

type BuilderData map[int]map[string]int

type Builder struct {
	data 			BuilderData
	mutex 			*sync.Mutex
}

func NewBuilder() *Builder {
	builder := &Builder {
		data:			make(BuilderData),
		mutex:			&sync.Mutex{},
	}

	return builder
}

// This function doesn't need concurrency control because it will be runned just once at the beggining of the execution, when there's just one goroutine.
func (builder *Builder) LoadBackup(dataset int, backups []string) {
	if _, found := builder.data[dataset]; !found {
		builder.data[dataset] = make(map[string]int)
	}
	
	for _, backup := range backups {
		builder.storeNewWeekdayData(dataset, backup)
	}
	
	for dataset, _ := range builder.data {
		log.Infof("Dataset #%d retrieved from backup.", dataset)
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
	builder.storeNewWeekdayData(dataset, rawData)
	builder.mutex.Unlock()
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (builder *Builder) storeNewWeekdayData(dataset int, rawData string) {
	var weekdayData comms.WeekdayData
	json.Unmarshal([]byte(rawData), &weekdayData)

	// Retrieving dataset
	datasetData, found := builder.data[dataset]
	if !found {
		builder.data[dataset] = make(map[string]int)
		log.Warnf("Data received from a dataset not initialized: %d.", dataset)
		datasetData = builder.data[dataset]
	}

	// Storing data
	datasetData[weekdayData.Weekday] = weekdayData.Reviews
	log.Infof("Saved %s reviews at %d.", weekdayData.Weekday, weekdayData.Reviews)
}

func (builder *Builder) BuildData(dataset int) string {
	builder.mutex.Lock()
	response := "Reviews by Weekday: "

	datasetData, found := builder.data[dataset]
	if !found {
		log.Warnf("Building data for a dataset not stored (#%d).", dataset)
		return response + "Error generating data."
	}

	builder.mutex.Unlock()
	return response + fmt.Sprintf(
		"SUN (%d), MON (%d), TUE (%d), WED (%d), THU (%d), FRI (%d), SAT (%d)",
		datasetData["Sunday"],
		datasetData["Monday"],
		datasetData["Tuesday"],
		datasetData["Wednesday"],
		datasetData["Thursday"],
		datasetData["Friday"],
		datasetData["Saturday"],
	)
}

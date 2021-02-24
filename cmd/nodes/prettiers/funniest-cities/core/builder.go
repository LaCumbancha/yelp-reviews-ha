package core

import (
	"fmt"
	"sort"
	"sync"
	"encoding/json"
	
	log "github.com/sirupsen/logrus"
	comms "github.com/LaCumbancha/yelp-review-ha/cmd/common/communication"
)

type BuilderData map[int][]comms.FunnyCityData

type Builder struct {
	data 			BuilderData
	mutex 			*sync.Mutex
	topSize			int
}

func NewBuilder(topSize int) *Builder {
	builder := &Builder {
		data:			make(BuilderData),
		mutex:			&sync.Mutex{},
		topSize:		topSize,
	}

	return builder
}

// This function doesn't need concurrency control because it will be runned just once at the beggining of the execution, when there's just one goroutine.
func (builder *Builder) LoadBackup(dataset int, backups []string) {
	if _, found := builder.data[dataset]; !found {
		builder.data[dataset] = make([]comms.FunnyCityData, 0)
	}
	
	for _, backup := range backups {
		builder.storeNewCityData(dataset, backup)
	}
	
	for dataset, datasetData := range builder.data {
		log.Infof("Dataset #%d retrieved from backup, with %d cities.", dataset, len(datasetData))
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
		builder.data[dataset] = make([]comms.FunnyCityData, 0)
		log.Infof("Dataset %d initialized in Builder.", dataset)
	} else {
		log.Warnf("Dataset %d was already initialized in Builder.", dataset)
	}

	builder.mutex.Unlock()
}

func (builder *Builder) Save(dataset int, bulk int, rawData string) {
	builder.mutex.Lock()
	builder.storeNewCityData(dataset, rawData)
	builder.mutex.Unlock()
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (builder *Builder) storeNewCityData(dataset int, rawData string) {
	var funnyCity comms.FunnyCityData
	json.Unmarshal([]byte(rawData), &funnyCity)

	// Retrieving dataset
	if _, found := builder.data[dataset]; !found {
		builder.data[dataset] = make([]comms.FunnyCityData, 0)
		log.Warnf("Data received from a dataset not initialized: %d.", dataset)
	}

	// Storing data
	builder.data[dataset] = append(builder.data[dataset], funnyCity)
	log.Infof("City %s stored with funniness at %d.", funnyCity.City, funnyCity.Funny)
}

func (builder *Builder) BuildData(dataset int) string {
	builder.mutex.Lock()
	response := "Top Funniest Cities: "

	datasetData, found := builder.data[dataset]
	if !found {
		log.Warnf("Building data for a dataset not stored (#%d).", dataset)
		return response + "Error generating data."
	}

	sort.SliceStable(datasetData, func(cityIdx1, cityIdx2 int) bool {
	    return datasetData[cityIdx1].Funny > datasetData[cityIdx2].Funny
	})

	var topTenCities []comms.FunnyCityData
	funnyCities := len(datasetData)
	if (funnyCities > builder.topSize) {
		log.Infof("%d cities where discarded due to not being funny enoguh.", funnyCities - builder.topSize)
		topTenCities = datasetData[0:builder.topSize]
	} else {
		log.Infof("They where just %d cities with a funniness higher than 0!", funnyCities)
		topTenCities = datasetData[0:funnyCities]
	}

	for _, funnyCity := range topTenCities {
		response += fmt.Sprintf("%s w/ %dp ; ", funnyCity.City, funnyCity.Funny)
    }

    builder.mutex.Unlock()
    if len(topTenCities) == 0 {
    	return "No cities have funny points."
    } else {
    	return response[0:len(response)-3]
    }
}

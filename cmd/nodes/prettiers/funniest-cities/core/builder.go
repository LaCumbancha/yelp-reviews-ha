package core

import (
	"fmt"
	"sort"
	"sync"
	"encoding/json"
	
	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type backupData struct {
	Data				[]comms.FunnyCityData
	Received			map[string]bool
	Dataset				int
}

type Builder struct {
	data 				[]comms.FunnyCityData
	dataMutex 			*sync.Mutex
	received			map[string]bool
	receivedMutex 		*sync.Mutex
	dataset				int
	topSize				int
}

func loadBackup() ([]comms.FunnyCityData, map[string]bool, int) {
	var backup backupData
	data := make([]comms.FunnyCityData, 0)
	received := make(map[string]bool)
	dataset	:= proc.DefaultDataset

	backupBytes := proc.LoadBackup(proc.DataBkp)
	if backupBytes != nil {
		json.Unmarshal([]byte(backupBytes), &backup)
		data = backup.Data
		received = backup.Received
		dataset = backup.Dataset
		log.Infof("Prettier data restored from backup file. Funny cities loaded: %d (%d messages).", len(data), len(received))
	}

	return data, received, dataset
}

func NewBuilder(topSize int) *Builder {
	data, received, dataset := loadBackup()
	
	builder := &Builder {
		data:				data,
		dataMutex:			&sync.Mutex{},
		received:			received,
		receivedMutex:		&sync.Mutex{},
		dataset:			dataset,
		topSize:			topSize,
	}

	return builder
}

func (builder *Builder) Clear(newDataset int) {
	builder.dataMutex.Lock()
	builder.data = []comms.FunnyCityData{}
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
		builder.storeNewCityData,
	)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (builder *Builder) storeNewCityData(rawData string) {
	var funnyCity comms.FunnyCityData
	json.Unmarshal([]byte(rawData), &funnyCity)

	// Storing data
	builder.data = append(builder.data, funnyCity)
	log.Infof("City %s stored with funniness at %d.", funnyCity.City, funnyCity.Funny)	

	// Updating backup
	backup := &backupData { Data: builder.data, Received: builder.received, Dataset: builder.dataset }
	backupBytes, err := json.Marshal(backup)

	if err != nil {
		log.Errorf("Error serializing Prettier backup. Err: %s", err)
	} else {
		proc.StoreBackup(proc.DataBkp, backupBytes)
	}
}

func (builder *Builder) BuildData(dataset int) string {
	response := "Top Funniest Cities: "

	if dataset != builder.dataset {
		log.Warnf("Building data for a dataset not stored (stored #%d but requested data from #%d).", builder.dataset, dataset)
		return response + "Error generating data."
	}

	sort.SliceStable(builder.data, func(cityIdx1, cityIdx2 int) bool {
	    return builder.data[cityIdx1].Funny > builder.data[cityIdx2].Funny
	})

	var topTenCities []comms.FunnyCityData
	funnyCities := len(builder.data)
	if (funnyCities > builder.topSize) {
		log.Infof("%d cities where discarded due to not being funny enoguh.", funnyCities - builder.topSize)
		topTenCities = builder.data[0:builder.topSize]
	} else {
		log.Infof("They where just %d cities with a funniness higher than 0!", funnyCities)
		topTenCities = builder.data[0:funnyCities]
	}

	for _, funnyCity := range topTenCities {
		response += fmt.Sprintf("%s w/ %dp ; ", funnyCity.City, funnyCity.Funny)
    }

    if len(topTenCities) == 0 {
    	return "No cities have funny points."
    } else {
    	return response[0:len(response)-3]
    }
}

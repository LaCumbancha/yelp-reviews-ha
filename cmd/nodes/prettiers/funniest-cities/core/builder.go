package core

import (
	"fmt"
	"sort"
	"sync"
	"encoding/json"
	
	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Builder struct {
	data 				[]comms.FunnyCityData
	dataMutex 			*sync.Mutex
	dataset				int
	topSize				int
}

func NewBuilder(topSize int) *Builder {
	builder := &Builder {
		data:				make([]comms.FunnyCityData, 0),
		dataMutex:			&sync.Mutex{},
		dataset:			proc.DefaultDataset,
		topSize:			topSize,
	}

	for _, backupData := range bkp.LoadSingleFlowDataBackup() {
		builder.dataMutex.Lock()
		builder.storeNewCityData(backupData)
		builder.dataMutex.Unlock()
	}

	return builder
}

func (builder *Builder) Clear(newDataset int) {
	builder.dataMutex.Lock()
	builder.data = []comms.FunnyCityData{}
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
	bkp.StoreSingleFlowDataBackup(rawData)
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

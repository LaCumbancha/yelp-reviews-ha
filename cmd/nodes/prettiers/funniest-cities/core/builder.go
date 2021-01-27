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

type Builder struct {
	data 				[]comms.FunnyCityData
	dataMutex 			*sync.Mutex
	received			map[int]bool
	receivedMutex 		*sync.Mutex
	dataset				int
	topSize				int
}

func NewBuilder(topSize int) *Builder {
	builder := &Builder {
		data:				[]comms.FunnyCityData{},
		dataMutex:			&sync.Mutex{},
		received:			make(map[int]bool),
		receivedMutex:		&sync.Mutex{},
		dataset:			comms.DefaultDataset,
		topSize:			topSize,
	}

	return builder
}

func (builder *Builder) Clear() {
	builder.dataMutex.Lock()
	builder.data = []comms.FunnyCityData{}
	builder.dataMutex.Unlock()

	builder.receivedMutex.Lock()
	builder.received = make(map[int]bool)
	builder.receivedMutex.Unlock()

	log.Infof("Builder storage cleared.")
}

func (builder *Builder) Save(datasetNumber int, bulkNumber int, rawData string) {
	proc.ValidateDataSaving(
		datasetNumber,
		bulkNumber,
		rawData,
		&builder.dataset,
		builder.received,
		builder.receivedMutex,
		builder.Clear,
		builder.storeNewCityData,
	)
}

func (builder *Builder) storeNewCityData(rawData string) {
	var funnyCity comms.FunnyCityData
	json.Unmarshal([]byte(rawData), &funnyCity)

	builder.dataMutex.Lock()
	builder.data = append(builder.data, funnyCity)
	builder.dataMutex.Unlock()

	log.Infof("City %s stored with funniness at %d.", funnyCity.City, funnyCity.Funny)
}

func (builder *Builder) BuildData(datasetNumber int) string {
	response := "Top Funniest Cities: "

	if datasetNumber != builder.dataset {
		log.Warnf("Building data for a dataset not stored (stored #%d but requested data from #%d).", builder.dataset, datasetNumber)
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

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
	received			map[string]bool
	receivedMutex 		*sync.Mutex
	dataset				int
	topSize				int
}

func NewBuilder(topSize int) *Builder {
	builder := &Builder {
		data:				[]comms.FunnyCityData{},
		dataMutex:			&sync.Mutex{},
		received:			make(map[string]bool),
		receivedMutex:		&sync.Mutex{},
		dataset:			proc.DefaultDataset,
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

package core

import (
	"fmt"
	"sort"
	"sync"
	"encoding/json"
	
	log "github.com/sirupsen/logrus"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Builder struct {
	data 			[]comms.FunnyCityData
	mutex 			*sync.Mutex
	topSize			int
}

func NewBuilder(topSize int) *Builder {
	builder := &Builder {
		data:		[]comms.FunnyCityData{},
		mutex:		&sync.Mutex{},
		topSize:	topSize,
	}

	return builder
}

func (builder *Builder) Clear() {
	builder.mutex.Lock()
	builder.data = []comms.FunnyCityData{}
	builder.mutex.Unlock()

	log.Infof("Builder storage cleared.")
}

func (builder *Builder) Save(rawData string) {
	var funnyCity comms.FunnyCityData
	json.Unmarshal([]byte(rawData), &funnyCity)

	builder.mutex.Lock()
	builder.data = append(builder.data, funnyCity)
	builder.mutex.Unlock()

	log.Infof("City %s stored with funniness at %d.", funnyCity.City, funnyCity.Funny)
}

func (builder *Builder) BuildTopTen() string {
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

	response := "Top Funniest Cities: "
	for _, funnyCity := range topTenCities {
		response += fmt.Sprintf("%s w/ %dp ; ", funnyCity.City, funnyCity.Funny)
    }

    if len(topTenCities) == 0 {
    	return "no cities have funny points."
    } else {
    	return response[0:len(response)-3]
    }
}

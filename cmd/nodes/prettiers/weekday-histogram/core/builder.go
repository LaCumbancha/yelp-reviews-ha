package core

import (
	"fmt"
	"sync"
	"encoding/json"
	
	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Builder struct {
	data 				map[string]int
	dataMutex 			*sync.Mutex
	received			map[int]bool
	receivedMutex 		*sync.Mutex
	dataset				int
}

func NewBuilder() *Builder {
	builder := &Builder {
		data:				make(map[string]int),
		dataMutex:			&sync.Mutex{},
		received:			make(map[int]bool),
		receivedMutex:		&sync.Mutex{},
		dataset:			comms.DefaultDataset,
	}

	return builder
}

func (builder *Builder) Clear() {
	builder.dataMutex.Lock()
	builder.data = make(map[string]int)
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
		builder.storeNewWeekdayData,
	)
}

func (builder *Builder) storeNewWeekdayData(rawData string) {
	var weekdayData comms.WeekdayData
	json.Unmarshal([]byte(rawData), &weekdayData)

	builder.dataMutex.Lock()

	builder.data[weekdayData.Weekday] = weekdayData.Reviews
	log.Infof("Saved %s reviews at %d.", weekdayData.Weekday, weekdayData.Reviews)

	builder.dataMutex.Unlock()
}

func (builder *Builder) BuildData(datasetNumber int) string {
	response := "Reviews by Weekday: "

	if datasetNumber != builder.dataset {
		log.Warnf("Building data for a dataset not stored (stored #%d but requested data from #%d).", builder.dataset, datasetNumber)
		return response + "Error generating data."
	}

	return response + fmt.Sprintf(
		"SUN (%d), MON (%d), TUE (%d), WED (%d), THU (%d), FRI (%d), SAT (%d)",
		builder.data["Sunday"],
		builder.data["Monday"],
		builder.data["Tuesday"],
		builder.data["Wednesday"],
		builder.data["Thursday"],
		builder.data["Friday"],
		builder.data["Saturday"],
	)
}

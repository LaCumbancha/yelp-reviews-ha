package core

import (
	"fmt"
	"sync"
	"encoding/json"
	
	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Builder struct {
	data 				map[string]int
	dataMutex 			*sync.Mutex
	dataset				int
}

func NewBuilder() *Builder {
	builder := &Builder {
		data:				make(map[string]int),
		dataMutex:			&sync.Mutex{},
		dataset:			proc.DefaultDataset,
	}

	for _, backupData := range bkp.LoadSingleFlowDataBackup() {
		builder.dataMutex.Lock()
		builder.storeNewWeekdayData(backupData)
		builder.dataMutex.Unlock()
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
		builder.storeNewWeekdayData,
	)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (builder *Builder) storeNewWeekdayData(rawData string) {
	var weekdayData comms.WeekdayData
	json.Unmarshal([]byte(rawData), &weekdayData)

	// Storing data
	builder.data[weekdayData.Weekday] = weekdayData.Reviews
	log.Infof("Saved %s reviews at %d.", weekdayData.Weekday, weekdayData.Reviews)

	// Updating backup
	bkp.StoreSingleFlowDataBackup(rawData)
}

func (builder *Builder) BuildData(dataset int) string {
	response := "Reviews by Weekday: "

	if dataset != builder.dataset {
		log.Warnf("Building data for a dataset not stored (stored #%d but requested data from #%d).", builder.dataset, dataset)
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

package core

import (
	"fmt"
	"sync"
	"encoding/json"
	
	log "github.com/sirupsen/logrus"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Builder struct {
	data 			map[string]int
	mutex 			*sync.Mutex
}

func NewBuilder() *Builder {
	builder := &Builder {
		data:		make(map[string]int),
		mutex:		&sync.Mutex{},
	}

	return builder
}

func (builder *Builder) Clear() {
	builder.mutex.Lock()
	builder.data = make(map[string]int)
	builder.mutex.Unlock()

	log.Infof("Builder storage cleared.")
}

func (builder *Builder) Save(rawData string) {
	var weekdayData comms.WeekdayData
	json.Unmarshal([]byte(rawData), &weekdayData)

	builder.mutex.Lock()

	builder.data[weekdayData.Weekday] = weekdayData.Reviews
	log.Infof("Saved %s reviews at %d.", weekdayData.Weekday, weekdayData.Reviews)

	builder.mutex.Unlock()
}

func (builder *Builder) BuildData() string {
	return fmt.Sprintf(
		"Reviews by Weekday: SUN (%d), MON (%d), TUE (%d), WED (%d), THU (%d), FRI (%d), SAT (%d)",
		builder.data["Sunday"],
		builder.data["Monday"],
		builder.data["Tuesday"],
		builder.data["Wednesday"],
		builder.data["Thursday"],
		builder.data["Friday"],
		builder.data["Saturday"],
	)
}

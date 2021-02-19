package core

import (
	"fmt"
	"sort"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Calculator struct {
	data 				map[string]int
	dataMutex 			*sync.Mutex
	dataset				int
	topSize				int
}

func NewCalculator(topSize int) *Calculator {
	calculator := &Calculator {
		data:				make(map[string]int),
		dataMutex:			&sync.Mutex{},
		dataset:			proc.DefaultDataset,
		topSize:			topSize,
	}

	for _, backupData := range bkp.LoadSingleFlowDataBackup() {
		calculator.dataMutex.Lock()
		calculator.saveData(backupData)
		calculator.dataMutex.Unlock()
	}

	return calculator
}

func (calculator *Calculator) Clear(newDataset int) {
	calculator.dataMutex.Lock()
	calculator.data = make(map[string]int)
	calculator.dataMutex.Unlock()

	calculator.dataset = newDataset

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) Save(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		rawData,
		&calculator.dataset,
		calculator.dataMutex,
		calculator.saveData,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Aggregator: %d cities stored.", dataset, bulk, len(calculator.data)), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveData(rawData string) {
	var funcitDataList []comms.FunnyCityData
	json.Unmarshal([]byte(rawData), &funcitDataList)

	// Storing data
	for _, funcitData := range funcitDataList {
		if value, found := calculator.data[funcitData.City]; found {
			newAmount := value + funcitData.Funny
		    calculator.data[funcitData.City] = newAmount
		} else {
			calculator.data[funcitData.City] = funcitData.Funny
		}
	}

	// Updating backup
	bkp.StoreSingleFlowDataBackup(rawData)
}

func (calculator *Calculator) AggregateData(dataset int) []comms.FunnyCityData {
	if dataset != calculator.dataset {
		log.Warnf("Aggregating data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, dataset)
		return make([]comms.FunnyCityData, 0)
	}

	funnyCities := make([]comms.FunnyCityData, 0)
	for city, funny := range calculator.data {
		aggregatedData := comms.FunnyCityData { City: city, Funny: funny }
		funnyCities = append(funnyCities, aggregatedData)
	}

	sort.SliceStable(funnyCities, func(cityIdx1, cityIdx2 int) bool {
	    return funnyCities[cityIdx1].Funny > funnyCities[cityIdx2].Funny
	})

	totalFunnyCities := len(funnyCities)
	if (totalFunnyCities > calculator.topSize) {
		log.Infof("%d cities where discarded due to not being funny enoguh.", totalFunnyCities - calculator.topSize)
		return funnyCities[0:calculator.topSize]
	} else {
		log.Infof("They where just %d cities with a funniness higher than 0!", totalFunnyCities)
		return funnyCities[0:totalFunnyCities]
	}
}

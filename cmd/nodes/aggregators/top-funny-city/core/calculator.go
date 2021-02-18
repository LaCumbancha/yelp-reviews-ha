package core

import (
	"fmt"
	"sort"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type backupData struct {
	Data				[]comms.FunnyCityData
	Dataset				int
}

type Calculator struct {
	data 				[]comms.FunnyCityData
	dataMutex 			*sync.Mutex
	dataset				int
	topSize				int
}

func loadBackup() ([]comms.FunnyCityData, int) {
	var backup backupData
	data := make([]comms.FunnyCityData, 0)
	dataset	:= proc.DefaultDataset

	backupBytes := proc.LoadBackup(proc.DataBkp)
	if backupBytes != nil {
		json.Unmarshal([]byte(backupBytes), &backup)
		data = backup.Data
		dataset = backup.Dataset
		log.Infof("Aggregator data restored from backup file. Funny cities loaded: %d .", len(data))
	}

	return data, dataset
}

func NewCalculator(topSize int) *Calculator {
	data, dataset := loadBackup()
	
	calculator := &Calculator {
		data:				data,
		dataMutex:			&sync.Mutex{},
		dataset:			dataset,
		topSize:			topSize,
	}

	return calculator
}

func (calculator *Calculator) Clear(newDataset int) {
	calculator.dataMutex.Lock()
	calculator.data = []comms.FunnyCityData{}
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

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Aggregator: %d funny cities stored.", dataset, bulk, len(calculator.data)), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveData(rawData string) {
	var funcitDataList []comms.FunnyCityData
	json.Unmarshal([]byte(rawData), &funcitDataList)

	// Storing data
	for _, funcitData := range funcitDataList {
		calculator.data = append(calculator.data, funcitData)
	}

	// Updating backup
	backup := &backupData { Data: calculator.data, Dataset: calculator.dataset }
	backupBytes, err := json.Marshal(backup)

	if err != nil {
		log.Errorf("Error serializing Aggregator backup. Err: %s", err)
	} else {
		proc.StoreBackup(proc.DataBkp, backupBytes)
	}
}

func (calculator *Calculator) AggregateData(dataset int) []comms.FunnyCityData {
	if dataset != calculator.dataset {
		log.Warnf("Aggregating data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, dataset)
		return make([]comms.FunnyCityData, 0)
	}
	
	sort.SliceStable(calculator.data, func(cityIdx1, cityIdx2 int) bool {
	    return calculator.data[cityIdx1].Funny > calculator.data[cityIdx2].Funny
	})

	funnyCities := len(calculator.data)
	if (funnyCities > calculator.topSize) {
		log.Infof("%d cities where discarded due to not being funny enoguh.", funnyCities - calculator.topSize)
		return calculator.data[0:calculator.topSize]
	} else {
		log.Infof("They where just %d cities with a funniness higher than 0!", funnyCities)
		return calculator.data[0:funnyCities]
	}
}

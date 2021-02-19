package core

import (
	"fmt"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Calculator struct {
	data1 				map[string]int
	data2 				map[string]string
	dataMutex1 			*sync.Mutex
	dataMutex2 			*sync.Mutex
	dataset				int
	maxBulkSize			int
}

func NewCalculator(bulkSize int) *Calculator {
	calculator := &Calculator {
		data1:				make(map[string]int),
		data2:				make(map[string]string),
		dataMutex1:			&sync.Mutex{},
		dataMutex2:			&sync.Mutex{},
		dataset:			proc.DefaultDataset,
		maxBulkSize:		bulkSize,
	}

	for _, backupData := range bkp.LoadMultiFlowDataBackup() {
		switch backupData.Flow {
		case 1:
			calculator.dataMutex1.Lock()
			calculator.saveFunnyBusiness(backupData.Data)
			calculator.dataMutex1.Unlock()
		case 2:
			calculator.dataMutex2.Lock()
			calculator.saveCityBusiness(backupData.Data)
			calculator.dataMutex2.Unlock()
		}
	}

	return calculator
}

// Just the funny-businesses information is cleared because city-businesses data is needed for futures datasets.
func (calculator *Calculator) Clear(newDataset int) {
	calculator.dataMutex1.Lock()
	calculator.data1 = make(map[string]int)
	calculator.dataMutex1.Unlock()

	calculator.dataset = newDataset

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) AddFunnyBusiness(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		rawData,
		&calculator.dataset,
		calculator.dataMutex1,
		calculator.saveFunnyBusiness,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d funny businesses stored.", dataset, bulk, len(calculator.data1)), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveFunnyBusiness(rawData string) {
	var funbizDataList []comms.FunnyBusinessData
	json.Unmarshal([]byte(rawData), &funbizDataList)

	// Storing data
	for _, funbizData := range funbizDataList {
		calculator.data1[funbizData.BusinessId] = funbizData.Funny
	}

	// Updating backup
	bkp.StoreMultiFlowDataBackup(1, rawData)
}

func (calculator *Calculator) AddCityBusiness(inputNode string, dataset int, instance string, bulk int, rawData string) {
	proc.ValidateDataSaving(
		dataset,
		rawData,
		&calculator.dataset,
		calculator.dataMutex2,
		calculator.saveCityBusiness,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d city businesses stored.", dataset, bulk, len(calculator.data2)), bulk)
}

// This function is guaranteed to be call in a mutual exclusion scenario.
func (calculator *Calculator) saveCityBusiness(rawData string) {
	var citbizDataList []comms.CityBusinessData
	json.Unmarshal([]byte(rawData), &citbizDataList)

	// Storing data
	for _, citbizData := range citbizDataList {
		calculator.data2[citbizData.BusinessId] = citbizData.City
	}

	// Updating backup
	bkp.StoreMultiFlowDataBackup(2, rawData)
}

func (calculator *Calculator) RetrieveMatches(dataset int) [][]comms.FunnyCityData {
	if dataset != calculator.dataset {
		log.Warnf("Joining data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, dataset)
		return make([][]comms.FunnyCityData, 0)
	}

	var bulkedList [][]comms.FunnyCityData
	var bulk []comms.FunnyCityData

	actualBulk := 0
	calculator.dataMutex1.Lock()
	for businessId, funny := range calculator.data1 {
		calculator.dataMutex1.Unlock()

		calculator.dataMutex2.Lock()
		if city, found := calculator.data2[businessId]; found {
			calculator.dataMutex2.Unlock()
			joinedData := comms.FunnyCityData {
				City:		city,
				Funny:		funny,
			}

			calculator.dataMutex1.Lock()
			delete(calculator.data1, businessId);
			calculator.dataMutex1.Unlock()

			bulk = append(bulk, joinedData)
			actualBulk++

			if actualBulk == calculator.maxBulkSize {
				bulkedList = append(bulkedList, bulk)
				bulk = make([]comms.FunnyCityData, 0)
				actualBulk = 0
			}

		} else {
			calculator.dataMutex2.Unlock()
		}

		calculator.dataMutex1.Lock()
	}

	calculator.dataMutex1.Unlock()

	if len(bulk) != 0 {
		bulkedList = append(bulkedList, bulk)
	}

	return bulkedList
}

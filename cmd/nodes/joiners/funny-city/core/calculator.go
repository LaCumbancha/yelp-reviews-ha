package core

import (
	"fmt"
	"sync"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type Calculator struct {
	data1 				map[string]int
	data2 				map[string]string
	dataMutex1 			*sync.Mutex
	dataMutex2 			*sync.Mutex
	received1			map[int]bool
	received2			map[int]bool
	receivedMutex1 		*sync.Mutex
	receivedMutex2 		*sync.Mutex
	dataset				int
	maxBulkSize			int
}

func NewCalculator(bulkSize int) *Calculator {
	calculator := &Calculator {
		data1:				make(map[string]int),
		data2:				make(map[string]string),
		dataMutex1:			&sync.Mutex{},
		dataMutex2:			&sync.Mutex{},
		received1:			make(map[int]bool),
		received2:			make(map[int]bool),
		receivedMutex1:		&sync.Mutex{},
		receivedMutex2:		&sync.Mutex{},
		dataset:			comms.DefaultDataset,
		maxBulkSize:		bulkSize,
	}

	return calculator
}

// Just the funny-businesses information is cleared because city-businesses data is needed for futures datasets.
func (calculator *Calculator) Clear() {
	calculator.dataMutex1.Lock()
	calculator.data1 = make(map[string]int)
	calculator.dataMutex1.Unlock()

	calculator.receivedMutex1.Lock()
	calculator.received1 = make(map[int]bool)
	calculator.receivedMutex1.Unlock()

	log.Infof("Calculator storage cleared.")
}

func (calculator *Calculator) AddFunnyBusiness(datasetNumber int, bulkNumber int, rawData string) {
	proc.ValidateDataSaving(
		datasetNumber,
		bulkNumber,
		rawData,
		&calculator.dataset,
		calculator.received1,
		calculator.receivedMutex1,
		calculator.Clear,
		calculator.saveFunnyBusiness,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d funny businesses stored.", datasetNumber, len(calculator.data1), bulkNumber), bulkNumber)
}

func (calculator *Calculator) saveFunnyBusiness(rawFunbizDataBulk string) {
	var funbizDataList []comms.FunnyBusinessData
	json.Unmarshal([]byte(rawFunbizDataBulk), &funbizDataList)

	for _, funbizData := range funbizDataList {
		calculator.dataMutex1.Lock()
		calculator.data1[funbizData.BusinessId] = funbizData.Funny
		calculator.dataMutex1.Unlock()
	}
}

func (calculator *Calculator) AddCityBusiness(datasetNumber int, bulkNumber int, rawData string) {
	proc.ValidateDataSaving(
		datasetNumber,
		bulkNumber,
		rawData,
		&calculator.dataset,
		calculator.received2,
		calculator.receivedMutex2,
		calculator.Clear,
		calculator.saveCityBusiness,
	)

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d.%d in Joiner: %d city businesses stored.", datasetNumber, len(calculator.data2), bulkNumber), bulkNumber)
}

func (calculator *Calculator) saveCityBusiness(rawCitbizDataBulk string) {
	var citbizDataList []comms.CityBusinessData
	json.Unmarshal([]byte(rawCitbizDataBulk), &citbizDataList)

	for _, citbizData := range citbizDataList {
		calculator.dataMutex2.Lock()
		calculator.data2[citbizData.BusinessId] = citbizData.City
		calculator.dataMutex2.Unlock()
	}
}

func (calculator *Calculator) RetrieveMatches(datasetNumber int) [][]comms.FunnyCityData {
	if datasetNumber != calculator.dataset {
		log.Warnf("Joining data for a dataset not stored (stored #%d but requested data from #%d).", calculator.dataset, datasetNumber)
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

package core

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	utils "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type FilterConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	MinReviews			int
	DishashAggregators	int
	DishashJoiners		int
}

type Filter struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	minReviews 			int
	inputQueue 			*rabbit.RabbitInputQueue
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewFilter(config FilterConfig) *Filter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.DishashAggregatorOutput)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.DishashFilterOutput, comms.EndMessage(config.Instance))

	filter := &Filter {
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		minReviews:			config.MinReviews,
		inputQueue:			inputQueue,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.DishashJoiners, PartitionableValues),
		endSignals:			config.DishashAggregators,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for users distinct text-hashes.")
	innerChannel := make(chan amqp.Delivery)

	closingConn := false
	connMutex := &sync.Mutex{}

	var connWg sync.WaitGroup
	connWg.Add(1)

	var procWg sync.WaitGroup
	procWg.Add(1)

	go proc.InitializeProcessingWorkers(filter.workersPool, innerChannel, filter.callback, &procWg)
	go proc.ProcessInputs(filter.inputQueue.ConsumeData(), innerChannel, filter.endSignals, &procWg, &connWg)
	go proc.ProcessFinish(filter.finishCallback, &procWg, closingConn, connMutex)
	proc.CloseConnection(filter.closeCallback, &procWg, &connWg, closingConn, connMutex)
}

func (filter *Filter) callback(bulkNumber int, bulk string) {
	filteredData := filter.filterData(bulkNumber, bulk)
	filter.sendFilteredData(bulkNumber, filteredData)
}

func (filter *Filter) finishCallback() {
	for _, partition := range utils.GetMapDistinctValues(filter.outputPartitions) {
		filter.outputDirect.PublishFinish(partition)
	}
}

func (filter *Filter) closeCallback() {
	// TODO
}

func (filter *Filter) filterData(bulkNumber int, rawDishashDataBulk string) []comms.DishashData {
	var dishashDataList []comms.DishashData
	var filteredDishashesDataList []comms.DishashData
	json.Unmarshal([]byte(rawDishashDataBulk), &dishashDataList)

	for _, dishashData := range dishashDataList {
		if (dishashData.Distinct == 1) {
			filteredDishashesDataList = append(filteredDishashesDataList, dishashData)	
		}
	}

	return filteredDishashesDataList
}

func (filter *Filter) sendFilteredData(bulkNumber int, filteredBulk []comms.DishashData) {
	dataListByPartition := make(map[string][]comms.DishashData)

	for _, data := range filteredBulk {
		partition := filter.outputPartitions[string(data.UserId[0])]

		if partition != "" {
			dishashDataListPartitioned := dataListByPartition[partition]

			if dishashDataListPartitioned != nil {
				dataListByPartition[partition] = append(dishashDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.DishashData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for user '%s'.", data.UserId)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		outputData, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {

			err := filter.outputDirect.PublishData(outputData, partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, filter.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, filter.outputDirect.Exchange, partition), bulkNumber)
			}	
		}
	}
}

func (filter *Filter) Stop() {
	log.Infof("Closing Distinct-Hash Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}

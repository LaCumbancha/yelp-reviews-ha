package core

import (
	"fmt"
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
	instance 			string
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
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.DishashFilterOutput)

	filter := &Filter {
		instance:			config.Instance,
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
	proc.Transformation(
		filter.workersPool,
		filter.endSignals,
		filter.inputQueue.ConsumeData(),
		filter.mainCallback,
		filter.finishCallback,
		filter.closeCallback,
	)
}

func (filter *Filter) mainCallback(datasetNumber int, bulkNumber int, bulk string) {
	filteredData := filter.filterData(bulk)
	filter.sendFilteredData(datasetNumber, bulkNumber, filteredData)
}

func (filter *Filter) finishCallback(datasetNumber int) {
	rabbit.OutputDirectFinish(comms.FinishMessageSigned(datasetNumber, filter.instance), filter.outputPartitions, filter.outputDirect)
}

func (filter *Filter) closeCallback() {
	// TODO
}

func (filter *Filter) filterData(rawDishashDataBulk string) []comms.DishashData {
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

func (filter *Filter) sendFilteredData(datasetNumber int, bulkNumber int, filteredBulk []comms.DishashData) {
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
		bytes, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {
			data := comms.SignMessage(datasetNumber, filter.instance, bulkNumber, string(bytes))
			err := filter.outputDirect.PublishData([]byte(data), partition)

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

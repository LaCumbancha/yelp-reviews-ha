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
	StarsMappers		int
	StarsAggregators	int
}

type Filter struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	inputQueue 			*rabbit.RabbitInputQueue
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewFilter(config FilterConfig) *Filter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.StarsMapperOutput)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.StarsFilterOutput)

	filter := &Filter {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		inputQueue:			inputQueue,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.StarsAggregators, PartitionableValues),
		endSignals:			config.StarsMappers,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for user stars data.")
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

func (filter *Filter) filterData(rawStarsDataBulk string) []comms.StarsData {
	var starsDataList []comms.StarsData
	var filteredStarsDataList []comms.StarsData
	json.Unmarshal([]byte(rawStarsDataBulk), &starsDataList)

	for _, starsData := range starsDataList {
		if (starsData.Stars == 5.0) {
			filteredStarsDataList = append(filteredStarsDataList, starsData)
		}
	}
	
	return filteredStarsDataList
}

func (filter *Filter) sendFilteredData(datasetNumber int, bulkNumber int, filteredBulk []comms.StarsData) {
	dataListByPartition := make(map[string][]comms.StarsData)

	for _, data := range filteredBulk {
		partition := filter.outputPartitions[string(data.UserId[0])]

		if partition != "" {
			starsDataListPartitioned := dataListByPartition[partition]

			if starsDataListPartitioned != nil {
				dataListByPartition[partition] = append(starsDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.StarsData, 0), data)
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
	log.Infof("Closing Stars Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}

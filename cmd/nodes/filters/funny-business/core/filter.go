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
	FunbizMappers 		int
	FunbizAggregators	int
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

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.FunbizMapperOutput)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.FunbizFilterOutput)

	filter := &Filter {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		inputQueue:			inputQueue,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.FunbizAggregators, PartitionableValues),
		endSignals:			config.FunbizMappers,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for funny-business data.")
	proc.Transformation(
		filter.workersPool,
		filter.endSignals,
		filter.inputQueue.ConsumeData(),
		filter.mainCallback,
		filter.finishCallback,
		filter.closeCallback,
	)
}

func (filter *Filter) mainCallback(bulkNumber int, bulk string) {
	filteredData := filter.filterData(bulkNumber, bulk)
	filter.sendFilteredData(bulkNumber, filteredData)
}

func (filter *Filter) finishCallback(datasetNumber int) {
	rabbit.OutputDirectFinish(comms.EndMessage(filter.instance, datasetNumber), filter.outputPartitions, filter.outputDirect)
}

func (filter *Filter) closeCallback() {
	// TODO
}

func (filter *Filter) filterData(bulkNumber int, rawFunbizDataBulk string) []comms.FunnyBusinessData {
	var funbizDataList []comms.FunnyBusinessData
	var filteredFunbizDataList []comms.FunnyBusinessData
	json.Unmarshal([]byte(rawFunbizDataBulk), &funbizDataList)

	for _, funbizData := range funbizDataList {
		if (funbizData.Funny > 0) {
			filteredFunbizDataList = append(filteredFunbizDataList, funbizData)	
		}
	}

	return filteredFunbizDataList
}

func (filter *Filter) sendFilteredData(bulkNumber int, filteredBulk []comms.FunnyBusinessData) {
	dataListByPartition := make(map[string][]comms.FunnyBusinessData)

	for _, data := range filteredBulk {
		partition := filter.outputPartitions[string(data.BusinessId[0])]

		if partition != "" {
			funbizDataListPartitioned := dataListByPartition[partition]

			if funbizDataListPartitioned != nil {
				dataListByPartition[partition] = append(funbizDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.FunnyBusinessData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for business '%s'.", data.BusinessId)
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
	log.Infof("Closing Funny-Business Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}

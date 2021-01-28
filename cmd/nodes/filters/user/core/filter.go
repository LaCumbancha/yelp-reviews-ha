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

const NODE_CODE = "F4"

type FilterConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	MinReviews			int
	UserAggregators		int
	StarsJoiners		int
}

type Filter struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	minReviews 			int
	inputQueue 			*rabbit.RabbitInputQueue
	outputQueue 		*rabbit.RabbitOutputQueue
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewFilter(config FilterConfig) *Filter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.UserAggregatorOutput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.UserFilterOutput, comms.EndSignals(1))
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.BestUsersFilterOutput)

	filter := &Filter {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		minReviews:			config.MinReviews,
		inputQueue:			inputQueue,
		outputQueue:		outputQueue,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.StarsJoiners, PartitionableValues),
		endSignals:			config.UserAggregators,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for user reviews data.")
	proc.Transformation(
		filter.workersPool,
		filter.endSignals,
		filter.inputQueue.ConsumeData(),
		filter.mainCallback,
		filter.startCallback,
		filter.finishCallback,
		filter.closeCallback,
	)
}

func (filter *Filter) mainCallback(inputNode string, dataset int, instance string, bulk int, data string) {
	filteredData := filter.filterData(data)
	filter.sendFilteredData(dataset, bulk, filteredData)
}

func (filter *Filter) startCallback(dataset int) {
	// Sending Start-Message to consumers.
	rabbit.OutputQueueStart(comms.StartMessageSigned(NODE_CODE, dataset, filter.instance), filter.outputQueue)
	rabbit.OutputDirectStart(comms.StartMessageSigned(NODE_CODE, dataset, filter.instance), filter.outputPartitions, filter.outputDirect)
}

func (filter *Filter) finishCallback(dataset int) {
	// Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(NODE_CODE, dataset, filter.instance), filter.outputQueue)
	rabbit.OutputDirectFinish(comms.FinishMessageSigned(NODE_CODE, dataset, filter.instance), filter.outputPartitions, filter.outputDirect)
}

func (filter *Filter) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(NODE_CODE, filter.instance), filter.outputQueue)
	rabbit.OutputDirectClose(comms.CloseMessageSigned(NODE_CODE, filter.instance), filter.outputPartitions, filter.outputDirect)
}

func (filter *Filter) filterData(rawUserDataBulk string) []comms.UserData {
	var userDataList []comms.UserData
	var filteredUserDataList []comms.UserData
	json.Unmarshal([]byte(rawUserDataBulk), &userDataList)

	for _, userData := range userDataList {
		if (userData.Reviews >= filter.minReviews) {
			filteredUserDataList = append(filteredUserDataList, userData)
		}
	}

	return filteredUserDataList
}

func (filter *Filter) sendFilteredData(dataset int, bulk int, filteredData []comms.UserData) {
	bytes, err := json.Marshal(filteredData)
	if err != nil {
		log.Errorf("Error generating Json from filtered bulk #%d. Err: '%s'", bulk, err)
	} else {
		data := comms.SignMessage(NODE_CODE, dataset, filter.instance, bulk, string(bytes))
		err := filter.outputQueue.PublishData([]byte(data))

		if err != nil {
			log.Errorf("Error sending filtered bulk #%d to output queue %s. Err: '%s'", bulk, filter.outputQueue.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Filtered bulk #%d sent to output queue %s.", bulk, filter.outputQueue.Name), bulk)
		}
	}

	dataListByPartition := make(map[string][]comms.UserData)

	for _, data := range filteredData {
		partition := filter.outputPartitions[string(data.UserId[0])]

		if partition == "" {
			partition = proc.DefaultPartition
			log.Errorf("Couldn't calculate partition for user '%s'. Setting default (%s).", data.UserId, partition)
		}

		userDataListPartitioned := dataListByPartition[partition]
		if userDataListPartitioned != nil {
			dataListByPartition[partition] = append(userDataListPartitioned, data)
		} else {
			dataListByPartition[partition] = append(make([]comms.UserData, 0), data)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		bytes, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {
			data := comms.SignMessage(NODE_CODE, dataset, filter.instance, bulk, string(bytes))
			err := filter.outputDirect.PublishData([]byte(data), partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulk, filter.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulk, filter.outputDirect.Exchange, partition), bulk)
			}	
		}
	}
}

func (filter *Filter) Stop() {
	log.Infof("Closing User Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}

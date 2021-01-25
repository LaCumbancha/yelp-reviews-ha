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
	rabbit.OutputQueueFinish(comms.EndMessage(filter.instance), filter.outputQueue)
	rabbit.OutputDirectFinish(comms.EndMessage(filter.instance), filter.outputPartitions, filter.outputDirect)
}

func (filter *Filter) closeCallback() {
	// TODO
}

func (filter *Filter) filterData(bulkNumber int, rawUserDataBulk string) []comms.UserData {
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

func (filter *Filter) sendFilteredData(bulkNumber int, filteredBulk []comms.UserData) {
	data, err := json.Marshal(filteredBulk)
	if err != nil {
		log.Errorf("Error generating Json from filtered bulk #%d. Err: '%s'", bulkNumber, err)
	} else {
		err := filter.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending filtered bulk #%d to output queue %s. Err: '%s'", bulkNumber, filter.outputQueue.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Filtered bulk #%d sent to output queue %s.", bulkNumber, filter.outputQueue.Name), bulkNumber)
		}
	}

	dataListByPartition := make(map[string][]comms.UserData)

	for _, data := range filteredBulk {
		partition := filter.outputPartitions[string(data.UserId[0])]

		if partition != "" {
			userDataListPartitioned := dataListByPartition[partition]

			if userDataListPartitioned != nil {
				dataListByPartition[partition] = append(userDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.UserData, 0), data)
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
	log.Infof("Closing User Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}

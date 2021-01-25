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

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	InputTopic			string
	HashMappers 		int
	DishashAggregators 	int
	OutputBulkSize		int
}

type Aggregator struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect 		*rabbit.RabbitInputDirect
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.HashMapperOutput, config.InputTopic, "")
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.HashAggregatorOutput)

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect:		inputDirect,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.DishashAggregators, PartitionableValues),
		endSignals:			config.HashMappers,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for hashed-text reviews data.")
	innerChannel := make(chan amqp.Delivery)

	closingConn := false
	connMutex := &sync.Mutex{}

	var connWg sync.WaitGroup
	connWg.Add(1)

	var procWg sync.WaitGroup
	procWg.Add(1)

	go proc.InitializeProcessingWorkers(aggregator.workersPool, innerChannel, aggregator.callback, &procWg)
	go proc.ProcessInputs(aggregator.inputDirect.ConsumeData(), innerChannel, aggregator.endSignals, &procWg, &connWg)
	go proc.ProcessFinish(aggregator.finishCallback, &procWg, closingConn, connMutex)
	proc.CloseConnection(aggregator.closeCallback, &procWg, &connWg, closingConn, connMutex)
}

func (aggregator *Aggregator) callback(bulkNumber int, bulk string) {
	aggregator.calculator.Aggregate(bulkNumber, bulk)
}

func (aggregator *Aggregator) finishCallback(datasetNumber int) {
	// Calculating aggregations
	outputBulkNumber := 0
	for _, aggregatedData := range aggregator.calculator.RetrieveData() {
		outputBulkNumber++
		logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d generated.", outputBulkNumber), outputBulkNumber)
		aggregator.sendAggregatedData(outputBulkNumber, aggregatedData)
	}

	// Clearing Calculator for next dataset.
	aggregator.calculator.Clear()

	// Sending End-Message to consumers.
	rabbit.OutputDirectFinish(comms.EndMessage(aggregator.instance, datasetNumber), aggregator.outputPartitions, aggregator.outputDirect)
}

func (aggregator *Aggregator) closeCallback() {
	// TODO
}

func (aggregator *Aggregator) sendAggregatedData(bulkNumber int, aggregatedBulk []comms.HashedTextData) {
	dataListByPartition := make(map[string][]comms.HashedTextData)

	for _, data := range aggregatedBulk {
		partition := aggregator.outputPartitions[string(data.UserId[0])]

		if partition != "" {
			hashedTextDataListPartitioned := dataListByPartition[partition]

			if hashedTextDataListPartitioned != nil {
				dataListByPartition[partition] = append(hashedTextDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.HashedTextData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for user '%s'.", data.UserId)
		}
	}

	for partition, funbizDataListPartitioned := range dataListByPartition {
		outputData, err := json.Marshal(funbizDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", funbizDataListPartitioned, err)
		} else {

			err := aggregator.outputDirect.PublishData(outputData, partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, aggregator.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, aggregator.outputDirect.Exchange, partition), bulkNumber)
			}	
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Hash-Text Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}

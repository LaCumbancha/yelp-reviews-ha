package core

import (
	"fmt"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
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
	FuncitJoiners		int
	FuncitFilters 		int
	OutputBulkSize		int
}

type Aggregator struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect 		*rabbit.RabbitInputDirect
	outputQueue 		*rabbit.RabbitOutputQueue
	endSignals			int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.FuncitJoinerOutput, config.InputTopic, "")
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.FuncitAggregatorOutput, comms.EndSignals(config.FuncitFilters))

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect:		inputDirect,
		outputQueue:		outputQueue,
		endSignals:			config.FuncitFilters,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for funny-city data.")
	proc.Transformation(
		aggregator.workersPool,
		aggregator.endSignals,
		aggregator.inputDirect.ConsumeData(),
		aggregator.mainCallback,
		aggregator.finishCallback,
		aggregator.closeCallback,
	)
}

func (aggregator *Aggregator) mainCallback(datasetNumber int, bulkNumber int, bulk string) {
	aggregator.calculator.Save(datasetNumber, bulkNumber, bulk)
}

func (aggregator *Aggregator) finishCallback(datasetNumber int) {
	// Calculating aggregations
	outputBulkCounter := 0
	for _, aggregatedData := range aggregator.calculator.AggregateData(datasetNumber) {
		outputBulkCounter++
		logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d generated.", outputBulkCounter), outputBulkCounter)
		aggregator.sendAggregatedData(datasetNumber, outputBulkCounter, aggregatedData)
	}

	// Clearing Calculator for next dataset.
	aggregator.calculator.Clear()

	// Sending End-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(datasetNumber, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) closeCallback() {
	// TODO
}

func (aggregator *Aggregator) sendAggregatedData(datasetNumber int, bulkNumber int, aggregatedBulk []comms.FunnyCityData) {
	bytes, err := json.Marshal(aggregatedBulk)
	if err != nil {
		log.Errorf("Error generating Json from aggregated bulk #%d. Err: '%s'", bulkNumber, err)
	} else {
		data := comms.SignMessage(datasetNumber, aggregator.instance, bulkNumber, string(bytes))
		err := aggregator.outputQueue.PublishData([]byte(data))

		if err != nil {
			log.Errorf("Error sending aggregated bulk #%d to output queue %s. Err: '%s'", bulkNumber, aggregator.outputQueue.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d sent to output queue %s.", bulkNumber, aggregator.outputQueue.Name), bulkNumber)
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Funny-City Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}

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

const NODE_CODE = "A6"

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	InputTopic			string
	HashAggregators		int
	DishashFilters		int
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

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.AggregatorA5_Output, config.InputTopic, "")
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.AggregatorA6_Output, comms.EndSignals(config.DishashFilters))

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect:		inputDirect,
		outputQueue:		outputQueue,
		endSignals:			config.HashAggregators,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for distinct hashed-texts.")
	proc.Transformation(
		aggregator.workersPool,
		aggregator.endSignals,
		aggregator.inputDirect.ConsumeData(),
		aggregator.mainCallback,
		aggregator.startCallback,
		aggregator.finishCallback,
		aggregator.closeCallback,
	)
}

func (aggregator *Aggregator) mainCallback(inputNode string, dataset int, instance string, bulk int, data string) {
	aggregator.calculator.Save(inputNode, dataset, instance, bulk, data)
}

func (aggregator *Aggregator) startCallback(dataset int) {
	// Clearing Calculator for next dataset.
	aggregator.calculator.Clear(dataset)

	// Sending Start-Message to consumers.
	rabbit.OutputQueueStart(comms.StartMessageSigned(NODE_CODE, dataset, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) finishCallback(dataset int) {
	// Calculating aggregations
	outputBulk := 0
	for _, aggregatedData := range aggregator.calculator.AggregateData(dataset) {
		outputBulk++
		logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d generated.", outputBulk), outputBulk)
		aggregator.sendAggregatedData(dataset, outputBulk, aggregatedData)
	}

	// Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(NODE_CODE, dataset, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(NODE_CODE, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) sendAggregatedData(dataset int, bulk int, aggregatedData []comms.DishashData) {
	bytes, err := json.Marshal(aggregatedData)
	if err != nil {
		log.Errorf("Error generating Json from aggregated bulk #%d. Err: '%s'", bulk, err)
	} else {
		data := comms.SignMessage(NODE_CODE, dataset, aggregator.instance, bulk, string(bytes))
		err := aggregator.outputQueue.PublishData([]byte(data))

		if err != nil {
			log.Errorf("Error sending aggregated bulk #%d to output queue %s. Err: '%s'", bulk, aggregator.outputQueue.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d sent to output queue %s.", bulk, aggregator.outputQueue.Name), bulk)
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Distinct-Hash Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}

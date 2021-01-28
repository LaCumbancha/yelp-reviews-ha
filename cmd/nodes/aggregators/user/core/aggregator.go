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

const NODE_CODE = "A7"

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	InputTopic			string
	UserMappers 		int
	UserFilters 		int
	BotUserFilters		int
	OutputBulkSize		int
}

type Aggregator struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect 		*rabbit.RabbitInputDirect
	outputQueue1 		*rabbit.RabbitOutputQueue
	outputQueue2		*rabbit.RabbitOutputQueue
	endSignals			int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.UserMapperOutput, config.InputTopic, "")
	outputQueue1 := rabbit.NewRabbitOutputQueue(channel, props.UserAggregatorOutput, comms.EndSignals(config.UserFilters))
	outputQueue2 := rabbit.NewRabbitOutputQueue(channel, props.BotUsersAggregatorOutput, comms.EndSignals(config.BotUserFilters))

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect:		inputDirect,
		outputQueue1:		outputQueue1,
		outputQueue2:		outputQueue2,
		endSignals:			config.UserMappers,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for user reviews data.")
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
	rabbit.OutputQueueStart(comms.StartMessageSigned(NODE_CODE, dataset, aggregator.instance), aggregator.outputQueue1)
	rabbit.OutputQueueStart(comms.StartMessageSigned(NODE_CODE, dataset, aggregator.instance), aggregator.outputQueue2)
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
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(NODE_CODE, dataset, aggregator.instance), aggregator.outputQueue1)
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(NODE_CODE, dataset, aggregator.instance), aggregator.outputQueue2)
}

func (aggregator *Aggregator) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(NODE_CODE, aggregator.instance), aggregator.outputQueue1)
	rabbit.OutputQueueClose(comms.CloseMessageSigned(NODE_CODE, aggregator.instance), aggregator.outputQueue2)
}

func (aggregator *Aggregator) sendAggregatedData(dataset int, bulk int, aggregatedData []comms.UserData) {
	bytes, err := json.Marshal(aggregatedData)
	if err != nil {
		log.Errorf("Error generating Json from aggregated bulk #%d. Err: '%s'", bulk, err)
	} else {
		data := comms.SignMessage(NODE_CODE, dataset, aggregator.instance, bulk, string(bytes))

		err = aggregator.outputQueue1.PublishData([]byte(data))
		if err != nil {
			log.Errorf("Error sending aggregated bulk #%d to output queue %s. Err: '%s'", bulk, aggregator.outputQueue1.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d sent to output queue %s.", bulk, aggregator.outputQueue1.Name), bulk)
		}

		err = aggregator.outputQueue2.PublishData([]byte(data))
		if err != nil {
			log.Errorf("Error sending aggregated bulk #%d to output queue %s. Err: '%s'", bulk, aggregator.outputQueue2.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d sent to output queue %s.", bulk, aggregator.outputQueue2.Name), bulk)
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing User Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}

package core

import (
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
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
	WeekdayMappers 		int
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

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.WeekdayMapperOutput, config.InputTopic, "")
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.WeekdayAggregatorOutput, comms.EndSignals(1))

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(),
		inputDirect:		inputDirect,
		outputQueue:		outputQueue,
		endSignals:			config.WeekdayMappers,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for weekday data.")
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
	weekdayNumber := 1
	for _, aggregatedData := range aggregator.calculator.AggregateData(datasetNumber) {
		aggregator.sendAggregatedData(datasetNumber, weekdayNumber, aggregatedData)
		weekdayNumber++
	}

	// Clearing Calculator for next dataset.
	aggregator.calculator.Clear()

	// Sending End-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(datasetNumber, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) closeCallback() {
	// TODO
}

func (aggregator *Aggregator) sendAggregatedData(datasetNumber int, weekdayNumber int, aggregatedData comms.WeekdayData) {
	weekday := strings.ToUpper(aggregatedData.Weekday[0:3])
	bytes, err := json.Marshal(aggregatedData)

	if err != nil {
		log.Errorf("Error generating Json from %s aggregated data. Err: '%s'", weekday, err)
	} else {
		data := comms.SignMessage(datasetNumber, aggregator.instance, weekdayNumber, string(bytes))
		err := aggregator.outputQueue.PublishData([]byte(data))

		if err != nil {
			log.Errorf("Error sending %s aggregated data to output queue %s. Err: '%s'", weekday, aggregator.outputQueue.Name, err)
		} else {
			log.Infof("%s aggregated data sent to output queue %s.", weekday, aggregator.outputQueue.Name)
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Funny-Business Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}

package core

import (
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

const NODE_CODE = "A3"

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	FuncitAggregators	int
	TopSize				int
}

type Aggregator struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputQueue 			*rabbit.RabbitInputQueue
	outputQueue 		*rabbit.RabbitOutputQueue
	endSignals			int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.FuncitAggregatorOutput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.FuncitTopOutput, comms.EndSignals(1))

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.TopSize),
		inputQueue:			inputQueue,
		outputQueue:		outputQueue,
		endSignals:			config.FuncitAggregators,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for funny-city data.")
	proc.Transformation(
		aggregator.workersPool,
		aggregator.endSignals,
		aggregator.inputQueue.ConsumeData(),
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
	cityCounter := 0
	for _, cityData := range aggregator.calculator.AggregateData(dataset) {
		cityCounter++
		aggregator.sendTopTenData(dataset, cityCounter, cityData)
	}

	// Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(NODE_CODE, dataset, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(NODE_CODE, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) sendTopTenData(dataset int, cityNumber int, topTenCity comms.FunnyCityData) {
	bytes, err := json.Marshal(topTenCity)
	if err != nil {
		log.Errorf("Error generating Json from funniest city #%d data. Err: '%s'", cityNumber, err)
	} else {
		data := comms.SignMessage(NODE_CODE, dataset, aggregator.instance, cityNumber, string(bytes))
		err := aggregator.outputQueue.PublishData([]byte(data))

		if err != nil {
			log.Errorf("Error sending funniest city #%d data to output queue %s. Err: '%s'", cityNumber, aggregator.outputQueue.Name, err)
		} else {
			log.Infof("Funniest city #%d data sent to output queue %s.", cityNumber, aggregator.outputQueue.Name)
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Funny-City Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}

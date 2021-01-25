package common

import (
	"sync"
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
	for _, aggregatedData := range aggregator.calculator.RetrieveData() {
		aggregator.sendAggregatedData(aggregatedData)
	}

	// Clearing Calculator for next dataset.
	aggregator.calculator.Clear()

	// Sending End-Message to consumers.
	rabbit.OutputQueueFinish(comms.EndMessage(aggregator.instance, datasetNumber), aggregator.outputQueue)
}

func (aggregator *Aggregator) closeCallback() {
	// TODO
}

func (aggregator *Aggregator) sendAggregatedData(aggregatedData comms.WeekdayData) {
	weekday := strings.ToUpper(aggregatedData.Weekday[0:3])

	data, err := json.Marshal(aggregatedData)
	if err != nil {
		log.Errorf("Error generating Json from %s aggregated data. Err: '%s'", weekday, err)
	} else {
		err := aggregator.outputQueue.PublishData(data)

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

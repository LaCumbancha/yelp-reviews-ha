package core

import (
	"sync"
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
	FuncitAggregators	int
	TopSize				int
}

type Aggregator struct {
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
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.FuncitTopOutput, comms.EndMessage(config.Instance), comms.EndSignals(1))

	aggregator := &Aggregator {
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
	innerChannel := make(chan amqp.Delivery)

	closingConn := false
	connMutex := &sync.Mutex{}

	var connWg sync.WaitGroup
	connWg.Add(1)

	var procWg sync.WaitGroup
	procWg.Add(1)

	go proc.InitializeProcessingWorkers(aggregator.workersPool, innerChannel, aggregator.callback, &procWg)
	go proc.ProcessInputs(aggregator.inputQueue.ConsumeData(), innerChannel, aggregator.endSignals, &procWg, &connWg)
	go proc.ProcessFinish(aggregator.finishCallback, &procWg, closingConn, connMutex)
	proc.CloseConnection(aggregator.closeCallback, &procWg, &connWg, closingConn, connMutex)
}

func (aggregator *Aggregator) callback(bulkNumber int, bulk string) {
	aggregator.calculator.Save(bulkNumber, bulk)
}

func (aggregator *Aggregator) finishCallback() {
	// Calculating aggregations
	cityCounter := 0
	for _, cityData := range aggregator.calculator.RetrieveTopTen() {
		cityCounter++
		aggregator.sendTopTenData(cityCounter, cityData)
	}

	// Clearing Calculator for next dataset.
	aggregator.calculator.Clear()

	// Sending End-Message to consumers.
	aggregator.outputQueue.PublishFinish()
}

func (aggregator *Aggregator) closeCallback() {
	// TODO
}

func (aggregator *Aggregator) sendTopTenData(cityNumber int, topTenCity comms.FunnyCityData) {
	data, err := json.Marshal(topTenCity)
	if err != nil {
		log.Errorf("Error generating Json from funniest city #%d data. Err: '%s'", cityNumber, err)
	} else {
		err := aggregator.outputQueue.PublishData(data)

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

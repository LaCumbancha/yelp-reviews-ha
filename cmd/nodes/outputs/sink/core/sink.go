package core

import (
	"sync"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

const FUNCIT = "Funniest Cities"
const WEEKDAY = "Weekday Histogram"
const TOPUSERS = "Top-Users"
const BOTUSERS = "Bot-Users"
const BESTUSERS = "Best-Users"

type SinkConfig struct {
	RabbitIp					string
	RabbitPort					string
}

type Sink struct {
	connection 					*amqp.Connection
	channel 					*amqp.Channel
	funniestCitiesQueue 		*rabbit.RabbitInputQueue
	weekdayHistogramQueue 		*rabbit.RabbitInputQueue
	topUsersQueue 				*rabbit.RabbitInputQueue
	bestUsersQueue 				*rabbit.RabbitInputQueue
	botUsersQueue 				*rabbit.RabbitInputQueue
}

func NewSink(config SinkConfig) *Sink {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	funniestCitiesQueue := rabbit.NewRabbitInputQueue(channel, props.FunniestCitiesPrettierOutput)
	weekdayHistogramQueue := rabbit.NewRabbitInputQueue(channel, props.WeekdayHistogramPrettierOutput)
	topUsersQueue := rabbit.NewRabbitInputQueue(channel, props.TopUsersPrettierOutput)
	bestUsersQueue := rabbit.NewRabbitInputQueue(channel, props.BestUsersPrettierOutput)
	botUsersQueue := rabbit.NewRabbitInputQueue(channel, props.BotUsersPrettierOutput)

	sink := &Sink {
		connection:				connection,
		channel:				channel,
		funniestCitiesQueue:	funniestCitiesQueue,
		weekdayHistogramQueue:  weekdayHistogramQueue,
		topUsersQueue:			topUsersQueue,
		bestUsersQueue:			bestUsersQueue,
		botUsersQueue:			botUsersQueue,
	}

	return sink
}

func (sink *Sink) Run() {
	log.Infof("Starting to listen for results.")

	closingConn := false
	connMutex := &sync.Mutex{}

	var procWg sync.WaitGroup
	procWg.Add(5)

	var connWg sync.WaitGroup
	connWg.Add(1)

	go sink.retrieveFlowResults(TOPUSERS, sink.topUsersQueue, &procWg, &connWg)
	go sink.retrieveFlowResults(BOTUSERS, sink.botUsersQueue, &procWg, &connWg)
	go sink.retrieveFlowResults(BESTUSERS, sink.bestUsersQueue, &procWg, &connWg)
	go sink.retrieveFlowResults(FUNCIT, sink.funniestCitiesQueue, &procWg, &connWg)
	go sink.retrieveFlowResults(WEEKDAY, sink.weekdayHistogramQueue, &procWg, &connWg)

	go proc.ProcessFinish(sink.finishCallback, &procWg, closingConn, connMutex)
	proc.CloseConnection(sink.closeCallback, &procWg, &connWg, closingConn, connMutex)
}

func (sink *Sink) retrieveFlowResults(flow string, inputQueue *rabbit.RabbitInputQueue, procWg *sync.WaitGroup, connWg * sync.WaitGroup) {
	datasetNumber := 1
	distinctEndSignals := make(map[string]int)
	distinctCloseSignals := make(map[string]int)

	for message := range inputQueue.ConsumeData() {
		messageBody := string(message.Body)
		if comms.IsCloseMessage(messageBody) {
			_, allCloseReceived := comms.LastEndMessage(messageBody, datasetNumber, distinctCloseSignals, 1)

			if allCloseReceived {
				log.Infof("Close-Message received from the %s flow.", flow)
				connWg.Done()
			}

		} else if comms.IsEndMessage(messageBody) {
			_, allFinishReceived := comms.LastEndMessage(messageBody, datasetNumber, distinctEndSignals, 1)

			if allFinishReceived {
				// Clearing End-Messages flags.
				datasetNumber++
				distinctEndSignals = make(map[string]int)

				log.Infof("End-Message received from the %s flow.", flow)
				procWg.Done()
			}

		} else {
			log.Infof(messageBody)
		}
	}
}

func (sink *Sink) finishCallback(datasetNumber int) {
	log.Infof("Dataset #%d analysis finished.", datasetNumber)
}

func (sink *Sink) closeCallback() {
	log.Infof("Closing process.")
}

func (sink *Sink) Stop() {
	log.Infof("Closing Sink connections.")
	sink.connection.Close()
	sink.channel.Close()
}

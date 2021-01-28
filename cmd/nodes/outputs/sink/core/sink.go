package core

import (
	"sync"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
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
	mainChannel := make(chan amqp.Delivery)
	endingChannel := make(chan int)

	closingConn := false
	connMutex := &sync.Mutex{}

	var connWg sync.WaitGroup
	connWg.Add(1)

	var procWg sync.WaitGroup
	procWg.Add(5)

	go proc.InitializeProcessingWorkers(1, mainChannel, sink.mainCallback, &procWg)
	go proc.ProcessInputs(TOPUSERS, sink.topUsersQueue.ConsumeData(), mainChannel, endingChannel, 1, &procWg, &connWg)
	go proc.ProcessInputs(BOTUSERS, sink.botUsersQueue.ConsumeData(), mainChannel, endingChannel, 1, &procWg, &connWg)
	go proc.ProcessInputs(BESTUSERS, sink.bestUsersQueue.ConsumeData(), mainChannel, endingChannel, 1, &procWg, &connWg)
	go proc.ProcessInputs(FUNCIT, sink.funniestCitiesQueue.ConsumeData(), mainChannel, endingChannel, 1, &procWg, &connWg)
	go proc.ProcessInputs(WEEKDAY, sink.weekdayHistogramQueue.ConsumeData(), mainChannel, endingChannel, 1, &procWg, &connWg)

	neededInputs := 5
	savedInputs := 0
	go proc.ProcessMultipleFinish(neededInputs, savedInputs, endingChannel, sink.finishCallback, &procWg, closingConn, connMutex)
	proc.CloseConnection(sink.closeCallback, &procWg, &connWg, closingConn, connMutex)
}

func (sink *Sink) mainCallback(nodeCode string, dataset int, instance string, bulk int, message string) {
	log.Infof(message)
}

func (sink *Sink) finishCallback(dataset int) {
	log.Infof("Dataset #%d analysis finished.", dataset)
}

func (sink *Sink) closeCallback() {
	log.Infof("Closing process.")
}

func (sink *Sink) Stop() {
	log.Infof("Closing Sink connections.")
	sink.connection.Close()
	sink.channel.Close()
}

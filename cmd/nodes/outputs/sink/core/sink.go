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
	startingChannel := make(chan amqp.Delivery)
	finishingChannel := make(chan amqp.Delivery)
	closingChannel := make(chan *proc.FlowMessage)

	var procWgs = make(map[int]*sync.WaitGroup)
	var procWgsMutex = &sync.Mutex{}
	var finishWg sync.WaitGroup
	var connWg sync.WaitGroup
	connWg.Add(1)

	neededInputs := 5
	savedInputs := 0
	startSignals, finishSignals, closeSignals := proc.LoadBackupedSignals()
	proc.InitializeProcessWaitGroups(procWgs, procWgsMutex, startSignals, finishSignals, neededInputs, savedInputs)

	go proc.ProcessData(1, mainChannel, sink.mainCallback, procWgs, procWgsMutex)
	go proc.ReceiveInputs(TOPUSERS, sink.topUsersQueue.ConsumeData(), mainChannel, startingChannel, finishingChannel, closingChannel, 1, procWgs, procWgsMutex)
	go proc.ReceiveInputs(BOTUSERS, sink.botUsersQueue.ConsumeData(), mainChannel, startingChannel, finishingChannel, closingChannel, 1, procWgs, procWgsMutex)
	go proc.ReceiveInputs(BESTUSERS, sink.bestUsersQueue.ConsumeData(), mainChannel, startingChannel, finishingChannel, closingChannel, 1, procWgs, procWgsMutex)
	go proc.ReceiveInputs(FUNCIT, sink.funniestCitiesQueue.ConsumeData(), mainChannel, startingChannel, finishingChannel, closingChannel, 1, procWgs, procWgsMutex)
	go proc.ReceiveInputs(WEEKDAY, sink.weekdayHistogramQueue.ConsumeData(), mainChannel, startingChannel, finishingChannel, closingChannel, 1, procWgs, procWgsMutex)

	go proc.ProcessStart(startSignals, neededInputs, savedInputs, startingChannel, sink.startCallback)
	go proc.ProcessFinish(finishSignals, neededInputs, savedInputs, finishingChannel, sink.finishCallback, procWgs, procWgsMutex, &finishWg)
	go proc.ProcessClose(closeSignals, neededInputs, closingChannel, sink.closeCallback, procWgs, procWgsMutex, &finishWg, &connWg)
	connWg.Wait()
}

func (sink *Sink) mainCallback(nodeCode string, dataset int, instance string, bulk int, message string) {
	log.Infof(message)
}

func (sink *Sink) startCallback(dataset int) {
	log.Infof("Dataset #%d analysis started.", dataset)
}

func (sink *Sink) finishCallback(dataset int) {
	log.Infof("Dataset #%d analysis finished.", dataset)
}

func (sink *Sink) closeCallback() {
	log.Infof("Closing analysis process.")
}

func (sink *Sink) Stop() {
	log.Infof("Closing Sink connections.")
	sink.connection.Close()
	sink.channel.Close()
}

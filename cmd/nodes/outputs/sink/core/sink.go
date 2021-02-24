package core

import (
	"time"
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
	timeMap						map[int]time.Time
	timeMutex					*sync.Mutex
}

func NewSink(config SinkConfig) *Sink {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	funniestCitiesQueue := rabbit.NewRabbitInputQueue(channel, props.PrettierP1_Output)
	weekdayHistogramQueue := rabbit.NewRabbitInputQueue(channel, props.PrettierP2_Output)
	topUsersQueue := rabbit.NewRabbitInputQueue(channel, props.PrettierP4_Output)
	bestUsersQueue := rabbit.NewRabbitInputQueue(channel, props.PrettierP5_Output)
	botUsersQueue := rabbit.NewRabbitInputQueue(channel, props.PrettierP3_Output)

	sink := &Sink {
		connection:				connection,
		channel:				channel,
		funniestCitiesQueue:	funniestCitiesQueue,
		weekdayHistogramQueue:  weekdayHistogramQueue,
		topUsersQueue:			topUsersQueue,
		bestUsersQueue:			bestUsersQueue,
		botUsersQueue:			botUsersQueue,
		timeMap:				make(map[int]time.Time),
		timeMutex:				&sync.Mutex{},
	}

	return sink
}

func (sink *Sink) Run() {
	log.Infof("Starting to listen for results.")
	workersPool := 1
	endSignalsNeeded := map[string]int{
		props.PrettierP1_Name: 1,
		props.PrettierP2_Name: 1,
		props.PrettierP3_Name: 1,
		props.PrettierP4_Name: 1,
		props.PrettierP5_Name: 1,
	}
	dataByInput := map[string]<-chan amqp.Delivery{
		props.PrettierP1_Name: sink.funniestCitiesQueue.ConsumeData(),
		props.PrettierP2_Name: sink.weekdayHistogramQueue.ConsumeData(),
		props.PrettierP3_Name: sink.botUsersQueue.ConsumeData(),
		props.PrettierP4_Name: sink.topUsersQueue.ConsumeData(),
		props.PrettierP5_Name: sink.bestUsersQueue.ConsumeData(),
	}
	mainCallbackByInput := map[string]func(string, int, string, int, string){
		props.PrettierP1_Name: sink.mainCallback,
		props.PrettierP2_Name: sink.mainCallback,
		props.PrettierP3_Name: sink.mainCallback,
		props.PrettierP4_Name: sink.mainCallback,
		props.PrettierP5_Name: sink.mainCallback,
	}
	
	proc.ProcessInputsStatelessly(
		dataByInput,
		workersPool,
		endSignalsNeeded,
		[]string{},
		mainCallbackByInput,
		sink.startCallback,
		sink.finishCallback,
		sink.closeCallback,
	)
}

func (sink *Sink) mainCallback(nodeCode string, dataset int, instance string, bulk int, message string) {
	log.Infof(message)
}

func (sink *Sink) startCallback(dataset int) {
	log.Infof("Dataset #%d analysis started.", dataset)
	sink.timeMutex.Lock()
	sink.timeMap[dataset] = time.Now()
	sink.timeMutex.Unlock()
}

func (sink *Sink) finishCallback(dataset int) {
	sink.timeMutex.Lock()
	startTime, found := sink.timeMap[dataset]
	delete(sink.timeMap, dataset)
	sink.timeMutex.Unlock()

	if found {
		log.Infof("Dataset #%d analysis finished in %s.", dataset, time.Now().Sub(startTime).String())
	} else {
		log.Warnf("Starting time not found for dataset %d.", dataset)
		log.Infof("Dataset #%d analysis finished.", dataset)
	}
}

func (sink *Sink) closeCallback() {
	log.Infof("Closing analysis process.")
}

func (sink *Sink) Stop() {
	log.Infof("Closing Sink connections.")
	sink.connection.Close()
	sink.channel.Close()
}

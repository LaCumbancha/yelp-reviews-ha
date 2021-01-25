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

type PrettierConfig struct {
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	MinReviews			int
	BestUserJoiners 	int
}

type Prettier struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	builder				*Builder
	inputQueue 			*rabbit.RabbitInputQueue
	outputQueue 		*rabbit.RabbitOutputQueue
	endSignals 			int
}

func NewPrettier(config PrettierConfig) *Prettier {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.BestUsersJoinerOutput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.BestUsersPrettierOutput, comms.EndSignals(1))

	prettier := &Prettier {
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		builder:			NewBuilder(config.MinReviews),
		inputQueue:			inputQueue,
		outputQueue:		outputQueue,
		endSignals:			config.BestUserJoiners,
	}

	return prettier
}

func (prettier *Prettier) Run() {
	log.Infof("Starting to listen for users with +50 reviews, only 5-stars.")
	innerChannel := make(chan amqp.Delivery)

	closingConn := false
	connMutex := &sync.Mutex{}

	var connWg sync.WaitGroup
	connWg.Add(1)

	var procWg sync.WaitGroup
	procWg.Add(1)

	go proc.InitializeProcessingWorkers(prettier.workersPool, innerChannel, prettier.callback, &procWg)
	go proc.ProcessInputs(prettier.inputQueue.ConsumeData(), innerChannel, prettier.endSignals, &procWg, &connWg)
	go proc.ProcessFinish(prettier.finishCallback, &procWg, closingConn, connMutex)
	proc.CloseConnection(prettier.closeCallback, &procWg, &connWg, closingConn, connMutex)
}

func (prettier *Prettier) callback(bulkNumber int, bulk string) {
	prettier.builder.Save(bulk)
}

func (prettier *Prettier) finishCallback() {
	prettier.sendResults()

	// There's no need for an instance definition in the End-Message because there's only one Prettier
	rabbit.OutputQueueFinish(comms.EndMessage(""), prettier.outputQueue)
}

func (prettier *Prettier) closeCallback() {
	// TODO
}

func (prettier *Prettier) sendResults() {
	data := []byte(prettier.builder.BuildData())
	err := prettier.outputQueue.PublishData(data)

	if err != nil {
		log.Errorf("Error sending best users results to output queue %s. Err: '%s'", prettier.outputQueue.Name, err)
	} else {
		log.Infof("Best user results sent to output queue %s.", prettier.outputQueue.Name)
	}
}

func (prettier *Prettier) Stop() {
	log.Infof("Closing Best-Users Prettier connections.")
	prettier.connection.Close()
	prettier.channel.Close()
}

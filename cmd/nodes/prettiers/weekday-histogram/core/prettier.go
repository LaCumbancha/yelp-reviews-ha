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
	WeekdayAggregators 	int
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

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.WeekdayAggregatorOutput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.WeekdayHistogramPrettierOutput, comms.EndMessage(""), comms.EndSignals(1))

	prettier := &Prettier {
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		builder:			NewBuilder(),
		inputQueue:			inputQueue,
		outputQueue:		outputQueue,
		endSignals:			config.WeekdayAggregators,
	}

	return prettier
}

func (prettier *Prettier) Run() {
	log.Infof("Starting to listen for weekday reviews data.")
	innerChannel := make(chan amqp.Delivery)

	var wg sync.WaitGroup
	wg.Add(1)

	go proc.InitializeProcessingWorkers(prettier.workersPool, innerChannel, prettier.callback, &wg)
	go proc.ProcessInputs(prettier.inputQueue.ConsumeData(), innerChannel, prettier.endSignals, &wg)
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Sending results
    prettier.sendResults()

    // Publishing end messages.
    prettier.outputQueue.PublishFinish()
}

func (prettier *Prettier) callback(bulkNumber int, bulk string) {
	prettier.builder.Save(bulk)
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
	log.Infof("Closing Weekday-Histogram Prettier connections.")
	prettier.connection.Close()
	prettier.channel.Close()
}

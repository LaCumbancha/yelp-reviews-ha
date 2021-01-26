package core

import (
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
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.WeekdayHistogramPrettierOutput, comms.EndSignals(1))

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
	proc.Transformation(
		prettier.workersPool,
		prettier.endSignals,
		prettier.inputQueue.ConsumeData(),
		prettier.mainCallback,
		prettier.finishCallback,
		prettier.closeCallback,
	)
}

func (prettier *Prettier) mainCallback(bulkNumber int, bulk string) {
	prettier.builder.Save(bulk)
}

func (prettier *Prettier) finishCallback(datasetNumber int) {
	// Sending results
	prettier.sendResults()

	// Clearing Calculator for next dataset.
	prettier.builder.Clear()

    // There's no need for an instance definition in the End-Message because there's only one Prettier
	rabbit.OutputQueueFinish(comms.EndMessage("", datasetNumber), prettier.outputQueue)
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
	log.Infof("Closing Weekday-Histogram Prettier connections.")
	prettier.connection.Close()
	prettier.channel.Close()
}

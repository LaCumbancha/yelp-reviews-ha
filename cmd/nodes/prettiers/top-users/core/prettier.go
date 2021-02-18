package core

import (
	"github.com/streadway/amqp"
	
	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

const NODE_CODE = "P4"

type PrettierConfig struct {
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	MinReviews			int
	UserFilters 		int
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

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.FilterF4_Output1)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.PrettierP4_Output, comms.EndSignals(1))

	prettier := &Prettier {
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		builder:			NewBuilder(config.MinReviews),
		inputQueue:			inputQueue,
		outputQueue:		outputQueue,
		endSignals:			config.UserFilters,
	}

	return prettier
}

func (prettier *Prettier) Run() {
	log.Infof("Starting to listen for users with +50 reviews data.")
	proc.Transformation(
		prettier.workersPool,
		prettier.endSignals,
		prettier.inputQueue.ConsumeData(),
		prettier.mainCallback,
		prettier.startCallback,
		prettier.finishCallback,
		prettier.closeCallback,
	)
}

func (prettier *Prettier) mainCallback(inputNode string, dataset int, instance string, bulk int, data string) {
	prettier.builder.Save(inputNode, dataset, instance, bulk, data)
}

func (prettier *Prettier) startCallback(dataset int) {
	// Clearing Calculator for next dataset.
	prettier.builder.Clear(dataset)
	
	// Sending Start-Message to consumers.
	rabbit.OutputQueueStart(comms.StartMessageSigned(NODE_CODE, dataset, "0"), prettier.outputQueue)
}

func (prettier *Prettier) finishCallback(dataset int) {
	// Sending results
	prettier.sendResults(dataset)

    // Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(NODE_CODE, dataset, "0"), prettier.outputQueue)
}

func (prettier *Prettier) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(NODE_CODE, "0"), prettier.outputQueue)
}

func (prettier *Prettier) sendResults(dataset int) {
	messageNumber := 1
	prettierInstance := "0"
	data := comms.SignMessage(NODE_CODE, dataset, prettierInstance, messageNumber, prettier.builder.BuildData(dataset))
	err := prettier.outputQueue.PublishData([]byte(data))

	if err != nil {
		log.Errorf("Error sending best users results to output queue %s. Err: '%s'", prettier.outputQueue.Name, err)
	} else {
		log.Infof("Best user results sent to output queue %s.", prettier.outputQueue.Name)
	}
}

func (prettier *Prettier) Stop() {
	log.Infof("Closing Top-Users Prettier connections.")
	prettier.connection.Close()
	prettier.channel.Close()
}

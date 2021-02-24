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
	MinReviews			int
	BotsAggregators 	int
}

type Prettier struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	builder				*Builder
	inputQueue 			*rabbit.RabbitInputQueue
	outputQueue 		*rabbit.RabbitOutputQueue
	endSignalsNeeded	map[string]int
}

func NewPrettier(config PrettierConfig) *Prettier {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.AggregatorA5_Output)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.PrettierP3_Output, comms.Outputs)

	endSignalsNeeded := map[string]int{props.AggregatorA5_Name: config.BotsAggregators}

	prettier := &Prettier {
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		builder:			NewBuilder(config.MinReviews),
		inputQueue:			inputQueue,
		outputQueue:		outputQueue,
		endSignalsNeeded:	endSignalsNeeded,
	}

	return prettier
}

func (prettier *Prettier) Run() {
	log.Infof("Starting to listen for users with +5 reviews, all with the same text.")
	dataByInput := map[string]<-chan amqp.Delivery{props.AggregatorA5_Name: prettier.inputQueue.ConsumeData()}
	mainCallbackByInput := map[string]func(string, int, string, int, string){props.AggregatorA5_Name: prettier.mainCallback}
	backupCallbackByInput := map[string]func(int, []string){props.AggregatorA5_Name: prettier.builder.LoadBackup}
	
	proc.ProcessInputsStatefully(
		dataByInput,
		prettier.workersPool,
		prettier.endSignalsNeeded,
		[]string{},
		mainCallbackByInput,
		backupCallbackByInput,
		prettier.startCallback,
		prettier.finishCallback,
		prettier.closeCallback,
	)
}

func (prettier *Prettier) mainCallback(inputNode string, dataset int, instance string, bulk int, data string) {
	prettier.builder.Save(dataset, bulk, data)
}

func (prettier *Prettier) startCallback(dataset int) {
	// Initializing new dataset in Builder.
	prettier.builder.RegisterDataset(dataset)

	// Sending Start-Message to consumers.
	rabbit.OutputQueueStart(comms.StartMessageSigned(props.PrettierP3_Name, dataset, "0"), prettier.outputQueue)
}

func (prettier *Prettier) finishCallback(dataset int) {
	// Sending results.
	prettier.sendResults(dataset)

	// Removing processed dataset from Builder.
	prettier.builder.Clear(dataset)

    // Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(props.PrettierP3_Name, dataset, "0"), prettier.outputQueue)
}

func (prettier *Prettier) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(props.PrettierP3_Name, "0"), prettier.outputQueue)
}

func (prettier *Prettier) sendResults(dataset int) {
	messageNumber := 1
	prettierInstance := "0"
	data := comms.SignMessage(props.PrettierP3_Name, dataset, prettierInstance, messageNumber, prettier.builder.BuildData(dataset))
	err := prettier.outputQueue.PublishData([]byte(data))

	if err != nil {
		log.Errorf("Error sending bot users results to output queue %s. Err: '%s'", prettier.outputQueue.Name, err)
	} else {
		log.Infof("Bot user results sent to output queue %s.", prettier.outputQueue.Name)
	}
}

func (prettier *Prettier) Stop() {
	log.Infof("Closing Bot-Users Prettier connections.")
	prettier.connection.Close()
	prettier.channel.Close()
}

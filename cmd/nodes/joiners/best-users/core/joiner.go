package core

import (
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type JoinerConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	InputTopic			string
	StarsAggregators	int
	UserFilters 		int
}

type Joiner struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect1 		*rabbit.RabbitInputDirect
	inputDirect2 		*rabbit.RabbitInputDirect
	outputQueue 		*rabbit.RabbitOutputQueue
	endSignals1			int
	endSignals2			int
}

func NewJoiner(config JoinerConfig) *Joiner {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect1 := rabbit.NewRabbitInputDirect(channel, props.StarsAggregatorOutput, config.InputTopic, "")
	inputDirect2 := rabbit.NewRabbitInputDirect(channel, props.BestUsersFilterOutput, config.InputTopic, "")
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.BestUsersJoinerOutput, comms.EndSignals(1))

	joiner := &Joiner {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(),
		inputDirect1:		inputDirect1,
		inputDirect2:		inputDirect2,
		outputQueue:		outputQueue,
		endSignals1:		config.StarsAggregators,
		endSignals2:		config.UserFilters,
	}

	return joiner
}

func (joiner *Joiner) Run() {
	reloadWaitCount := 2
	log.Infof("Starting to listen for common users and best users (with just 5-stars reviews).")
	proc.Join(
		joiner.workersPool,
		joiner.endSignals1,
		joiner.endSignals2,
		joiner.inputDirect1.ConsumeData(),
		joiner.inputDirect2.ConsumeData(),
		joiner.mainCallback1,
		joiner.mainCallback2,
		joiner.finishCallback,
		joiner.closeCallback,
		reloadWaitCount,
	)
}

func (joiner *Joiner) mainCallback1(bulkNumber int, bulk string) {
	joiner.calculator.AddBestUser(bulkNumber, bulk)
}

func (joiner *Joiner) mainCallback2(bulkNumber int, bulk string) {
	joiner.calculator.AddUser(bulkNumber, bulk)
}

func (joiner *Joiner) finishCallback(datasetNumber int) {
	// Retrieving join matches.
	joinMatches := joiner.calculator.RetrieveMatches()

	if len(joinMatches) == 0 {
    	log.Warnf("No join match to send.")
    }

    messageCounter := 0
    for _, joinedData := range joinMatches {
    	messageCounter++
    	joiner.sendJoinedData(messageCounter, joinedData)
	}

	// Clearing Calculator for next dataset.
	joiner.calculator.Clear()

	// Sending End-Message to consumers.
	rabbit.OutputQueueFinish(comms.EndMessage(joiner.instance, datasetNumber), joiner.outputQueue)
}

func (joiner *Joiner) closeCallback() {
	// TODO
}

func (joiner *Joiner) sendJoinedData(messageNumber int, joinedData comms.UserData) {
	data, err := json.Marshal(joinedData)
	if err != nil {
		log.Errorf("Error generating Json from joined best user #%d. Err: '%s'", messageNumber, err)
	} else {
		err := joiner.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending joined best user #%d to output queue %s. Err: '%s'", messageNumber, joiner.outputQueue.Name, err)
		} else {
			log.Infof("Joined best user #%d sent to output queue %s.", messageNumber, joiner.outputQueue.Name)
		}
	}
}

func (joiner *Joiner) Stop() {
	log.Infof("Closing Best-Users Joiner connections.")
	joiner.connection.Close()
	joiner.channel.Close()
}

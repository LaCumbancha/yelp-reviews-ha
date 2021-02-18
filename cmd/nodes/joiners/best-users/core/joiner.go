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

const NODE_CODE = "J3"
const FLOW1 = "Best-Users"
const FLOW2 = "Common-Users"

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

	inputDirect1 := rabbit.NewRabbitInputDirect(channel, props.AggregatorA8_Output, config.InputTopic, "")
	inputDirect2 := rabbit.NewRabbitInputDirect(channel, props.FilterF4_Output2, config.InputTopic, "")
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.JoinerJ3_Output, comms.EndSignals(1))

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
	neededInputs := 2
	savedInputs := 0
	log.Infof("Starting to listen for common users and best users (with just 5-stars reviews).")
	proc.Join(
		FLOW1,
		FLOW2,
		neededInputs,
		savedInputs,
		joiner.workersPool,
		joiner.endSignals1,
		joiner.endSignals2,
		joiner.inputDirect1.ConsumeData(),
		joiner.inputDirect2.ConsumeData(),
		joiner.mainCallback1,
		joiner.mainCallback2,
		joiner.startCallback,
		joiner.finishCallback,
		joiner.closeCallback,
	)
}

func (joiner *Joiner) mainCallback1(inputNode string, dataset int, instance string, bulk int, data string) {
	joiner.calculator.AddBestUser(inputNode, dataset, instance, bulk, data)
}

func (joiner *Joiner) mainCallback2(inputNode string, dataset int, instance string, bulk int, data string) {
	joiner.calculator.AddUser(inputNode, dataset, instance, bulk, data)
}

func (joiner *Joiner) startCallback(dataset int) {
	// Clearing Calculator for next dataset.
	joiner.calculator.Clear(dataset)

	// Sending Start-Message to consumers.
	rabbit.OutputQueueStart(comms.StartMessageSigned(NODE_CODE, dataset, joiner.instance), joiner.outputQueue)
}

func (joiner *Joiner) finishCallback(dataset int) {
	// Retrieving join matches.
	joinMatches := joiner.calculator.RetrieveMatches(dataset)

	if len(joinMatches) == 0 {
    	log.Warnf("No join match to send.")
    }

    messageNumber := 0
    for _, joinedData := range joinMatches {
    	messageNumber++
    	joiner.sendJoinedData(dataset, messageNumber, joinedData)
	}

	// Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(NODE_CODE, dataset, joiner.instance), joiner.outputQueue)
}

func (joiner *Joiner) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(NODE_CODE, joiner.instance), joiner.outputQueue)
}

func (joiner *Joiner) sendJoinedData(dataset int, messageNumber int, joinedData comms.UserData) {
	bytes, err := json.Marshal(joinedData)
	if err != nil {
		log.Errorf("Error generating Json from joined best user #%d. Err: '%s'", messageNumber, err)
	} else {
		data := comms.SignMessage(NODE_CODE, dataset, joiner.instance, messageNumber, string(bytes))
		err := joiner.outputQueue.PublishData([]byte(data))

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

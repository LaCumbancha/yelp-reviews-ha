package core

import (
	"sync"
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
	DishashFilters		int
	BotUsersFilters 	int
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

	inputDirect1 := rabbit.NewRabbitInputDirect(channel, props.DishashFilterOutput, config.InputTopic, "")
	inputDirect2 := rabbit.NewRabbitInputDirect(channel, props.BotUsersFilterOutput, config.InputTopic, "")
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.BotUsersJoinerOutput, comms.EndSignals(1))

	joiner := &Joiner {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(),
		inputDirect1:		inputDirect1,
		inputDirect2:		inputDirect2,
		outputQueue:		outputQueue,
		endSignals1:		config.DishashFilters,
		endSignals2:		config.BotUsersFilters,
	}

	return joiner
}

func (joiner *Joiner) Run() {
	var procWg sync.WaitGroup
	var connWg sync.WaitGroup
	connWg.Add(1)

	closingConn := false
	connMutex := &sync.Mutex{}

	initialProcWait := 2
	procWg.Add(initialProcWait)

	innerChannel1 := make(chan amqp.Delivery)
	innerChannel2 := make(chan amqp.Delivery)

	log.Infof("Starting to listen for bot users with only one text.")
	go proc.InitializeProcessingWorkers(int(joiner.workersPool/2), innerChannel1, joiner.callback1, &procWg)
	go proc.ProcessInputs(joiner.inputDirect1.ConsumeData(), innerChannel1, joiner.endSignals1, &procWg, &connWg)

	log.Infof("Starting to listen for users reviews data.")
	go proc.InitializeProcessingWorkers(int(joiner.workersPool/2), innerChannel2, joiner.callback2, &procWg)
	go proc.ProcessInputs(joiner.inputDirect2.ConsumeData(), innerChannel2, joiner.endSignals2, &procWg, &connWg)

	// Retrieving joined data and closing connection.
	go proc.ProcessFinish(joiner.finishCallback, &procWg, initialProcWait, closingConn, connMutex)
	proc.CloseConnection(joiner.closeCallback, &procWg, &connWg, closingConn, connMutex)
}

func (joiner *Joiner) callback1(bulkNumber int, bulk string) {
	joiner.calculator.AddBotUser(bulkNumber, bulk)
}

func (joiner *Joiner) callback2(bulkNumber int, bulk string) {
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
		log.Errorf("Error generating Json from joined bot user #%d. Err: '%s'", messageNumber, err)
	} else {
		err := joiner.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending joined bot user #%d to output queue %s. Err: '%s'", messageNumber, joiner.outputQueue.Name, err)
		} else {
			log.Infof("Joined bot user #%d sent to output queue %s.", messageNumber, joiner.outputQueue.Name)
		}
	}
}

func (joiner *Joiner) Stop() {
	log.Infof("Closing Bot-Users Joiner connections.")
	joiner.connection.Close()
	joiner.channel.Close()
}

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
	StarsAggregators	int
	UserFilters 		int
}

type Joiner struct {
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
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.BestUsersJoinerOutput, comms.EndMessage(config.Instance), comms.EndSignals(1))

	joiner := &Joiner {
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
	var procWg sync.WaitGroup
	var connWg sync.WaitGroup
	connWg.Add(1)

	closingConn := false
	connMutex := &sync.Mutex{}

	log.Infof("Starting to listen for users 5-stars reviews data.")
	innerChannel1 := make(chan amqp.Delivery)
	procWg.Add(1)

	// Receiving and processing messages from the best-users flow.
	go proc.InitializeProcessingWorkers(int(joiner.workersPool/2), innerChannel1, joiner.callback1, &procWg)
	go proc.ProcessInputs(joiner.inputDirect1.ConsumeData(), innerChannel1, joiner.endSignals1, &procWg, &connWg)

	log.Infof("Starting to listen for users reviews data.")
	innerChannel2 := make(chan amqp.Delivery)
	procWg.Add(1)

	// Receiving and processing messages from the common-users flow.
	go proc.InitializeProcessingWorkers(int(joiner.workersPool/2), innerChannel2, joiner.callback2, &procWg)
	go proc.ProcessInputs(joiner.inputDirect2.ConsumeData(), innerChannel2, joiner.endSignals2, &procWg, &connWg)

	// Retrieving joined data and closing connection.
	go proc.ProcessFinish(joiner.finishCallback, &procWg, closingConn, connMutex)
	proc.CloseConnection(joiner.closeCallback, &procWg, &connWg, closingConn, connMutex)
}

func (joiner *Joiner) callback1(bulkNumber int, bulk string) {
	joiner.calculator.AddBestUser(bulkNumber, bulk)
}

func (joiner *Joiner) callback2(bulkNumber int, bulk string) {
	joiner.calculator.AddUser(bulkNumber, bulk)
}

func (joiner *Joiner) finishCallback() {
	joinMatches := joiner.calculator.RetrieveMatches()

	if len(joinMatches) == 0 {
    	log.Warnf("No join match to send.")
    }

    messageCounter := 0
    for _, joinedData := range joinMatches {
    	messageCounter++
    	joiner.sendJoinedData(messageCounter, joinedData)
	}

	joiner.outputQueue.PublishFinish()
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

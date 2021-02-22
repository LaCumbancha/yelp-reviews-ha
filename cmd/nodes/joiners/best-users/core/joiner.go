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
	endSignalsNeeded	map[string]int
}

func NewJoiner(config JoinerConfig) *Joiner {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect1 := rabbit.NewRabbitInputDirect(channel, props.FilterF4_Output2, config.InputTopic, rabbit.InnerQueueName(props.JoinerJ3_Input1, config.Instance))
	inputDirect2 := rabbit.NewRabbitInputDirect(channel, props.AggregatorA8_Output, config.InputTopic, rabbit.InnerQueueName(props.JoinerJ3_Input2, config.Instance))
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.JoinerJ3_Output, comms.EndSignals(1))

	endSignalsNeeded := map[string]int{
		props.FilterF4_Name: 		config.UserFilters, 
		props.AggregatorA8_Name: 	config.StarsAggregators,
	}

	joiner := &Joiner {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(),
		inputDirect1:		inputDirect1,
		inputDirect2:		inputDirect2,
		outputQueue:		outputQueue,
		endSignalsNeeded:	endSignalsNeeded,
	}

	return joiner
}

func (joiner *Joiner) Run() {
	log.Infof("Starting to listen for common users and best users (with just 5-stars reviews).")
	dataByInput := map[string]<-chan amqp.Delivery{
		props.FilterF4_Name: 		joiner.inputDirect1.ConsumeData(),
		props.AggregatorA8_Name: 	joiner.inputDirect2.ConsumeData(),
	}
	mainCallbackByInput := map[string]func(string, int, string, int, string){
		props.FilterF4_Name: 		joiner.mainCallback1, 
		props.AggregatorA8_Name: 	joiner.mainCallback2,
	}
	
	proc.ProcessInputs(
		dataByInput,
		joiner.workersPool,
		joiner.endSignalsNeeded,
		[]string{},
		mainCallbackByInput,
		joiner.startCallback,
		joiner.finishCallback,
		joiner.closeCallback,
	)
}

func (joiner *Joiner) mainCallback1(inputNode string, dataset int, instance string, bulk int, data string) {
	joiner.calculator.AddUser(dataset, bulk, data)
}

func (joiner *Joiner) mainCallback2(inputNode string, dataset int, instance string, bulk int, data string) {
	joiner.calculator.AddBestUser(dataset, bulk, data)
}

func (joiner *Joiner) startCallback(dataset int) {
	// Initializing new dataset in Calculator.
	joiner.calculator.RegisterDataset(dataset)

	// Sending Start-Message to consumers.
	rabbit.OutputQueueStart(comms.StartMessageSigned(props.JoinerJ3_Name, dataset, joiner.instance), joiner.outputQueue)
}

func (joiner *Joiner) finishCallback(dataset int) {
	// Retrieving join matches.
	joinMatches := joiner.calculator.RetrieveMatches(dataset)

	totalJoinMatches := len(joinMatches)
	if totalJoinMatches == 0 {
		log.Warnf("No join match to send.")
	}

    messageNumber := 0
    for _, joinedData := range joinMatches {
    	messageNumber++
    	joiner.sendJoinedData(dataset, messageNumber, joinedData)
	}

	// Removing processed dataset from Calculator.
	joiner.calculator.Clear(dataset)

	// Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(props.JoinerJ3_Name, dataset, joiner.instance), joiner.outputQueue)
}

func (joiner *Joiner) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(props.JoinerJ3_Name, joiner.instance), joiner.outputQueue)
}

func (joiner *Joiner) sendJoinedData(dataset int, messageNumber int, joinedData comms.UserData) {
	bytes, err := json.Marshal(joinedData)
	if err != nil {
		log.Errorf("Error generating Json from joined best user #%d. Err: '%s'", messageNumber, err)
	} else {
		data := comms.SignMessage(props.JoinerJ3_Name, dataset, joiner.instance, messageNumber, string(bytes))
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

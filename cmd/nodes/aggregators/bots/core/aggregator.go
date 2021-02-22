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

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	InputTopic			string
	HashMappers			int
	MinReviews			int
}

type Aggregator struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect 		*rabbit.RabbitInputDirect
	outputQueue 		*rabbit.RabbitOutputQueue
	endSignalsNeeded	map[string]int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.MapperM4_Output, config.InputTopic, rabbit.InnerQueueName(props.AggregatorA5_Input, config.Instance))
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.AggregatorA5_Output, comms.EndSignals(1))

	endSignalsNeeded := map[string]int{props.MapperM4_Name: config.HashMappers}

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.MinReviews),
		inputDirect:		inputDirect,
		outputQueue:		outputQueue,
		endSignalsNeeded:	endSignalsNeeded,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for distinct hashed-texts.")
	dataByInput := map[string]<-chan amqp.Delivery{props.MapperM4_Name: aggregator.inputDirect.ConsumeData()}
	mainCallbackByInput := map[string]func(string, int, string, int, string){props.MapperM4_Name: aggregator.mainCallback}

	proc.ProcessInputs(
		dataByInput,
		aggregator.workersPool,
		aggregator.endSignalsNeeded,
		[]string{},
		mainCallbackByInput,
		aggregator.startCallback,
		aggregator.finishCallback,
		aggregator.closeCallback,
	)
}

func (aggregator *Aggregator) mainCallback(inputNode string, dataset int, instance string, bulk int, data string) {
	aggregator.calculator.Save(dataset, bulk, data)
}

func (aggregator *Aggregator) startCallback(dataset int) {
	// Initializing new dataset in Calculator.
	aggregator.calculator.RegisterDataset(dataset)

	// Sending Start-Message to consumers.
	rabbit.OutputQueueStart(comms.StartMessageSigned(props.AggregatorA5_Name, dataset, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) finishCallback(dataset int) {
	// Computing aggregations.
	botNumber := 0
	for _, aggregatedData := range aggregator.calculator.AggregateData(dataset) {
		botNumber++
		aggregator.sendAggregatedData(dataset, botNumber, aggregatedData)
	}

	// Removing processed dataset from Calculator.
	aggregator.calculator.Clear(dataset)

	// Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(props.AggregatorA5_Name, dataset, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(props.AggregatorA5_Name, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) sendAggregatedData(dataset int, botNumber int, aggregatedBot comms.UserData) {
	bytes, err := json.Marshal(aggregatedBot)
	if err != nil {
		log.Errorf("Error generating Json from aggregated bot #%d. Err: '%s'", botNumber, err)
	} else {
		data := comms.SignMessage(props.AggregatorA5_Name, dataset, aggregator.instance, botNumber, string(bytes))
		err := aggregator.outputQueue.PublishData([]byte(data))

		if err != nil {
			log.Errorf("Error sending aggregated bot #%d to output queue %s. Err: '%s'", botNumber, aggregator.outputQueue.Name, err)
		} else {
			log.Infof("Aggregated bot #%d sent to output queue %s.", botNumber, aggregator.outputQueue.Name)
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Bots Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}

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

const NODE_CODE = "A2"

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	InputTopic			string
	FuncitJoiners		int
	TopSize				int
}

type Aggregator struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect 		*rabbit.RabbitInputDirect
	outputQueue 		*rabbit.RabbitOutputQueue
	endSignals			int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.JoinerJ1_Output, config.InputTopic, "")
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.AggregatorA2_Output, comms.EndSignals(1))

	aggregator := &Aggregator {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.TopSize),
		inputDirect:		inputDirect,
		outputQueue:		outputQueue,
		endSignals:			config.FuncitJoiners,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for funny-city data.")
	proc.Transformation(
		aggregator.workersPool,
		aggregator.endSignals,
		aggregator.inputDirect.ConsumeData(),
		aggregator.mainCallback,
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
	rabbit.OutputQueueStart(comms.StartMessageSigned(NODE_CODE, dataset, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) finishCallback(dataset int) {
	// Computing aggregations.
	cityNumber := 0
	for _, cityData := range aggregator.calculator.AggregateData(dataset) {
		cityNumber++
		aggregator.sendAggregatedData(dataset, cityNumber, cityData)
	}

	// Removing processed dataset from Calculator.
	aggregator.calculator.Clear(dataset)

	// Sending Finish-Message to consumers.
	rabbit.OutputQueueFinish(comms.FinishMessageSigned(NODE_CODE, dataset, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) closeCallback() {
	// Sending Close-Message to consumers.
	rabbit.OutputQueueClose(comms.CloseMessageSigned(NODE_CODE, aggregator.instance), aggregator.outputQueue)
}

func (aggregator *Aggregator) sendAggregatedData(dataset int, cityNumber int, topTenCity comms.FunnyCityData) {
	bytes, err := json.Marshal(topTenCity)
	if err != nil {
		log.Errorf("Error generating Json from funniest city #%d. Err: '%s'", cityNumber, err)
	} else {
		data := comms.SignMessage(NODE_CODE, dataset, aggregator.instance, cityNumber, string(bytes))
		err := aggregator.outputQueue.PublishData([]byte(data))

		if err != nil {
			log.Errorf("Error sending aggregated bulk #%d to output queue %s. Err: '%s'", cityNumber, aggregator.outputQueue.Name, err)
		} else {
			log.Infof("Aggregated bulk #%d sent to output queue %s.", cityNumber, aggregator.outputQueue.Name)
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Funny-City Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}

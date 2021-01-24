package core

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	utils "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
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
	FunbizAggregators 	int
	CitbizMappers		int
	FuncitAggregators	int
	OutputBulkSize		int
}

type Joiner struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect1 		*rabbit.RabbitInputDirect
	inputDirect2 		*rabbit.RabbitInputDirect
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals1			int
	endSignals2			int
}

func NewJoiner(config JoinerConfig) *Joiner {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect1 := rabbit.NewRabbitInputDirect(channel, props.FunbizAggregatorOutput, config.InputTopic, "")
	inputDirect2 := rabbit.NewRabbitInputDirect(channel, props.CitbizMapperOutput, config.InputTopic, "")
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.FuncitJoinerOutput, comms.EndMessage(config.Instance))

	joiner := &Joiner {
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect1:		inputDirect1,
		inputDirect2:		inputDirect2,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.FuncitAggregators, PartitionableValues),
		endSignals1:		config.FunbizAggregators,
		endSignals2:		config.CitbizMappers,
	}

	return joiner
}

func (joiner *Joiner) Run() {
	var procWg sync.WaitGroup
	var connWg sync.WaitGroup
	connWg.Add(1)

	closingConn := false
	connMutex := &sync.Mutex{}

	log.Infof("Starting to listen for funny-business data.")
	innerChannel1 := make(chan amqp.Delivery)
	procWg.Add(1)

	// Receiving and processing messages from the best-users flow.
	go proc.InitializeProcessingWorkers(int(joiner.workersPool/2), innerChannel1, joiner.callback1, &procWg)
	go proc.ProcessInputs(joiner.inputDirect1.ConsumeData(), innerChannel1, joiner.endSignals1, &procWg, &connWg)

	log.Infof("Starting to listen for city-business data.")
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
	joiner.calculator.AddFunnyBusiness(bulkNumber, bulk)
}

func (joiner *Joiner) callback2(bulkNumber int, bulk string) {
	joiner.calculator.AddCityBusiness(bulkNumber, bulk)
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

	for _, partition := range utils.GetMapDistinctValues(joiner.outputPartitions) {
		joiner.outputDirect.PublishFinish(partition)
	}
}

func (joiner *Joiner) closeCallback() {
	// TODO
}

func (joiner *Joiner) sendJoinedData(bulkNumber int, joinedBulk []comms.FunnyCityData) {
	dataListByPartition := make(map[string][]comms.FunnyCityData)

	for _, data := range joinedBulk {
		partition := joiner.outputPartitions[string(data.City[0])]

		if partition != "" {
			funcitDataListPartitioned := dataListByPartition[partition]

			if funcitDataListPartitioned != nil {
				dataListByPartition[partition] = append(funcitDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.FunnyCityData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for city '%s'.", data.City)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		outputData, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {

			err := joiner.outputDirect.PublishData(outputData, partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, joiner.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, joiner.outputDirect.Exchange, partition), bulkNumber)
			}	
		}
	}
}

func (joiner *Joiner) Stop() {
	log.Infof("Closing Funny-City Joiner connections.")
	joiner.connection.Close()
	joiner.channel.Close()
}

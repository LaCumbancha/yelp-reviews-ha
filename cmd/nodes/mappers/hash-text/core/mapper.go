package core

import (
	"fmt"
	"sync"
	"strings"
	"crypto/md5"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	utils "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type MapperConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	ReviewsInputs		int
	HashAggregators		int
}

type Mapper struct {
	instance 			string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	inputDirect 		*rabbit.RabbitInputDirect
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals 			int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.ReviewsScatterOutput, props.HashMapperTopic, props.HashMapperInput)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.HashMapperOutput)

	mapper := &Mapper {
		instance:			config.Instance,
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		inputDirect:		inputDirect,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.HashAggregators, PartitionableValues),
		endSignals:			config.ReviewsInputs,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for reviews.")
	innerChannel := make(chan amqp.Delivery)

	closingConn := false
	connMutex := &sync.Mutex{}

	var connWg sync.WaitGroup
	connWg.Add(1)

	var procWg sync.WaitGroup
	procWg.Add(1)

	go proc.InitializeProcessingWorkers(mapper.workersPool, innerChannel, mapper.callback, &procWg)
	go proc.ProcessInputs(mapper.inputDirect.ConsumeData(), innerChannel, mapper.endSignals, &procWg, &connWg)
	go proc.ProcessFinish(mapper.finishCallback, &procWg, closingConn, connMutex)
	proc.CloseConnection(mapper.closeCallback, &procWg, &connWg, closingConn, connMutex)
}

func (mapper *Mapper) callback(bulkNumber int, bulk string) {
	mappedData := mapper.mapData(bulkNumber, bulk)
	mapper.sendMappedData(bulkNumber, mappedData)
}

func (mapper *Mapper) finishCallback() {
	rabbit.OutputDirectFinish(comms.EndMessage(mapper.instance), mapper.outputPartitions, mapper.outputDirect)
}

func (mapper *Mapper) closeCallback() {
	// TODO
}

func (mapper *Mapper) mapData(bulkNumber int, rawReviewsBulk string) []comms.HashedTextData {
	var review comms.FullReview
	var hashTextDataList []comms.HashedTextData

	rawReviews := strings.Split(rawReviewsBulk, "\n")
	for _, rawReview := range rawReviews {
		json.Unmarshal([]byte(rawReview), &review)

		if (review.UserId != "") {
			hasher := md5.New()
			hasher.Write([]byte(review.Text))
			hashedText := fmt.Sprintf("%x", hasher.Sum(nil))
	
			mappedReview := comms.HashedTextData {
				UserId:			review.UserId,
				HashedText:		hashedText,
			}

			hashTextDataList = append(hashTextDataList, mappedReview)			
		} else {
			log.Warnf("Empty UserID detected in raw review %s.", rawReview)
		}
	}

	return hashTextDataList
}

func (mapper *Mapper) sendMappedData(bulkNumber int, mappedBulk []comms.HashedTextData) {
	dataListByPartition := make(map[string][]comms.HashedTextData)

	for _, data := range mappedBulk {
		partition := mapper.outputPartitions[string(data.UserId[0])]

		if partition != "" {
			hashedDataList := dataListByPartition[partition]

			if hashedDataList != nil {
				dataListByPartition[partition] = append(hashedDataList, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.HashedTextData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for user (%s).", data.UserId)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		outputData, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {

			err := mapper.outputDirect.PublishData(outputData, partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, mapper.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, mapper.outputDirect.Exchange, partition), bulkNumber)
			}	
		}
	}
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing User Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}

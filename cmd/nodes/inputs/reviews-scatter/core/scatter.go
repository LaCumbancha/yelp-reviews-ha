package core

import (
	"os"
	"fmt"
	"time"
	"bufio"
	"bytes"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type ScatterConfig struct {
	Instance			string
	Data				string
	RabbitIp			string
	RabbitPort			string
	BulkSize			int
	FunbizMappers		int
	WeekdaysMappers		int
	HashesMappers		int
	UsersMappers		int
	StarsMappers		int
}

type Scatter struct {
	instance 			string
	data 				string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	bulkSize			int
	outputDirect 		*rabbit.RabbitOutputDirect
	outputSignals		map[string]int
}

func NewScatter(config ScatterConfig) *Scatter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.ReviewsScatterOutput)
	
	scatter := &Scatter {
		instance:			config.Instance,
		data: 				config.Data,
		connection:			connection,
		channel:			channel,
		bulkSize:			config.BulkSize,
		outputDirect:		outputDirect,
		outputSignals:		GenerateSignalsMap(config.FunbizMappers, config.WeekdaysMappers, config.HashesMappers, config.UsersMappers, config.StarsMappers),
	}

	return scatter
}

func (scatter *Scatter) Run() {
	start := time.Now()

	log.Infof("Starting to load reviews.")

    file, err := os.Open(scatter.data)
    if err != nil {
        log.Fatalf("Error opening reviews data file. Err: '%s'", err)
    }
    defer file.Close()

    bulkNumber := 0
    chunkNumber := 0
    scanner := bufio.NewScanner(file)
    buffer := bytes.NewBufferString("")
    for scanner.Scan() {
        buffer.WriteString(scanner.Text())
        buffer.WriteString("\n")

        chunkNumber++
        if chunkNumber == scatter.bulkSize {
            bulkNumber++
            bulk := buffer.String()
            scatter.sendBulk(bulkNumber, bulk[:len(bulk)-1])

            buffer = bytes.NewBufferString("")
            chunkNumber = 0
        }
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading reviews data from file %s. Err: '%s'", scatter.data, err)
    }

    bulkNumber++
    bulk := buffer.String()
    if bulk != "" {
    	scatter.sendBulk(bulkNumber, bulk[:len(bulk)-1])
    }

    // Publishing end messages.
    for _, partition := range PartitionableValues {
    	errors := false
    	for idx := 0 ; idx < scatter.outputSignals[partition]; idx++ {
    		err := scatter.outputDirect.PublishData(comms.EndMessage(scatter.instance), partition)

    		if err != nil {
				errors = true
				log.Errorf("Error sending End-Message to direct-exchange %s (partition %s). Err: '%s'", scatter.outputDirect.Exchange, partition, err)
			}
    	}

    	if !errors {
			log.Infof("End-Message sent to direct-exchange %s (partition %s).", scatter.outputDirect.Exchange, partition)
		}
    }

    log.Infof("Time: %s.", time.Now().Sub(start).String())







    time.Sleep(180000 * time.Millisecond)
    log.Infof("Starting to load reviews from second file.")

    file, err = os.Open("reviews2.json")
    if err != nil {
        log.Fatalf("Error opening reviews data file. Err: '%s'", err)
    }
    defer file.Close()

    bulkNumber = 0
    chunkNumber = 0
    scanner = bufio.NewScanner(file)
    buffer = bytes.NewBufferString("")
    for scanner.Scan() {
        buffer.WriteString(scanner.Text())
        buffer.WriteString("\n")

        chunkNumber++
        if chunkNumber == scatter.bulkSize {
            bulkNumber++
            bulk := buffer.String()
            scatter.sendBulk(bulkNumber, bulk[:len(bulk)-1])

            buffer = bytes.NewBufferString("")
            chunkNumber = 0
        }
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading reviews data from file %s. Err: '%s'", scatter.data, err)
    }

    bulkNumber++
    bulk := buffer.String()
    if bulk != "" {
    	scatter.sendBulk(bulkNumber, bulk[:len(bulk)-1])
    }

    // Publishing end messages.
    for _, partition = range PartitionableValues {
    	for idx := 0 ; idx < scatter.outputSignals[partition]; idx++ {
    		scatter.outputDirect.PublishFinish(partition)
    	}
    }

    log.Infof("Time: %s.", time.Now().Sub(start).String())

    time.Sleep(180000 * time.Millisecond)
    log.Infof("Starting to load reviews from third file.")

    file, err := os.Open("reviews3.json")
    if err != nil {
        log.Fatalf("Error opening reviews data file. Err: '%s'", err)
    }
    defer file.Close()

    bulkNumber = 0
    chunkNumber = 0
    scanner = bufio.NewScanner(file)
    buffer = bytes.NewBufferString("")
    for scanner.Scan() {
        buffer.WriteString(scanner.Text())
        buffer.WriteString("\n")

        chunkNumber++
        if chunkNumber == scatter.bulkSize {
            bulkNumber++
            bulk := buffer.String()
            scatter.sendBulk(bulkNumber, bulk[:len(bulk)-1])

            buffer = bytes.NewBufferString("")
            chunkNumber = 0
        }
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading reviews data from file %s. Err: '%s'", scatter.data, err)
    }

    bulkNumber++
    bulk := buffer.String()
    if bulk != "" {
    	scatter.sendBulk(bulkNumber, bulk[:len(bulk)-1])
    }

    // Publishing end messages.
    for _, partition = range PartitionableValues {
    	for idx := 0 ; idx < scatter.outputSignals[partition]; idx++ {
    		scatter.outputDirect.PublishFinish(partition)
    	}
    }

    log.Infof("Time: %s.", time.Now().Sub(start).String())
}

func (scatter *Scatter) sendBulk(bulkNumber int, bulk string) {
	for _, partition := range PartitionableValues {
		err := scatter.outputDirect.PublishData([]byte(bulk), partition)

		if err != nil {
			log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, scatter.outputDirect.Exchange, partition, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, scatter.outputDirect.Exchange, partition), bulkNumber)
		}
	}
}

func (scatter *Scatter) Stop() {
	log.Infof("Closing Reviews-Scatter connections.")
	scatter.connection.Close()
	scatter.channel.Close()
}

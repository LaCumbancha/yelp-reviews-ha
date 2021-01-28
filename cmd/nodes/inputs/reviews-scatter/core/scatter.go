package core

import (
    "os"
    "fmt"
    "time"
    "bufio"
    "bytes"
    "github.com/streadway/amqp"
    "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"

    log "github.com/sirupsen/logrus"
    logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
    props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
    comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
    rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

const NODE_CODE = "I2"

type ScatterConfig struct {
    Instance            string
    RabbitIp            string
    RabbitPort          string
    BulkSize            int
    FunbizMappers       int
    WeekdaysMappers     int
    HashesMappers       int
    UsersMappers        int
    StarsMappers        int
}

type Scatter struct {
    instance            string
    data                string
    connection          *amqp.Connection
    channel             *amqp.Channel
    bulkSize            int
    outputDirect        *rabbit.RabbitOutputDirect
    outputSignals       map[string]int
}

func NewScatter(config ScatterConfig) *Scatter {
    connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

    outputDirect := rabbit.NewRabbitOutputDirect(channel, props.ReviewsScatterOutput)
    
    scatter := &Scatter {
        instance:           config.Instance,
        connection:         connection,
        channel:            channel,
        bulkSize:           config.BulkSize,
        outputDirect:       outputDirect,
        outputSignals:      GenerateSignalsMap(config.FunbizMappers, config.WeekdaysMappers, config.HashesMappers, config.UsersMappers, config.StarsMappers),
    }

    return scatter
}

func (scatter *Scatter) Run() {
    reader := bufio.NewReader(os.Stdin)
    datasetNumber := 1
    exitFlag := false

    for !exitFlag {
        scatter.printMenu()
        fmt.Print("Option: ")
        option := utils.ReadInput(reader)

        for {
            if option == "S" {
                fmt.Print("Dataset path: ")
                datasetPath := utils.ReadInput(reader)

                if _, err := os.Stat(datasetPath); os.IsNotExist(err) {
                    fmt.Printf("Dataset %s doesn't exist!\n", datasetPath)
                } else {
                    scatter.processFile(datasetPath, datasetNumber)
                    datasetNumber++
                }
                
                break
            } else if option == "X" {
                fmt.Println()
                scatter.closeConnection()
                exitFlag = true
                break
            } else {
                fmt.Print("Wrong option. Retry: ")
                option = utils.ReadInput(reader)
            }
        }
    }
}

func (scatter *Scatter) printMenu() {
    fmt.Println()
    fmt.Println("Reviews Scatter")
    fmt.Println("---------------")
    fmt.Println("[S] SEND DATASET")
    fmt.Println("[X] CLOSE")
}

func (scatter *Scatter) processFile(filePath string, dataset int) {
    start := time.Now()
    fmt.Println()

    log.Infof("Starting to load reviews from dataset #%d.", dataset)
    file, err := os.Open(filePath)
    if err != nil {
        log.Fatalf("Error opening file %s. Err: '%s'", filePath, err)
    }
    defer file.Close()

    scatter.startDataset(dataset)

    bulk := 0
    chunk := 0
    scanner := bufio.NewScanner(file)
    buffer := bytes.NewBufferString("")
    for scanner.Scan() {
        buffer.WriteString(scanner.Text())
        buffer.WriteString("\n")

        chunk++
        if chunk == scatter.bulkSize {
            bulk++
            bulkData := buffer.String()
            scatter.sendBulk(dataset, bulk, bulkData[:len(bulkData)-1])

            buffer = bytes.NewBufferString("")
            chunk = 0
        }
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading reviews data from file %s. Err: '%s'", scatter.data, err)
    }

    bulk++
    bulkData := buffer.String()
    if bulkData != "" {
        scatter.sendBulk(dataset, bulk, bulkData[:len(bulkData)-1])
    }

    scatter.finishDataset(dataset)

    log.Infof("Total time: %s.", time.Now().Sub(start).String())
}

func (scatter *Scatter) sendBulk(dataset int, bulk int, bulkData string) {
    errors := false
    for _, partition := range PartitionableValues {
        err := scatter.outputDirect.PublishData([]byte(comms.SignMessage(NODE_CODE, dataset, scatter.instance, bulk, bulkData)), partition)

        if err != nil {
            errors = true
            log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulk, scatter.outputDirect.Exchange, partition, err)
        }
    }

    if !errors {
        logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (all partitions).", bulk, scatter.outputDirect.Exchange), bulk)
    }
}

func (scatter *Scatter) startDataset(dataset int) {
    // Sending Start-Message to consumers.
    scatter.sendSpecialMessage("Start-Message", comms.StartMessageSigned(NODE_CODE, dataset, scatter.instance))
}

func (scatter *Scatter) finishDataset(dataset int) {
    // Sending Finish-Message to consumers.
    scatter.sendSpecialMessage("Finish-Message", comms.FinishMessageSigned(NODE_CODE, dataset, scatter.instance))
}

func (scatter *Scatter) closeConnection() {
    // Sending Close-Message to consumers.
    scatter.sendSpecialMessage("Close-Message", comms.CloseMessageSigned(NODE_CODE, scatter.instance))
}

func (scatter *Scatter) sendSpecialMessage(messageType string, message []byte) {
    errors := false
    for _, partition := range PartitionableValues {
        for idx := 0 ; idx < scatter.outputSignals[partition]; idx++ {
            err := scatter.outputDirect.PublishData(message, partition)

            if err != nil {
                errors = true
                log.Errorf("Error sending %s to direct-exchange %s (partition %s). Err: '%s'", messageType, scatter.outputDirect.Exchange, partition, err)
            }
        }
    }

    if !errors {
        log.Infof("%s sent to direct-exchange %s (all partitions).", messageType, scatter.outputDirect.Exchange)
    }
}

func (scatter *Scatter) Stop() {
    log.Infof("Closing Reviews-Scatter connections.")
    scatter.connection.Close()
    scatter.channel.Close()
}

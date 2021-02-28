package core

import (
    "os"
    "fmt"
    "time"
    "bufio"
    "bytes"
    "strings"
    "strconv"
    "github.com/streadway/amqp"
    "github.com/LaCumbancha/yelp-review-ha/cmd/common/utils"

    log "github.com/sirupsen/logrus"
    quit "github.com/LaCumbancha/yelp-review-ha/cmd/common/quit"
    logb "github.com/LaCumbancha/yelp-review-ha/cmd/common/logger"
    props "github.com/LaCumbancha/yelp-review-ha/cmd/common/properties"
    comms "github.com/LaCumbancha/yelp-review-ha/cmd/common/communication"
    rabbit "github.com/LaCumbancha/yelp-review-ha/cmd/common/middleware"
)

type ScatterConfig struct {
    RabbitIp            string
    RabbitPort          string
    BulkSize            int
    FunbizMappers       int
    WeekdaysMappers     int
    HashesMappers       int
    UsersMappers        int
    StarsMappers        int
    Monitors            string
}

type Scatter struct {
    instance            string
    data                string
    connection          *amqp.Connection
    channel             *amqp.Channel
    bulkSize            int
    outputFanout        *rabbit.RabbitOutputFanout
    outputSignals       int
    processedDatasets   []int
    datasetNumber       int
    monitors            []string
}

func NewScatter(config ScatterConfig) *Scatter {
    connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

    outputFanout := rabbit.NewRabbitOutputFanout(channel, props.InputI2_Output)
    
    scatter := &Scatter {
        instance:           "0",
        connection:         connection,
        channel:            channel,
        bulkSize:           config.BulkSize,
        outputFanout:       outputFanout,
        outputSignals:      utils.MaxInt(config.FunbizMappers, config.WeekdaysMappers, config.HashesMappers, config.UsersMappers, config.StarsMappers),
        processedDatasets:  make([]int, 0),
        datasetNumber:      1,
        monitors:           strings.Split(config.Monitors, ","),
    }

    return scatter
}

func (scatter *Scatter) Run() {
    reader := bufio.NewReader(os.Stdin)
    exitFlag := false

    for !exitFlag {
        scatter.printMenu()
        fmt.Print("Option: ")
        option := strings.ToUpper(utils.ReadInput(reader))

        for {
            if option == "S" {
                fmt.Print("Dataset path: ")
                datasetPath := utils.ReadInput(reader)

                if _, err := os.Stat(datasetPath); os.IsNotExist(err) {
                    fmt.Printf("Dataset %s doesn't exist!\n", datasetPath)
                } else {
                    scatter.insertDatasetNumber(reader)
                    scatter.processFile(datasetPath, scatter.datasetNumber)
                    scatter.processedDatasets = append(scatter.processedDatasets, scatter.datasetNumber)
                    scatter.datasetNumber++
                }
                
                break
            } else if option == "X" {
                fmt.Println()
                exitFlag = scatter.closeConnection()
                break
            } else {
                fmt.Print("Wrong option. Retry: ")
                option = utils.ReadInput(reader)
            }
        }
    }
}

func (scatter *Scatter) insertDatasetNumber(reader *bufio.Reader) {
    datasetOk := false
    fmt.Printf("Dataset number [default is %d]: ", scatter.datasetNumber)
    inputDataset := utils.ReadInput(reader)
    for !datasetOk {
        if inputDataset == "" {
            datasetOk = true
        } else {
            newDatasetNumber, err := strconv.Atoi(inputDataset)
            if err == nil {
                if utils.IntInSlice(newDatasetNumber, scatter.processedDatasets) {
                    fmt.Printf("Dataset #%d already processed. Retry: ", newDatasetNumber)
                    inputDataset = utils.ReadInput(reader)
                } else {
                    scatter.datasetNumber = newDatasetNumber
                    datasetOk = true
                }
            } else {
                fmt.Print("Wrong value. Must be an integer: ")
                inputDataset = utils.ReadInput(reader)
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
    err := scatter.outputFanout.PublishData([]byte(comms.SignMessage(props.InputI2_Name, dataset, scatter.instance, bulk, bulkData)))

    if err != nil {
        log.Errorf("Error sending bulk #%d to fanout-exchange %s. Err: '%s'", bulk, scatter.outputFanout.Exchange, err)
    } else {
        logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to fanout-exchange %s.", bulk, scatter.outputFanout.Exchange), bulk)
    }
}

func (scatter *Scatter) startDataset(dataset int) {
    // Sending Start-Message to consumers.
    rabbit.OutputFanoutStart(comms.StartMessageSigned(props.InputI2_Name, dataset, scatter.instance), scatter.outputSignals, scatter.outputFanout)
}

func (scatter *Scatter) finishDataset(dataset int) {
    // Sending Finish-Message to consumers.
    rabbit.OutputFanoutFinish(comms.FinishMessageSigned(props.InputI2_Name, dataset, scatter.instance), scatter.outputSignals, scatter.outputFanout)
}

func (scatter *Scatter) closeConnection() bool {
    log.Errorf("Starting closing process.")

    // Stopping every monitor.
    allStopped := true
    for _, monitor := range scatter.monitors {
        monitorIp := utils.MonitorIP(monitor)
        if !quit.StopRequest(monitorIp) {
            allStopped = false
        }
    }

    if !allStopped {
        log.Errorf("Couldn't stop every monitor. Aborting closing process.")
        return false
    }

    // Shutdowning every monitor.
    for _, monitor := range scatter.monitors {
        monitorIp := utils.MonitorIP(monitor)
        quit.ShutdownRequest(monitorIp)
    }

    // Sending Close-Message to consumers.
    rabbit.OutputFanoutClose(comms.CloseMessageSigned(props.InputI2_Name, scatter.instance), scatter.outputSignals, scatter.outputFanout)
    return true
}

func (scatter *Scatter) Stop() {
    log.Infof("Closing Reviews-Scatter connections.")
    scatter.connection.Close()
    scatter.channel.Close()
}

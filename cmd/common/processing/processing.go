package processing

import (
	"fmt"
	"sync"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/common/utils"

	log "github.com/sirupsen/logrus"
	bkp "github.com/LaCumbancha/reviews-analysis/cmd/common/backup"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

const DefaultDataset = 0
const DefaultPartition = "0"

func ReceiveInputMessages(
	messagesByInput map[string]<-chan amqp.Delivery,
	dataChannelByInput map[string]chan amqp.Delivery,
	startSignalsMap map[int]map[string]map[string]int,
	finishSignalsMap map[int]map[string]map[string]int,
	closeSignalsMap map[string]map[string]int,
	signalsNeeded  map[string]int,
	savedInputs []string,
	startCallback func(int),
	finishCallback func(int),
	closeCallback func(),
	connWg *sync.WaitGroup,
	finishWg *sync.WaitGroup,
	procWgsByDataset map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
) {
	startSignalsMutex := &sync.Mutex{}
	finishSignalsMutex := &sync.Mutex{}
	closeSignalsMutex := &sync.Mutex{}

	for inputCode, inputChannel := range messagesByInput {
		innerChannel, found := dataChannelByInput[inputCode]

		if !found {
			log.Errorf("There was no channel initialized for input '%s'.", inputCode)
		} else {
			go func(inputChannel <- chan amqp.Delivery, innerChannel chan amqp.Delivery) {
				for message := range inputChannel {
					messageBody := string(message.Body)
					inputNode, dataset, instance, bulk, mainMessage := comms.UnsignMessage(messageBody)

					if comms.IsStartMessage(mainMessage) {
						processStartSignal(message, dataset, inputNode, instance, startSignalsMap, startSignalsMutex, signalsNeeded, savedInputs, procWgsByDataset, procWgsMutex, startCallback)
					} else if comms.IsFinishMessage(mainMessage) {
						processFinishSignal(message, dataset, inputNode, instance, finishSignalsMap, finishSignalsMutex, signalsNeeded, savedInputs, finishWg, procWgsByDataset, procWgsMutex, finishCallback)
					} else if comms.IsCloseMessage(mainMessage) {
						processCloseSignal(message, inputNode, instance, closeSignalsMap, closeSignalsMutex, signalsNeeded, connWg, finishWg, procWgsByDataset, procWgsMutex, closeCallback)
					} else {
						log.Debugf("Message from dataset %d received. ProcWgs: %v.", dataset, procWgsByDataset)
						log.Tracef("Message: '%s'.", string(message.Body))

						if datasetWg := utils.WaitGroupByDataset(dataset, procWgsByDataset, procWgsMutex); datasetWg != nil {
							datasetWg.Add(1)
							innerChannel <- message
						} else {
							log.Errorf("Message #%s.%d.%s.%d received but the dataset's WaitGroup wasn't initialized. Dropping message.", inputNode, dataset, instance, bulk)
							rabbit.AckMessage(message)
						}
					}
				}
			}(inputChannel, innerChannel)
		}
	}
}

func processStartSignal(
	message amqp.Delivery,
	dataset int,
	inputNode string,
	instance string,
	signalsMap map[int]map[string]map[string]int,
	signalsMutex *sync.Mutex,
	signalsNeeded  map[string]int,
	savedInputs []string,
	procWgsByDataset map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
	callback func(int),
) {
	signalsMutex.Lock()
	firstReceivedByInput, _, _, everyInputAllSignals := comms.MultiDatasetSignalsControl(dataset, inputNode, instance, signalsMap, signalsNeeded, savedInputs)
	signalsMutex.Unlock()

	if firstReceivedByInput {
		log.Infof("First Start-Message from dataset #%d from the %s input received.", dataset, inputNode)
		utils.DefineWaitGroupByDataset(dataset, procWgsByDataset, procWgsMutex).Add(1)
	}

	if everyInputAllSignals {
		callback(dataset)
	}

	signalsMutex.Lock()
	bkp.StoreSignalsBackup(signalsMap, bkp.StartBkp)
	signalsMutex.Unlock()

	rabbit.AckMessage(message)
}

func processFinishSignal(
	message amqp.Delivery,
	dataset int,
	inputNode string,
	instance string,
	signalsMap map[int]map[string]map[string]int,
	signalsMutex *sync.Mutex,
	signalsNeeded  map[string]int,
	savedInputs []string,
	finishWg *sync.WaitGroup,
	procWgsByDataset map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
	callback func(int),
) {
	counter := 0
	signalsMutex.Lock()
	counter++
	_, _, allReceivedByInput, everyInputAllSignals := comms.MultiDatasetSignalsControl(dataset, inputNode, instance, signalsMap, signalsNeeded, savedInputs)
	signalsMutex.Unlock()

	if allReceivedByInput {
		log.Infof("All Finish-Messages from dataset #%d from the %s input received.", dataset, inputNode)
		utils.WaitGroupByDataset(dataset, procWgsByDataset, procWgsMutex).Done()
	}

	if everyInputAllSignals {
		log.Infof("Every Finish-Message needed were received.")
		finishWg.Add(1)
		utils.WaitGroupByDataset(dataset, procWgsByDataset, procWgsMutex).Wait()
		utils.DeleteWaitGroupByDataset(dataset, procWgsByDataset, procWgsMutex)
		callback(dataset)
		finishWg.Done()
	}

	signalsMutex.Lock()
	bkp.StoreSignalsBackup(signalsMap, bkp.FinishBkp)
	signalsMutex.Unlock()

	rabbit.AckMessage(message)
}

func processCloseSignal(
	message amqp.Delivery,
	inputNode string,
	instance string,
	signalsMap map[string]map[string]int,
	signalsMutex *sync.Mutex,
	signalsNeeded  map[string]int,
	connWg *sync.WaitGroup,
	finishWg *sync.WaitGroup,
	procWgsByDataset map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
	callback func(),
) {
	signalsMutex.Lock()
	_, newReceivedByInput, allReceivedByInput, everyInputAllSignals := comms.SingleDatasetSignalsControl(inputNode, instance, signalsMap, signalsNeeded)
	signalsMutex.Unlock()

	if newReceivedByInput {
		log.Infof("Close-Message #%d from the %s input received.", len(signalsMap), inputNode)
	}

	if allReceivedByInput {
		log.Infof("All Close-Messages from the %s input received.", inputNode)
	}

	if everyInputAllSignals {
		log.Infof("Every Close-Message needed were received.")
		for _, datasetWg := range utils.AllDatasetsWaitGroups(procWgsByDataset, procWgsMutex) {
			datasetWg.Wait()
		}
		finishWg.Wait()
		callback()
		connWg.Done()
	}

	signalsMutex.Lock()
	bkp.StoreSignalsBackup(signalsMap, bkp.CloseBkp)
	signalsMutex.Unlock()

	rabbit.AckMessage(message)
}

func ProcessData(
	workersPool int,
	mainChannel chan amqp.Delivery,
	callback func(string, int, string, int, string),
	receivedMsgs map[string]bool,
	receivedMutex *sync.Mutex,
	procWgsByDataset map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
) {
	log.Tracef("Initializing %d workers.", workersPool)
	for worker := 1 ; worker <= workersPool ; worker++ {
		log.Tracef("Initializing worker #%d.", worker)
		
		go func() {
			for message := range mainChannel {
				messageBody := string(message.Body)
				inputNode, dataset, instance, bulk, data := comms.UnsignMessage(messageBody)

				if data != "" {
					messageId := MessageSavingId(inputNode, dataset, bulk)
					logb.Instance().Infof(fmt.Sprintf("Message #%s.%s.%d.%d received.", inputNode, instance, dataset, bulk), bulk)

					receivedMutex.Lock()
					if _, found := receivedMsgs[messageId]; found {
						receivedMutex.Unlock()
						log.Warnf("Message #%s was already received and processed.", messageId)
					} else {
						receivedMsgs[messageId] = true
						receivedMutex.Unlock()
						callback(inputNode, dataset, instance, bulk, data)

						receivedMutex.Lock()
						bkp.StoreSignalsBackup(receivedMsgs, bkp.ReceivedBkp)
						receivedMutex.Unlock()
					}

					rabbit.AckMessage(message)
    				utils.WaitGroupByDataset(dataset, procWgsByDataset, procWgsMutex).Done()
				} else {
					log.Warnf("Unexpected message received: '%s'", messageBody)
				}
				
			}
		}()
	}
}

func InitializeProcessWaitGroups(
	startSignalsReceivedByDataset map[int]map[string]map[string]int,
	finishSignalsReceivedByDataset map[int]map[string]map[string]int,
	signalsNeededByInput  map[string]int,
	procWgsByDataset map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
) {
	for dataset, startSignalsReceivedByInput := range startSignalsReceivedByDataset {
		runningInputs := len(startSignalsReceivedByInput)
		utils.DefineWaitGroupByDataset(dataset, procWgsByDataset, procWgsMutex).Add(runningInputs)
	}

	for dataset, finishSignalsReceivedByInput := range finishSignalsReceivedByDataset {
		allFinished := true
		for input, finishSignalsReceivedByInstance := range finishSignalsReceivedByInput {
			if inputSignalsNeeded, found := signalsNeededByInput[input]; found {
				if len(finishSignalsReceivedByInstance) == inputSignalsNeeded {
					utils.WaitGroupByDataset(dataset, procWgsByDataset, procWgsMutex).Done()
				} else {
					allFinished = false
				}
			} else {
				log.Warnf("Finish signals from input %s restored from backup but there wasn't defined a full condition.", input)
				allFinished = false
			}
		}

		startSignalsReceivedByInput, found := startSignalsReceivedByDataset[dataset]
		if found && allFinished && len(startSignalsReceivedByInput) == len(finishSignalsReceivedByInput) {
			utils.DeleteWaitGroupByDataset(dataset, procWgsByDataset, procWgsMutex)
		}
	}
}

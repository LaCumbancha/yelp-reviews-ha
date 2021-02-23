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
	startSignalsMap map[int]map[string]map[string]bool,
	finishSignalsMap map[int]map[string]map[string]bool,
	closeSignalsMap map[string]map[string]bool,
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
	signalsMap map[int]map[string]map[string]bool,
	signalsMutex *sync.Mutex,
	signalsNeeded  map[string]int,
	savedInputs []string,
	procWgsByDataset map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
	callback func(int),
) {
	signalsMutex.Lock()
	firstInstanceSignaledByInput, newInstanceSignaledByInput, _, everyInputAllInstancesSignaled := 
		comms.MultiDatasetSignalsControl(dataset, inputNode, instance, signalsMap, signalsNeeded, savedInputs)
	signalsMutex.Unlock()

	if firstInstanceSignaledByInput {
		log.Infof("First Start-Message from dataset #%d from the %s input received.", dataset, inputNode)
		utils.DefineWaitGroupByDataset(dataset, procWgsByDataset, procWgsMutex).Add(1)
	}

	if everyInputAllInstancesSignaled {
		callback(dataset)
	}

	signalsMutex.Lock()
	bkp.StoreSignalsBackup(signalsMap, bkp.StartBkp)
	signalsMutex.Unlock()

	if newInstanceSignaledByInput {
		rabbit.AckMessage(message)
	} else {
		rabbit.NackMessage(message)
		log.Debugf("Repeated Start-Message from the %s input (instance #%s) received.", inputNode, instance)
	}
}

func processFinishSignal(
	message amqp.Delivery,
	dataset int,
	inputNode string,
	instance string,
	signalsMap map[int]map[string]map[string]bool,
	signalsMutex *sync.Mutex,
	signalsNeeded  map[string]int,
	savedInputs []string,
	finishWg *sync.WaitGroup,
	procWgsByDataset map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
	callback func(int),
) {
	signalsMutex.Lock()
	_, newInstanceSignaledByInput, allInstancesSignaledByInput, everyInputAllInstancesSignaled := 
		comms.MultiDatasetSignalsControl(dataset, inputNode, instance, signalsMap, signalsNeeded, savedInputs)
	signalsMutex.Unlock()

	if allInstancesSignaledByInput {
		log.Infof("All Finish-Messages from dataset #%d from the %s input received.", dataset, inputNode)
		utils.WaitGroupByDataset(dataset, procWgsByDataset, procWgsMutex).Done()
	}

	if everyInputAllInstancesSignaled {
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

	if newInstanceSignaledByInput {
		rabbit.AckMessage(message)
	} else {
		rabbit.NackMessage(message)
		log.Debugf("Repeated Finish-Message from the %s input (instance #%s) received.", inputNode, instance)
	}
}

func processCloseSignal(
	message amqp.Delivery,
	inputNode string,
	instance string,
	signalsMap map[string]map[string]bool,
	signalsMutex *sync.Mutex,
	signalsNeeded  map[string]int,
	connWg *sync.WaitGroup,
	finishWg *sync.WaitGroup,
	procWgsByDataset map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
	callback func(),
) {
	signalsMutex.Lock()
	_, newInstanceSignaledByInput, allInstancesSignaledByInput, everyInputAllInstancesSignaled := 
		comms.SingleDatasetSignalsControl(inputNode, instance, signalsMap, signalsNeeded)
	signalsMutex.Unlock()

	if newInstanceSignaledByInput {
		log.Infof("Close-Message #%d from the %s input received.", len(signalsMap), inputNode)
	}

	if allInstancesSignaledByInput {
		log.Infof("All Close-Messages from the %s input received.", inputNode)
	}

	if everyInputAllInstancesSignaled {
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

	if newInstanceSignaledByInput {
		rabbit.AckMessage(message)
	} else {
		rabbit.NackMessage(message)
		log.Debugf("Repeated Close-Message from the %s input (instance #%s) received.", inputNode, instance)
	}
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
	startSignalsReceivedByDataset map[int]map[string]map[string]bool,
	finishSignalsReceivedByDataset map[int]map[string]map[string]bool,
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

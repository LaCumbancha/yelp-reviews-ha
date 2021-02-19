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

const DefaultFlow = ""
const DefaultDataset = 0
const DefaultPartition = "0"


type FlowMessage struct {
	Flow 		string
	Message 	amqp.Delivery
}

func flowLogMessage(messageType string, flow string) {
	if flow == DefaultFlow {
		log.Infof("%s received.", messageType)
	} else {
		log.Infof("%s from the %s flow received.", messageType, flow)
	}
}

func ReceiveInputs(
	flow string, 
	inputs <- chan amqp.Delivery, 
	mainChannel chan amqp.Delivery, 
	startingChannel chan amqp.Delivery,
	finishingChannel chan amqp.Delivery, 
	closingChannel chan *FlowMessage,
	inputSignals int,
	procWgs map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
) {
	distinctCloseSignals := make(map[string]int)
	distinctStartSignals := make(map[int]map[string]int)
	distinctFinishSignals := make(map[int]map[string]int)

	for message := range inputs {
		messageBody := string(message.Body)
		_, dataset, instance, _, mainMessage := comms.UnsignMessage(messageBody)

		if comms.IsStartMessage(mainMessage) {
			firstStartReceived, newStartReceived, _ := comms.MultiDatasetControl(dataset, instance, distinctStartSignals, inputSignals)

			if firstStartReceived {
				flowLogMessage(fmt.Sprintf("First Start-Messages from dataset #%d", dataset), flow)
				startingChannel <- message
			} else {
				rabbit.AckMessage(message)
			}

			if newStartReceived {
				flowLogMessage(fmt.Sprintf("Start-Message #%d from dataset #%d", len(distinctStartSignals[dataset]), dataset), flow)
				utils.DefineWaitGroupByDataset(dataset, procWgs, procWgsMutex).Add(1)
			}

		} else if comms.IsFinishMessage(mainMessage) {
			_, newFinishReceived, allFinishReceived := comms.MultiDatasetControl(dataset, instance, distinctFinishSignals, inputSignals)

			if newFinishReceived {
				flowLogMessage(fmt.Sprintf("Finish-Message #%d from dataset #%d", len(distinctFinishSignals[dataset]), dataset), flow)
				utils.WaitGroupByDataset(dataset, procWgs, procWgsMutex).Done()
			}

			if allFinishReceived {
				flowLogMessage(fmt.Sprintf("All Finish-Messages from dataset #%d", dataset), flow)
				finishingChannel <- message
			} else {
				rabbit.AckMessage(message)
			}

		} else if comms.IsCloseMessage(mainMessage) {
			_, newCloseReceived, allCloseReceived := comms.SingleControl(instance, distinctCloseSignals, inputSignals)

			if newCloseReceived {
				flowLogMessage(fmt.Sprintf("Close-Message #%d", len(distinctCloseSignals)), flow)
			}

			if allCloseReceived {
				flowLogMessage("All Close-Messages", flow)
				closingChannel <- &FlowMessage { flow, message }
			} else {
				rabbit.AckMessage(message)
			}

		} else {
			log.Debugf("Message from dataset %d received. ProcWgs: %v.", dataset, procWgs)
			log.Tracef("Message: '%s'.", string(message.Body))
			utils.WaitGroupByDataset(dataset, procWgs, procWgsMutex).Add(1)
			mainChannel <- message
		}
	}
}

func ProcessData(
	workersPool int, 
	mainChannel chan amqp.Delivery, 
	callback func(string, int, string, int, string), 
	receivedMsgs map[string]bool,
	procMutex *sync.Mutex,
	procWgs map[int]*sync.WaitGroup,
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

					procMutex.Lock()
					if _, found := receivedMsgs[messageId]; found {
						procMutex.Unlock()
						log.Warnf("Message #%s was already received and processed.", messageId)
					} else {
						receivedMsgs[messageId] = true
						procMutex.Unlock()
						callback(inputNode, dataset, instance, bulk, data)

						procMutex.Lock()
						bkp.StoreSignalsBackup(receivedMsgs, bkp.ReceivedBkp)
						procMutex.Unlock()
					}

					rabbit.AckMessage(message)
    				utils.WaitGroupByDataset(dataset, procWgs, procWgsMutex).Done()
				} else {
					log.Warnf("Unexpected message received: '%s'", messageBody)
				}
				
			}
		}()
	}
}

func ProcessStart(
	startingSignals map[int]int,
	neededInputs int,
	savedInputs int,
	startingChannel chan amqp.Delivery,
	callback func(int),
) {
	// Send start message when all inputs starting messages where received.
	for message := range startingChannel {
		_, datasetStarted, _, _, _ := comms.UnsignMessage(string(message.Body))

		if received, found := startingSignals[datasetStarted]; found {
			startingSignals[datasetStarted] = received + 1
		} else if datasetStarted > 0 {
			startingSignals[datasetStarted] = savedInputs + 1
		} else {
			startingSignals[datasetStarted] = 1
		}

		if startingSignals[datasetStarted] == neededInputs {
			log.Infof("Every Start-Message needed were received.")
			callback(datasetStarted)
		}

		bkp.StoreSignalsBackup(startingSignals, bkp.StartBkp)
		rabbit.AckMessage(message)
	}
}

func ProcessFinish(
	finishingSignals map[int]int,
	neededInputs int,
	savedInputs int,
	finishingChannel chan amqp.Delivery,
	callback func(int),
	procWgs map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
	finishWg *sync.WaitGroup,
) {
	// Send finish message each time a dataset is completed.
	for finishMessage := range finishingChannel {
		_, datasetFinished, _, _, _ := comms.UnsignMessage(string(finishMessage.Body))

		if received, found := finishingSignals[datasetFinished]; found {
			finishingSignals[datasetFinished] = received + 1
		} else if datasetFinished > 0 {
			finishingSignals[datasetFinished] = savedInputs + 1
		} else {
			finishingSignals[datasetFinished] = 1
		}

		if finishingSignals[datasetFinished] == neededInputs {
			log.Infof("Every Finish-Message needed were received.")
			finishWg.Add(1)
			utils.WaitGroupByDataset(datasetFinished, procWgs, procWgsMutex).Wait()
			utils.DeleteWaitGroupByDataset(datasetFinished, procWgs, procWgsMutex)
			callback(datasetFinished)
			finishWg.Done()
		}

		bkp.StoreSignalsBackup(finishingSignals, bkp.FinishBkp)
		rabbit.AckMessage(finishMessage)
	}
}

func ProcessClose(
	closingSignals map[string]int,
	neededInputs int,
	closingChannel chan *FlowMessage,
	callback func(),
	procWgs map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
	finishWg *sync.WaitGroup,
	connWg *sync.WaitGroup,
) {
	// Send close message when all closing messages where received.
	for closeMessage := range closingChannel {
		if received, found := closingSignals[closeMessage.Flow]; found {
			closingSignals[closeMessage.Flow] = received + 1
		} else {
			closingSignals[closeMessage.Flow] = 1
		}
		
		if len(closingSignals) == neededInputs {
			log.Infof("Every Close-Message needed were received.")
			for _, datasetWg := range utils.AllDatasetsWaitGroups(procWgs, procWgsMutex) { 
				datasetWg.Wait()
			}
			finishWg.Wait()
			callback()
			connWg.Done()
		}

		bkp.StoreSignalsBackup(closingSignals, bkp.CloseBkp)
		rabbit.AckMessage(closeMessage.Message)
	}
}

func InitializeProcessWaitGroups(
	procWgs map[int]*sync.WaitGroup, 
	procWgsMutex *sync.Mutex, 
	startSignals map[int]int, 
	finishSignals map[int]int,
	neededInputs int, 
	savedInputs int,
) {
	for dataset, startSignalsReceived := range startSignals {
		if finishSignalsReceived, found := finishSignals[dataset]; found {
			if finishSignalsReceived != neededInputs {
				runningInputs := startSignalsReceived - finishSignalsReceived

				procWgsMutex.Lock()
				for idx := 1; idx <= runningInputs; idx++ {
					procWgs[dataset] = &sync.WaitGroup{}
					procWgs[dataset].Add(1)
				}
				procWgsMutex.Unlock()
			}
		} else {
			runningInputs := startSignalsReceived
			if dataset > 1 {
				runningInputs -= savedInputs
			}

			procWgsMutex.Lock()
			for idx := 1; idx <= runningInputs; idx++ {
				procWgs[dataset] = &sync.WaitGroup{}
			    procWgs[dataset].Add(1)
			}
			procWgsMutex.Unlock()
		}
	}
}

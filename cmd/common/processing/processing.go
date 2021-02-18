package processing

import (
	"fmt"
	"sync"
	"time"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/common/utils"

	log "github.com/sirupsen/logrus"
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
			newStartReceived, allStartReceived := comms.MultiDatasetControl(dataset, instance, distinctStartSignals, inputSignals)

			if newStartReceived {
				flowLogMessage(fmt.Sprintf("Start-Message #%d from dataset #%d", len(distinctStartSignals[dataset]), dataset), flow)
			}

			if allStartReceived {
				utils.DefineWaitGroupByDataset(dataset, procWgs, procWgsMutex).Add(1)
				flowLogMessage(fmt.Sprintf("All Start-Messages from dataset #%d", dataset), flow)
				startingChannel <- message
			} else {
				rabbit.AckMessage(message)
			}

		} else if comms.IsFinishMessage(mainMessage) {
			newFinishReceived, allFinishReceived := comms.MultiDatasetControl(dataset, instance, distinctFinishSignals, inputSignals)

			if newFinishReceived {
				flowLogMessage(fmt.Sprintf("Finish-Message #%d from dataset #%d", len(distinctFinishSignals[dataset]), dataset), flow)
			}

			if allFinishReceived {
				utils.WaitGroupByDataset(dataset, procWgs, procWgsMutex).Done()
				flowLogMessage(fmt.Sprintf("All Finish-Messages from dataset #%d", dataset), flow)
				finishingChannel <- message
			} else {
				rabbit.AckMessage(message)
			}

		} else if comms.IsCloseMessage(mainMessage) {
			newCloseReceived, allCloseReceived := comms.SingleControl(instance, distinctCloseSignals, inputSignals)

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
	receivedMsgsMutex *sync.Mutex,
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

					receivedMsgsMutex.Lock()
					if _, found := receivedMsgs[messageId]; found {
						receivedMsgsMutex.Unlock()
						log.Warnf("Message #%s was already received and processed.", messageId)
					} else {
						receivedMsgs[messageId] = true
						receivedMsgsMutex.Unlock()
						callback(inputNode, dataset, instance, bulk, data)

						receivedMsgsMutex.Lock()
						receivedMsgsBytes, err := json.Marshal(receivedMsgs)
						receivedMsgsMutex.Unlock()
						if err != nil {
							log.Errorf("Error serializing data from received messages map. Err: %s", err)
						} else {
							StoreBackup(ReceivedBkp, receivedMsgsBytes)
						}
					}

					// TODO: REMOVE! JUST FOR TESTING
					if inputNode == "M3" && bulk == 10 {
						receivedMsgsMutex.Lock()
						log.Infof("CORTAR AHORA!!!!")
						time.Sleep(10 * time.Second)
						receivedMsgsMutex.Unlock()
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
		} else if datasetStarted > 1 {
			startingSignals[datasetStarted] = savedInputs + 1
		} else {
			startingSignals[datasetStarted] = 1
		}

		if startingSignals[datasetStarted] == neededInputs {
			callback(datasetStarted)
		}

		startSignalsBytes, err := json.Marshal(startingSignals)
		if err != nil {
			log.Errorf("Error serializing data from starting signals map. Err: %s", err)
		} else {
			StoreBackup(StartBkp, startSignalsBytes)
		}

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
		} else if datasetFinished > 1 {
			finishingSignals[datasetFinished] = savedInputs + 1
		} else {
			finishingSignals[datasetFinished] = 1
		}

		if finishingSignals[datasetFinished] == neededInputs {
			finishWg.Add(1)
			utils.WaitGroupByDataset(datasetFinished, procWgs, procWgsMutex).Wait()
			utils.DeleteWaitGroupByDataset(datasetFinished, procWgs, procWgsMutex)
			callback(datasetFinished)
			finishWg.Done()
		}

		finishSignalsBytes, err := json.Marshal(finishingSignals)
		if err != nil {
			log.Errorf("Error serializing data from finishing signals map. Err: %s", err)
		} else {
			StoreBackup(FinishBkp, finishSignalsBytes)
		}

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
			for _, datasetWg := range utils.AllDatasetsWaitGroups(procWgs, procWgsMutex) { 
				datasetWg.Wait()
			}
			finishWg.Wait()
			callback()
			connWg.Done()
		}

		closeSignalsBytes, err := json.Marshal(closingSignals)
		if err != nil {
			log.Errorf("Error serializing data from closing signals map. Err: %s", err)
		} else {
			StoreBackup(CloseBkp, closeSignalsBytes)
		}

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
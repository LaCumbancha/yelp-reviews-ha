package processing

import (
	"fmt"
	"sync"
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
				flowMessageLog(fmt.Sprintf("Start-Message #%d from dataset #%d", len(distinctStartSignals[dataset]), dataset), flow)
			}

			if allStartReceived {
				utils.DefineWaitGroupByDataset(dataset, procWgs, procWgsMutex).Add(1)
				flowMessageLog(fmt.Sprintf("All Start-Message from dataset #%d", dataset), flow)
				startingChannel <- message
			} else {
				rabbit.AckMessage(message)
			}

		} else if comms.IsFinishMessage(mainMessage) {
			newFinishReceived, allFinishReceived := comms.MultiDatasetControl(dataset, instance, distinctFinishSignals, inputSignals)

			if newFinishReceived {
				flowMessageLog(fmt.Sprintf("Finish-Message #%d from dataset #%d", len(distinctFinishSignals[dataset]), dataset), flow)
			}

			if allFinishReceived {
				utils.WaitGroupByDataset(dataset, procWgs, procWgsMutex).Done()
				flowMessageLog(fmt.Sprintf("All Finish-Message from dataset #%d", dataset), flow)
				finishingChannel <- message
			} else {
				rabbit.AckMessage(message)
			}

		} else if comms.IsCloseMessage(mainMessage) {
			newCloseReceived, allCloseReceived := comms.SingleControl(instance, distinctCloseSignals, inputSignals)

			if newCloseReceived {
				flowMessageLog(fmt.Sprintf("Close-Message #%d", len(distinctCloseSignals)), flow)
			}

			if allCloseReceived {
				flowMessageLog("All Close-Message", flow)
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
					logb.Instance().Infof(fmt.Sprintf("Message #%s.%s.%d.%d received.", inputNode, instance, dataset, bulk), bulk)
					callback(inputNode, dataset, instance, bulk, data)
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
	neededInputs int,
	savedInputs int,
	startingChannel chan amqp.Delivery,
	callback func(int),
) {
	startingSignals := make(map[int]int)

	// Send finish message each time a dataset is completed.
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

		rabbit.AckMessage(message)
	}
}

func ProcessFinish(
	neededInputs int,
	savedInputs int,
	finishingChannel chan amqp.Delivery,
	callback func(int),
	procWgs map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
	finishWg *sync.WaitGroup,
) {
	finishSignals := make(map[int]int)

	// Send finish message each time a dataset is completed.
	for finishMessage := range finishingChannel {
		_, datasetFinished, _, _, _ := comms.UnsignMessage(string(finishMessage.Body))

		if received, found := finishSignals[datasetFinished]; found {
			finishSignals[datasetFinished] = received + 1
		} else if datasetFinished > 1 {
			finishSignals[datasetFinished] = savedInputs + 1
		} else {
			finishSignals[datasetFinished] = 1
		}

		if finishSignals[datasetFinished] == neededInputs {
			finishWg.Add(1)
			utils.WaitGroupByDataset(datasetFinished, procWgs, procWgsMutex).Wait()
			utils.DeleteWaitGroupByDataset(datasetFinished, procWgs, procWgsMutex)
			callback(datasetFinished)
			finishWg.Done()
		}

		rabbit.AckMessage(finishMessage)
	}
}

func ProcessClose(
	neededInputs int,
	closingChannel chan *FlowMessage,
	callback func(),
	procWgs map[int]*sync.WaitGroup,
	procWgsMutex *sync.Mutex,
	finishWg *sync.WaitGroup,
	connWg *sync.WaitGroup,
) {
	closingSignals := make(map[string]int)

	// Send finish message each time a dataset is completed.
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

		rabbit.AckMessage(closeMessage.Message)
	}
}

func flowMessageLog(messageType string, flow string) {
	if flow == DefaultFlow {
		log.Infof("%s received.", messageType)
	} else {
		log.Infof("%s from the %s flow received.", messageType, flow)
	}
}

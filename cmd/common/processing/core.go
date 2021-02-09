package processing

import (
	"sync"
	"github.com/streadway/amqp"
)

// Common processing for Mappers, Filters, Aggregators and Prettiers.
func Transformation(
	workersPool int,
	endSignals int,
	inputs <- chan amqp.Delivery,
	mainCallback func(string, int, string, int, string),
	startCallback func(int),
	finishCallback func(int),
	closeCallback func(),
) {
	mainChannel := make(chan amqp.Delivery)
	startingChannel := make(chan amqp.Delivery)
	finishingChannel := make(chan amqp.Delivery)
	closingChannel := make(chan *FlowMessage)

	var procWgs = make(map[int]*sync.WaitGroup)
	var procWgsMutex = &sync.Mutex{}
	var finishWg sync.WaitGroup
	var connWg sync.WaitGroup
	connWg.Add(1)

	neededInputs := 1
	savedInputs := 0
	go ReceiveInputs(DefaultFlow, inputs, mainChannel, startingChannel, finishingChannel, closingChannel, endSignals, procWgs, procWgsMutex)
	go ProcessData(workersPool, mainChannel, mainCallback, procWgs, procWgsMutex)
	go ProcessStart(neededInputs, savedInputs, startingChannel, startCallback)
	go ProcessFinish(neededInputs, savedInputs, finishingChannel, finishCallback, procWgs, procWgsMutex, &finishWg)
	go ProcessClose(neededInputs, closingChannel, closeCallback, procWgs, procWgsMutex, &finishWg, &connWg)

	// Waiting for close procedure.
	connWg.Wait()
}

// Common processing for Joiners.
func Join(
	flow1 string,
	flow2 string,
	neededInputs int,
	savedInputs int,
	workersPool int,
	endSignals1 int,
	endSignals2 int,
	inputs1 <- chan amqp.Delivery,
	inputs2 <- chan amqp.Delivery,
	mainCallback1 func(string, int, string, int, string),
	mainCallback2 func(string, int, string, int, string),
	startCallback func(int),
	finishCallback func(int),
	closeCallback func(),
) {
	mainChannel1 := make(chan amqp.Delivery)
	mainChannel2 := make(chan amqp.Delivery)
	startingChannel := make(chan amqp.Delivery)
	finishingChannel := make(chan amqp.Delivery)
	closingChannel := make(chan *FlowMessage)
	
	var procWgs = make(map[int]*sync.WaitGroup)
	var procWgsMutex = &sync.Mutex{}
	var finishWg sync.WaitGroup
	var connWg sync.WaitGroup
	connWg.Add(1)
	
	go ReceiveInputs(flow1, inputs1, mainChannel1, startingChannel, finishingChannel, closingChannel, endSignals1, procWgs, procWgsMutex)
	go ReceiveInputs(flow2, inputs2, mainChannel2, startingChannel, finishingChannel, closingChannel, endSignals2, procWgs, procWgsMutex)
	go ProcessData(int(workersPool/2), mainChannel1, mainCallback1, procWgs, procWgsMutex)
	go ProcessData(int(workersPool/2), mainChannel2, mainCallback2, procWgs, procWgsMutex)
	go ProcessStart(neededInputs, savedInputs, startingChannel, startCallback)
	go ProcessFinish(neededInputs, savedInputs, finishingChannel, finishCallback, procWgs, procWgsMutex, &finishWg)
	go ProcessClose(neededInputs, closingChannel, closeCallback, procWgs, procWgsMutex, &finishWg, &connWg)

	// Waiting for close procedure.
	connWg.Wait()
}

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
	finishCallback func(int),
	closeCallback func(),
) {
	mainChannel := make(chan amqp.Delivery)
	endingChannel := make(chan int)

	closingConn := false
	connMutex := &sync.Mutex{}

	var connWg sync.WaitGroup
	connWg.Add(1)

	var procWg sync.WaitGroup
	procWg.Add(1)

	go InitializeProcessingWorkers(workersPool, mainChannel, mainCallback, &procWg)
	go ProcessInputs(DefaultFlow, inputs, mainChannel, endingChannel, endSignals, &procWg, &connWg)
	go ProcessSingleFinish(endingChannel, finishCallback, &procWg, closingConn, connMutex)
	CloseConnection(closeCallback, &procWg, &connWg, closingConn, connMutex)
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
	finishCallback func(int),
	closeCallback func(),
) {
	var connWg sync.WaitGroup
	connWg.Add(1)

	var procWg sync.WaitGroup
	procWg.Add(2)

	closingConn := false
	connMutex := &sync.Mutex{}

	mainChannel1 := make(chan amqp.Delivery)
	mainChannel2 := make(chan amqp.Delivery)
	endingChannel := make(chan int)
	
	go InitializeProcessingWorkers(int(workersPool/2), mainChannel1, mainCallback1, &procWg)
	go ProcessInputs(flow1, inputs1, mainChannel1, endingChannel, endSignals1, &procWg, &connWg)	

	go InitializeProcessingWorkers(int(workersPool/2), mainChannel2, mainCallback2, &procWg)
	go ProcessInputs(flow2, inputs2, mainChannel2, endingChannel, endSignals2, &procWg, &connWg)

	// Retrieving joined data and closing connection.
	go ProcessMultipleFinish(neededInputs, savedInputs, endingChannel, finishCallback, &procWg, closingConn, connMutex)
	CloseConnection(closeCallback, &procWg, &connWg, closingConn, connMutex)
}

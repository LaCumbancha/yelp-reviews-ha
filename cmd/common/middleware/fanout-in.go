package middleware

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitInputFanout struct {
	exchange 			string
	channel 			*amqp.Channel
	queue 				string
}

func NewRabbitInputFanout(channel *amqp.Channel, name string, queue string) *RabbitInputFanout {
	fanout := &RabbitInputFanout {
		exchange:	name,
		channel: 	channel,
		queue:		queue,
	}

	fanout.initialize()
	return fanout
}

func (fanout *RabbitInputFanout) initialize() {
	err := fanout.channel.ExchangeDeclare(
		fanout.exchange, 		// Name
		"fanout",				// Type
		true,   				// Durable
		false,   				// Auto-Deleted
		false,   				// Internal
		false,   				// No-Wait
		nil,     				// Args
	)

	if err != nil {
		log.Fatalf("Error creating Fanout-Exchange %s. Err: '%s'", fanout.exchange, err)
	} else {
		log.Infof("Fanout-Exchange %s created.", fanout.exchange)
	}

	queue, err := fanout.channel.QueueDeclare(
        fanout.queue,  			// Name
        true, 					// Durable
        false, 					// Auto-Deleted
        false,  				// Exclusive
        false, 					// No-Wait
        nil,   					// Args
    )

    if err != nil {
		log.Fatalf("Error creating queue for Fanout-Exchange %s. Err: '%s'", fanout.exchange, err)
	} else {
		log.Infof("Queue %s for Fanout-Exchange %s created.", queue.Name, fanout.exchange)
	}

	err = fanout.channel.QueueBind(
        queue.Name, 			// Queue
        "",  	 				// Routing-Key
        fanout.exchange, 		// Exchange
        false,
        nil,
    )

    if err != nil {
		log.Fatalf("Error binding queue %s to Fanout-Exchange %s. Err: '%s'", queue.Name, fanout.exchange, err)
	} else {
		log.Infof("Queue %s binded to Fanout-Exchange %s.", queue.Name, fanout.exchange)
	}

	fanout.queue = queue.Name
}

func (fanout *RabbitInputFanout) ConsumeData() <-chan amqp.Delivery {
	data, err := fanout.channel.Consume(
		fanout.queue, 			// Name
		"",     				// Consumer
		false,   				// Auto-ACK
		false,  				// Exclusive
		false,  				// No-Local
		false,  				// No-Wait
		nil,    				// Args
	)

	if err != nil {
		log.Fatalf("Error receiving data from Fanout-Exchange %s. Err: '%s'", fanout.exchange, err)
	}

	return data
}

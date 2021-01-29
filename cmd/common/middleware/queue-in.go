package middleware

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitInputQueue struct {
	channel 			*amqp.Channel
	name 				string
}

func NewRabbitInputQueue(channel *amqp.Channel, name string) *RabbitInputQueue {
	queue := &RabbitInputQueue {
		channel: 	channel,
		name:		name,
	}

	queue.initialize()
	return queue
}

func (queue *RabbitInputQueue) initialize() {
	_, err := queue.channel.QueueDeclare(
		queue.name, 	// Name
		true,   		// Durable
		false,   		// Auto-Deleted
		false,   		// Exclusive
		false,   		// No-wait
		nil,     		// Args
	)

	if err != nil {
		log.Fatalf("Error creating queue %s. Err: '%s'", queue.name, err)
	} else {
		log.Infof("Queue %s created.", queue.name)
	}
}

func (queue *RabbitInputQueue) ConsumeData() <-chan amqp.Delivery {
	data, err := queue.channel.Consume(
		queue.name, 	// Name
		"",     		// Consumer
		false,   		// Auto-ACK
		false,  		// Exclusive
		false,  		// No-Local
		false,  		// No-Wait
		nil,    		// Args
	)

	if err != nil {
		log.Errorf("Error receiving data from queue %s. Err: '%s'", queue.name, err)
	}

	return data
}

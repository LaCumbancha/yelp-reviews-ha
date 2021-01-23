package middleware

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitOutputQueue struct {
	Name 			string
	channel 		*amqp.Channel
	finishMessage	string
	endSignals 		int
}

func NewRabbitOutputQueue(channel *amqp.Channel, name string, endMessage string, endSignals int) *RabbitOutputQueue {
	queue := &RabbitOutputQueue {
		Name:			name,
		channel: 		channel,
		endSignals:		endSignals,
		finishMessage:	endMessage,
	}

	queue.initialize()
	return queue
}

func (queue *RabbitOutputQueue) initialize() {
	_, err := queue.channel.QueueDeclare(
		queue.Name, 						// Name
		false,   							// Durable
		false,   							// Auto-Deleted
		false,   							// Exclusive
		false,   							// No-wait
		nil,     							// Args
	)

	if err != nil {
		log.Fatalf("Error creating queue %s. Err: '%s'", queue.Name, err)
	} else {
		log.Infof("Queue %s created.", queue.Name)
	}
}

func (queue *RabbitOutputQueue) PublishData(data []byte) error {
	return queue.channel.Publish(
		"",     							// Exchange
		queue.Name, 						// Routing Key
		false,  							// Mandatory
		false,  							// Immediate
		amqp.Publishing{
			ContentType: 	"text/plain",
			Body:        	data,
		},
	)
}

func (queue *RabbitOutputQueue) PublishFinish() {
	errors := false
	for idx := 1; idx <= queue.endSignals; idx++ {
		err := queue.channel.Publish(
  			"", 							// Exchange
	  		queue.Name,     				// Routing Key
	  		false,  						// Mandatory
	  		false,  						// Immediate
	  		amqp.Publishing{
	  		    ContentType: 	"text/plain",
	  		    Body:        	[]byte(queue.finishMessage),
	  		},
	  	)

		if err != nil {
			errors = true
			log.Errorf("Error sending End-Message #%d to queue %s. Err: '%s'", idx, queue.Name, err)
		}
	}

	if !errors {
		log.Infof("End-Message sent to queue %s.", queue.Name)
	}
}

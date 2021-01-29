package middleware

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitOutputFanout struct {
	Exchange 		string
	channel 		*amqp.Channel
}

func NewRabbitOutputFanout(channel *amqp.Channel, name string) *RabbitOutputFanout {
	fanout := &RabbitOutputFanout {
		Exchange:			name,
		channel: 			channel,
	}

	fanout.initialize()
	return fanout
}

func (fanout *RabbitOutputFanout) initialize() {
	err := fanout.channel.ExchangeDeclare(
	  	fanout.Exchange,   						// Name
	  	"fanout", 								// Type
	  	true,     								// Durable
	  	false,    								// Auto-Deleted
	  	false,    								// Internal
	  	false,    								// No-Wait
	  	nil,      								// Arguments
	)

	if err != nil {
		log.Fatalf("Error creating Fanout-Exchange %s. Err: '%s'", fanout.Exchange, err)
	} else {
		log.Infof("Fanout-Exchange %s created.", fanout.Exchange)
	}
}

func (fanout *RabbitOutputFanout) PublishData(data []byte) error {
	return fanout.channel.Publish(
		fanout.Exchange, 						// Exchange
		"",    									// Routing Key
		false,  								// Mandatory
		false,  								// Immediate
		amqp.Publishing{
			DeliveryMode: 	amqp.Persistent,
		    ContentType: 	"text/plain",
		    Body:        	data,
		},
	)
}

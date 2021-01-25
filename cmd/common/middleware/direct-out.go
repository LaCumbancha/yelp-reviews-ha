package middleware

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitOutputDirect struct {
	Exchange 		string
	channel 		*amqp.Channel
}

func NewRabbitOutputDirect(channel *amqp.Channel, name string) *RabbitOutputDirect {
	direct := &RabbitOutputDirect {
		Exchange:			name,
		channel: 			channel,
	}

	direct.initialize()
	return direct
}

func (direct *RabbitOutputDirect) initialize() {
	err := direct.channel.ExchangeDeclare(
	  	direct.Exchange,   						// Name
	  	"direct", 								// Type
	  	false,     								// Durable
	  	false,    								// Auto-Deleted
	  	false,    								// Internal
	  	false,    								// No-Wait
	  	nil,      								// Arguments
	)

	if err != nil {
		log.Fatalf("Error creating direct-exchange %s. Err: '%s'", direct.Exchange, err)
	} else {
		log.Infof("Direct-Exchange %s created.", direct.Exchange)
	}
}

func (direct *RabbitOutputDirect) PublishData(data []byte, partition string) error {
	return direct.channel.Publish(
		direct.Exchange, 						// Exchange
		partition,    							// Routing Key
		false,  								// Mandatory
		false,  								// Immediate
		amqp.Publishing{
		    ContentType: 	"text/plain",
		    Body:        	data,
		},
	)
}

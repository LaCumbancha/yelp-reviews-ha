package middleware

import (
	"fmt"
	"strings"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

func EstablishConnection(rabbitIp string, rabbitPort string) (*amqp.Connection, *amqp.Channel) {
	connection, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", rabbitIp, rabbitPort))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ at (%s, %s). Err: '%s'", rabbitIp, rabbitPort, err)
	} else {
		log.Infof("Connected to RabbitMQ at (%s, %s).", rabbitIp, rabbitPort)
	}

	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel. Err: '%s'", err)
	} else {
		log.Infof("RabbitMQ channel opened.")
	}

	return connection, channel
}

func AckMessage(message amqp.Delivery) {
	if err := message.Ack(false); err != nil {
		log.Errorf("Error sending message ACK from message '%s'. Err: '%s'", message.MessageId, err)
	}
}

func NackMessage(message amqp.Delivery) {
	if err := message.Nack(false, true); err != nil {
		log.Errorf("Error sending message ACK from message '%s'. Err: '%s'", message.MessageId, err)
	}
}

func InnerQueueName(input string, instance string) string {
	idx1 := strings.Index(input, "-from-")
	
	if idx1 < 0 {
		return input + "." + instance
	} else {
		return input[:idx1] + "." + instance + input[idx1:]
	}
}
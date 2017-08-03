package main

import (
	"github.com/streadway/amqp"
)

// declare useful struct for get session
type SessionsSender struct {
	*amqp.Connection
	*amqp.Channel
  serviceName, correlationId string
}

// constructor for SessonsSender
func NewSessionsSender(serviceName, correlationId string) *SessionsSender {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	return &SessionsSender{conn, ch, serviceName, correlationId}
}

// declare method Write() to realize Writer interface
func (r *SessionsSender) Write(p []byte) (n int, err error) {
	err = r.Channel.Publish(
		r.serviceName,               // exchange
		"plutus-sessions", // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
      CorrelationId: r.correlationId,
			Body:        p,
		})
	return len(p), err
}

// declare initialization method
func (r *SessionsSender) Init() {
	err := r.Channel.ExchangeDeclare(
		r.serviceName, // name
		"topic",          // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		log.Fatal(err)
	}
}

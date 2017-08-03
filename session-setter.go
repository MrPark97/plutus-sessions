package main

import (
	"github.com/streadway/amqp"
)

// declare useful struct for get session
type SessionsSetter struct {
	*amqp.Connection
	*amqp.Channel
	serviceName string
}

// constructor for SessonsGetter
func NewSessionsSetter(serviceName string) *SessionsSetter {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	return &SessionsSetter{conn, ch, serviceName}
}

// declare method Set() to set session
func (r *SessionsSetter) Set(sessionId, correlationId string) (err error) {
	err = r.Channel.Publish(
		"plutus-sessions",       // exchange
		r.serviceName+"."+"set", // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: correlationId,
			Body:          []byte(sessionId),
		})
	return err
}

// declare initialization method
func (r *SessionsSetter) Init() {
	err := r.Channel.ExchangeDeclare(
		"plutus-sessions", // name
		"topic",           // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		log.Fatal(err)
	}
}

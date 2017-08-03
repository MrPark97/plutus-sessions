package main

import (
	"github.com/streadway/amqp"
)

// declare useful struct for get session
type SessionsGetter struct {
	*amqp.Connection
	*amqp.Channel
  serviceName, correlationId string
}

// constructor for SessonsGetter
func NewSessionsGetter(serviceName, correlationId string) *SessionsGetter {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	return &SessionsGetter{conn, ch, serviceName, correlationId}
}

// declare method Send() to send request
func (r *SessionsGetter) Send(p []byte) (err error) {
	err = r.Channel.Publish(
		"plutus-sessions",               // exchange
		r.serviceName+"."+"get", // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
      CorrelationId: r.correlationId,
			Body:        p,
		})
	return err
}

// declare method Get() to get userId
func (r *SessionsGetter) Get() (userId string, err error) {
  q, err := r.Channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unsused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	err = r.Channel.QueueBind(
		q.Name,           // queue name
		"plutus-sessions",              // routing key
		r.serviceName, // exchange
		false,
		nil)

	msgs, err := r.Channel.Consume(
    q.Name, // queue
    "",     // consumer
    true,   // auto-ack
    false,  // exclusive
    false,  // no-local
    false,  // no-wait
    nil,    // args
  )

  for d := range msgs {
    if r.correlationId == d.CorrelationId {
      userId = string(d.Body)
      break
    }
  }

	return
}

// declare initialization method
func (r *SessionsGetter) Init() {
	err := r.Channel.ExchangeDeclare(
		"plutus-sessions", // name
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

  err = r.Channel.ExchangeDeclare(
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

package main

import (
	"strings"
	"time"

	"plutus-sessions/logrus_rabbitmq"

	"github.com/garyburd/redigo/redis"
	"github.com/rcrowley/go-metrics"
	"github.com/rubyist/circuitbreaker"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Init redis and rabbit URLs for connection
var (
	rabbitMQURL = "amqp://guest:guest@localhost:5672"
	redis_proto = "tcp"
	redisURL    = ":6379"
	log         *logrus.Logger
)

// Define rabbitMQ struct for receiving messages with consecutive circuit breaker
type RabbitMQ struct {
	rmq       *amqp.Connection // rabbitMQ connection
	host      string           // url for rabbitMQ connection
	timeout   time.Duration    // func execution time out for circuit breaker
	ErrChan   chan error       // errors consumer channel
	rmqc      *amqp.Channel    // rabbitMQ channel
	circBreak *circuit.Breaker // circuit breaker
}

// Create new rabbit struct init Exchange Queue and Consume messages
func NewRabbitMQ(host string, timeout time.Duration, cb *circuit.Breaker) (*RabbitMQ, error) {
	// init rabbitMQ struct
	rm := &RabbitMQ{
		host:      host,
		timeout:   timeout,
		circBreak: cb,
		ErrChan:   make(chan error),
	}
	// init errors consumer
	failOnError(rm.ErrChan)

	// try to connect and init RabbitMQ if circuit breaker is not tripped
	var err error
	if !rm.circBreak.Tripped() {
		err = rm.Connect()
		if err != nil {
			return rm, err
		}
		err = rm.Listen()
	}
	return rm, err
}

// connect to rabbitMQ
func (r *RabbitMQ) Connect() error {

	var (
		err  error
		conn *amqp.Connection
	)

	// Creates a connection to RabbitMQ
	r.circBreak.Call(func() error {
		conn, err = amqp.Dial(rabbitMQURL)
		r.rmq = conn
		if err != nil {
			return ErrConnect(err.Error())
		}
		return nil
	}, r.timeout)
	if err != nil {
		return ErrConnect(err.Error())
	}
	return err
}

// Declare exchange queue bind queue with exchange and consume messages from RabbitMQ
func (r *RabbitMQ) Listen() error {
	ch, err := r.rmq.Channel()
	r.rmqc = ch

	err = ch.ExchangeDeclare(
		"plutus-sessions", // name
		"topic",           // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unsused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	err = ch.QueueBind(
		q.Name,            // queue name
		"*.*",             // routing key
		"plutus-sessions", // exchange
		false,
		nil)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)

	// Listen in new goroutine
	go func() {
		// create new consecutive breaker for InfluxDB
		cb := circuit.NewConsecutiveBreaker(10)

		// init circuit breaker listener
		// Subscribe to the circuit breaker events
		events := cb.Subscribe()

		var CurrentMessage amqp.Delivery
		go func() {
			for {
				e := <-events
				switch e {
				case circuit.BreakerTripped:
					cb.Reset()
					log.Fatalln("[x] service stopped")
				case circuit.BreakerFail:
					err := redisOps(CurrentMessage.Body, CurrentMessage.RoutingKey, CurrentMessage.CorrelationId, r.ErrChan, cb)
					if err != nil {
						r.ErrChan <- err
					}
					continue
				}
			}
		}()

		for d := range msgs {
			CurrentMessage = d

			// do operations with redis send error or acknnowledge
			err := redisOps(d.Body, d.RoutingKey, d.CorrelationId, r.ErrChan, cb)
			if err != nil {
				r.ErrChan <- err
			} else {
				d.Ack(false)
			}
		}
	}()
	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	return err
}

// Describes new error type ErrConnect which used if failed to connect remote services
type ErrConnect string

func (e ErrConnect) Error() string {
	return string(e)
}

// Describes new error type ErrSession which used if method get throws error from Redis
type ErrSession string

func (e ErrSession) Error() string {
	return "[x] wrong session id [" + string(e) + "]"
}

// initialize logger which will send all logs to rabbitmq
func init() {
	log = logrus.New()
	log.Hooks.Add(logrus_rabbitmq.NewRabbitMQHook())
}

func main() {
	// send log about service starting
	log.Warnln("service started")

	// init rabbitMQ circuit breaker
	// init event listener
	// Subscribe to the circuit breaker events
	rcb := circuit.NewConsecutiveBreaker(10)
	events := rcb.Subscribe()

	// The circuit breaker events handling
	go func() {
		for {
			e := <-events
			switch e {
			case circuit.BreakerTripped:
				rcb.Reset()
				log.Fatal("[x] service stopped")
			case circuit.BreakerFail:
				rmq, err := NewRabbitMQ(rabbitMQURL, time.Second*10, rcb)
				if err != nil && rmq.ErrChan != nil {
					rmq.ErrChan <- err
				}
				continue
			}
		}
	}()

	// try to create and init RabbitMQ with circuit breaker timeout 10 and threashold 10
	rmq, err := NewRabbitMQ(rabbitMQURL, time.Second*10, rcb)
	if err != nil {
		if rmq.ErrChan != nil {
			rmq.ErrChan <- err
		} else {
			log.Println(err.Error())
		}
	}
	// try to defer close rabbitMQ channel and connection if exist
	if rmq.rmq != nil {
		defer rmq.rmq.Close()
	}
	if rmq.rmqc != nil {
		defer rmq.rmqc.Close()
	}

	//init self system metrics
	sysMetrics := metrics.NewRegistry()
	metrics.RegisterDebugGCStats(sysMetrics)
	metrics.RegisterRuntimeMemStats(sysMetrics)

	// init rabbitmqwriter which will write this service metrics
	rabbitmqwriter := NewRabbitMQWriter()
	rabbitmqwriter.Init()

	// periodically capture metrics values and write to rabbitMQ
	metricsDuration := time.Second * 10
	go metrics.CaptureDebugGCStats(sysMetrics, metricsDuration)
	go metrics.CaptureRuntimeMemStats(sysMetrics, metricsDuration)
	go metrics.WriteJSON(sysMetrics, metricsDuration, rabbitmqwriter)

	// wait messages from rabbitMQ
	forever := make(chan bool)
	<-forever

}

// Is used for error handling
func failOnError(errChan <-chan error) {
	go func() {
		for {
			err := <-errChan
			if err != nil {
				switch err.(type) {
				case ErrSession:
					log.Printf(err.Error())
				case ErrConnect:
					log.Printf(err.Error())
				default:
					log.Fatalf(err.Error())
				}
			}
		}
	}()
}

// Do operations with Redis and send answer if nedded
func redisOps(sessionId []byte, routingKey string, correlationId string, errChan chan<- error, cb *circuit.Breaker) error {

	//get method and service-sender name
	routingSubKeys := strings.Split(routingKey, `.`)
	serviceName := routingSubKeys[0]
	methodName := routingSubKeys[1]

	var err error
	// try to connect to Redis and do operation
	cb.Call(func() error {
		//initialize redis connection
		c, err := redis.Dial(redis_proto, redisURL)
		if err != nil {
			return ErrConnect(err.Error())
		}
		defer c.Close()

		if methodName == "get" {

			// get userId from Redis and check errors
			userId, err := redis.String(c.Do("GET", string(sessionId)))
			if err != nil {
				return ErrSession(err.Error())
			}

			// init rabbitmqwriter which will write answer
			answerSender := NewSessionsSender(serviceName, correlationId)
			answerSender.Init()

			// send answer to rabbitmq and check errors
			_, err = answerSender.Write([]byte(userId))
			if err != nil {
				return ErrConnect(err.Error())
			}

		} else if methodName == "set" {
			c.Do("SET", string(sessionId), correlationId)
		}

		if err != nil {
			return ErrConnect(err.Error())
		}

		return nil
	}, time.Second*10)
	if err != nil {
		return ErrConnect(err.Error())
	}

	return err
}

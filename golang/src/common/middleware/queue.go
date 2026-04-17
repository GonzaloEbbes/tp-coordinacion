package middleware

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	middleware "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	ch          *amqp.Channel
	rabbitQueue amqp.Queue
	conn        *amqp.Connection
	consumerTag string

	shouldStopLock sync.Mutex
	shouldStop     bool
}

func NewQueueMiddleware(hostname string, port int, queueName string) (*QueueMiddleware, error) {
	middlewareQueue := &QueueMiddleware{}

	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%d/", hostname, port))
	if err != nil {
		return nil, middleware.ErrMessageMiddlewareDisconnected
	}
	middlewareQueue.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		_ = middlewareQueue.Close()
		return nil, middleware.ErrMessageMiddlewareMessage
	}
	middlewareQueue.ch = ch

	queue, err := ch.QueueDeclare(
		queueName,
		false, // durability
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		_ = middlewareQueue.Close()
		return nil, middleware.ErrMessageMiddlewareMessage
	}
	middlewareQueue.rabbitQueue = queue

	return middlewareQueue, nil
}

func (m *QueueMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) (err error) {
	m.consumerTag = fmt.Sprintf("queue-%s-%d", m.rabbitQueue.Name, time.Now().UnixNano())
	// We could use something as uuid but this would require an additional dependency that would modify the go.mod file
	// to avoid possible conflicts from discarding the go.mod changes, we just use timestamps

	msgs, err := m.ch.Consume(
		m.rabbitQueue.Name,
		m.consumerTag,
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}

	m.shouldStopLock.Lock()
	m.shouldStop = false // We set the value here to ensure that calling StartConsuming would restart consumption if it was stopped before
	m.shouldStopLock.Unlock()

	for msg := range msgs {
		ack := func() {
			_ = msg.Ack(false)
		}
		nack := func() {
			_ = msg.Nack(false, false)
		}
		callbackFunc(middleware.Message{Body: string(msg.Body)}, ack, nack)

		var shouldStop bool
		m.shouldStopLock.Lock()
		shouldStop = m.shouldStop
		m.shouldStopLock.Unlock()
		if shouldStop {
			log.Printf("Stopping consumption for queue %s after current message", m.rabbitQueue.Name)
			break
		}
	}
	return nil
}

func (m *QueueMiddleware) StopConsuming() {
	m.shouldStopLock.Lock()
	m.shouldStop = true
	m.shouldStopLock.Unlock()

	if m.ch != nil && m.consumerTag != "" {
		_ = m.ch.Cancel(m.consumerTag, false)
	}
}

func (m *QueueMiddleware) Send(msg middleware.Message) (err error) {
	if m.ch == nil || m.conn == nil {
		return middleware.ErrMessageMiddlewareDisconnected
	}

	err = m.ch.Publish(
		"",                 // exchange
		m.rabbitQueue.Name, // routing key
		false,              // mandatory
		false,              // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return middleware.ErrMessageMiddlewareDisconnected
		}
		return middleware.ErrMessageMiddlewareMessage
	}

	return nil
}

func (m *QueueMiddleware) Close() error {
	// TODO: If Close runs while the current callback is still processing a message,
	// Ack/Nack may race against channel shutdown. A simple future improvement would
	// be to track in-flight callbacks and wait for the current one before closing.
	var closeErr error

	if m.ch != nil {
		if err := m.ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = middleware.ErrMessageMiddlewareClose
		}
		m.ch = nil
	}
	if m.conn != nil {
		if err := m.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			if closeErr == nil {
				closeErr = middleware.ErrMessageMiddlewareClose
			}
		}
		m.conn = nil
	}

	return closeErr
}

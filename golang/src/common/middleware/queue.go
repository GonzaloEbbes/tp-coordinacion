package middleware

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const queuePrefetchCount = 1

type QueueMiddleware struct {
	ch          *amqp.Channel
	rabbitQueue amqp.Queue
	conn        *amqp.Connection
	consumerTag string

	shouldStopLock sync.Mutex
	shouldStop     bool
	inFlight       sync.WaitGroup
	callbackGate   sync.Mutex
	closing        bool
}

func NewQueueMiddleware(hostname string, port int, queueName string) (*QueueMiddleware, error) {
	middlewareQueue := &QueueMiddleware{}

	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%d/", hostname, port))
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}
	middlewareQueue.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		_ = middlewareQueue.Close()
		return nil, ErrMessageMiddlewareMessage
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
		return nil, ErrMessageMiddlewareMessage
	}
	middlewareQueue.rabbitQueue = queue

	return middlewareQueue, nil
}

func (m *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) (err error) {
	m.consumerTag = fmt.Sprintf("queue-%s-%d", m.rabbitQueue.Name, time.Now().UnixNano())
	// We could use something as uuid but this would require an additional dependency that would modify the go.mod file
	// to avoid possible conflicts from discarding the go.mod changes, we just use timestamps

	// With manual ACKs, prefetch=1 prevents RabbitMQ from reserving several
	// messages ahead of the current one. The Sum EOF coordination relies on this
	// to avoid closing a request before older reserved data is processed.
	if err := m.ch.Qos(queuePrefetchCount, 0, false); err != nil {
		return ErrMessageMiddlewareMessage
	}

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
		return ErrMessageMiddlewareMessage
	}

	m.shouldStopLock.Lock()
	m.shouldStop = false // We set the value here to ensure that calling StartConsuming would restart consumption if it was stopped before
	m.shouldStopLock.Unlock()
	m.callbackGate.Lock()
	m.closing = false
	m.callbackGate.Unlock()

	for msg := range msgs {
		m.callbackGate.Lock()
		if m.closing {
			m.callbackGate.Unlock()
			break
		}
		m.inFlight.Add(1)
		m.callbackGate.Unlock()

		ack := func() {
			_ = msg.Ack(false)
		}
		nack := func() {
			_ = msg.Nack(false, false)
		}
		func() {
			defer m.inFlight.Done()
			callbackFunc(Message{Body: string(msg.Body)}, ack, nack)
		}()

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

func (m *QueueMiddleware) StopConsuming() error {
	m.shouldStopLock.Lock()
	m.shouldStop = true
	m.shouldStopLock.Unlock()

	if m.ch != nil && m.consumerTag != "" {
		_ = m.ch.Cancel(m.consumerTag, false)
	}

	return nil
}

func (m *QueueMiddleware) Send(msg Message) (err error) {
	if m.ch == nil || m.conn == nil {
		return ErrMessageMiddlewareDisconnected
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
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	return nil
}

func (m *QueueMiddleware) Close() error {
	var closeErr error
	_ = m.StopConsuming()
	m.callbackGate.Lock()
	m.closing = true
	m.callbackGate.Unlock()
	m.inFlight.Wait()

	if m.ch != nil {
		if err := m.ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = ErrMessageMiddlewareClose
		}
		m.ch = nil
	}
	if m.conn != nil {
		if err := m.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			if closeErr == nil {
				closeErr = ErrMessageMiddlewareClose
			}
		}
		m.conn = nil
	}

	return closeErr
}

package middleware

import (
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	ch           *amqp.Channel
	conn         *amqp.Connection
	exchangeName string
	routingKeys  []string
	rabbitQueue  amqp.Queue
	consumerTag  string

	shouldStopLock sync.Mutex
	shouldStop     bool
	inFlight       sync.WaitGroup
	callbackGate   sync.Mutex
	closing        bool
}

func NewExchangeMiddleware(hostname string, port int, exchangeName string, routingKeys []string) (*ExchangeMiddleware, error) {
	middlewareExchange := &ExchangeMiddleware{}

	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%d/", hostname, port))
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}
	middlewareExchange.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		_ = middlewareExchange.Close()
		return nil, ErrMessageMiddlewareMessage
	}
	middlewareExchange.ch = ch

	err = ch.ExchangeDeclare(
		exchangeName,
		"direct", // type
		false,    // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		_ = middlewareExchange.Close()
		return nil, ErrMessageMiddlewareMessage
	}
	middlewareExchange.exchangeName = exchangeName
	middlewareExchange.routingKeys = append([]string(nil), routingKeys...)

	return middlewareExchange, nil
}

func (m *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) (err error) {
	if m.rabbitQueue.Name == "" {
		queue, err := m.ch.QueueDeclare(
			"",
			false,
			true,
			true,
			false,
			nil,
		)
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
		m.rabbitQueue = queue

		for _, key := range m.routingKeys {
			err = m.ch.QueueBind(
				queue.Name,
				key,
				m.exchangeName,
				false,
				nil,
			)
			if err != nil {
				return ErrMessageMiddlewareMessage
			}
		}
	}

	m.consumerTag = fmt.Sprintf("exchange-%s-%d", m.rabbitQueue.Name, time.Now().UnixNano())

	msgs, err := m.ch.Consume(
		m.rabbitQueue.Name,
		m.consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	m.shouldStopLock.Lock()
	m.shouldStop = false
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
			break
		}
	}

	return nil
}

func (m *ExchangeMiddleware) StopConsuming() error {
	m.shouldStopLock.Lock()
	m.shouldStop = true
	m.shouldStopLock.Unlock()

	if m.ch != nil && m.consumerTag != "" {
		_ = m.ch.Cancel(m.consumerTag, false)
	}
	m.consumerTag = ""
	m.rabbitQueue = amqp.Queue{}

	return nil
}

func (m *ExchangeMiddleware) Send(msg Message) (err error) {
	if m.ch == nil || m.conn == nil {
		return ErrMessageMiddlewareDisconnected
	}

	routingKeys := m.routingKeys
	if len(routingKeys) == 0 {
		routingKeys = []string{""}
	}

	for _, routingKey := range routingKeys {
		err = m.ch.Publish(
			m.exchangeName,
			routingKey,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			},
		)
		if err != nil {
			if errors.Is(err, amqp.ErrClosed) {
				return ErrMessageMiddlewareDisconnected
			}
			return ErrMessageMiddlewareMessage
		}
	}

	return nil
}

func (m *ExchangeMiddleware) Close() error {
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

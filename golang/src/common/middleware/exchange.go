package middleware

import (
	"errors"
	"fmt"
	"sync"
	"time"

	middleware "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
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
}

func NewExchangeMiddleware(hostname string, port int, exchangeName string, routingKeys []string) (*ExchangeMiddleware, error) {
	middlewareExchange := &ExchangeMiddleware{}

	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%d/", hostname, port))
	if err != nil {
		return nil, middleware.ErrMessageMiddlewareDisconnected
	}
	middlewareExchange.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		_ = middlewareExchange.Close()
		return nil, middleware.ErrMessageMiddlewareMessage
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
		return nil, middleware.ErrMessageMiddlewareMessage
	}
	middlewareExchange.exchangeName = exchangeName
	middlewareExchange.routingKeys = append([]string(nil), routingKeys...)

	queue, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		_ = middlewareExchange.Close()
		return nil, middleware.ErrMessageMiddlewareMessage
	}
	middlewareExchange.rabbitQueue = queue

	for _, key := range routingKeys {
		err = ch.QueueBind(
			queue.Name,
			key,
			exchangeName,
			false,
			nil,
		)
		if err != nil {
			_ = middlewareExchange.Close()
			return nil, middleware.ErrMessageMiddlewareMessage
		}
	}

	return middlewareExchange, nil
}

func (m *ExchangeMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) (err error) {
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
		return middleware.ErrMessageMiddlewareMessage
	}

	m.shouldStopLock.Lock()
	m.shouldStop = false
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
			break
		}
	}

	return nil
}

func (m *ExchangeMiddleware) StopConsuming() {
	m.shouldStopLock.Lock()
	m.shouldStop = true
	m.shouldStopLock.Unlock()

	if m.ch != nil && m.consumerTag != "" {
		_ = m.ch.Cancel(m.consumerTag, false)
	}
}

func (m *ExchangeMiddleware) Send(msg middleware.Message) (err error) {
	if m.ch == nil || m.conn == nil {
		return middleware.ErrMessageMiddlewareDisconnected
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
				return middleware.ErrMessageMiddlewareDisconnected
			}
			return middleware.ErrMessageMiddlewareMessage
		}
	}

	return nil
}

func (m *ExchangeMiddleware) Close() error {
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

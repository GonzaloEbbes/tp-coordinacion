package middleware

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	middleware, err := NewQueueMiddleware(connectionSettings.Hostname, connectionSettings.Port, queueName)
	if err != nil {
		return nil, err
	}
	return middleware, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	middleware, err := NewExchangeMiddleware(connectionSettings.Hostname, connectionSettings.Port, exchange, keys)
	if err != nil {
		return nil, err
	}
	return middleware, nil
}

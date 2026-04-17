package middleware

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	middleware, err := NewQueueMiddleware(connectionSettings.Hostname, connectionSettings.Port, queueName)
	if err != nil {
		return nil, err
	}
	return middleware, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	middleware, err := NewExchangeMiddleware(connectionSettings.Hostname, connectionSettings.Port, exchange, keys)
	if err != nil {
		return nil, err
	}
	return middleware, nil
}

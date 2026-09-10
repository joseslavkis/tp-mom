package factory

import (
	"fmt"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	exchangeName string
	routingKeys  []string
	connection   *amqp.Connection
	publisher    *amqp.Channel

	lifecycleMutex sync.Mutex
	closed         bool
}

func newExchangeMiddleware(exchangeName string, routingKeys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	exchange := &ExchangeMiddleware{
		exchangeName: exchangeName,
		routingKeys:  append([]string(nil), routingKeys...),
	}

	if err := exchange.connect(connectionSettings); err != nil {
		return nil, err
	}

	return exchange, nil
}

func (exchange *ExchangeMiddleware) connect(connectionSettings m.ConnSettings) error {
	url := fmt.Sprintf(
		"amqp://guest:guest@%s:%d/",
		connectionSettings.Hostname,
		connectionSettings.Port,
	)

	connection, err := amqp.Dial(url)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	publisher, err := connection.Channel()
	if err != nil {
		disconnected := connection.IsClosed()
		_ = connection.Close()
		if disconnected {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	err = publisher.ExchangeDeclare(exchange.exchangeName, "direct", false, false, false, false, nil)
	if err != nil {
		disconnected := connection.IsClosed()
		_ = publisher.Close()
		_ = connection.Close()
		if disconnected {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	exchange.connection = connection
	exchange.publisher = publisher
	return nil
}

func (exchange *ExchangeMiddleware) StartConsuming(func(m.Message, func(), func())) error {
	exchange.lifecycleMutex.Lock()
	defer exchange.lifecycleMutex.Unlock()

	if exchange.closed || exchange.connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return m.ErrMessageMiddlewareMessage
}

func (exchange *ExchangeMiddleware) StopConsuming() error {
	exchange.lifecycleMutex.Lock()
	defer exchange.lifecycleMutex.Unlock()

	if exchange.closed || exchange.connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (exchange *ExchangeMiddleware) Send(message m.Message) error {
	exchange.lifecycleMutex.Lock()
	defer exchange.lifecycleMutex.Unlock()

	if exchange.closed || exchange.connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	for _, routingKey := range exchange.routingKeys {
		err := exchange.publisher.Publish(
			exchange.exchangeName,
			routingKey,
			false,
			false,
			amqp.Publishing{Body: []byte(message.Body)},
		)
		if err != nil {
			if exchange.connection.IsClosed() {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}

	return nil
}

func (exchange *ExchangeMiddleware) Close() error {
	exchange.lifecycleMutex.Lock()
	defer exchange.lifecycleMutex.Unlock()

	if exchange.closed {
		return nil
	}
	exchange.closed = true

	var closeFailed bool
	if exchange.publisher != nil && exchange.publisher.Close() != nil {
		closeFailed = true
	}
	if exchange.connection != nil && exchange.connection.Close() != nil {
		closeFailed = true
	}

	if closeFailed {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

package factory

import (
	"fmt"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)


type QueueMiddleware struct {
	queueName  string
	connection *amqp.Connection
	publisher  *amqp.Channel

	mutex     sync.Mutex
	closed bool
}


func newQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	queue := &QueueMiddleware{
		queueName: queueName,
	}

	if err := queue.connect(connectionSettings); err != nil {
		return nil, err
	}

	return queue, nil
}

func (queue *QueueMiddleware) connect(connectionSettings m.ConnSettings) error {
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
		_ = connection.Close()
		return m.ErrMessageMiddlewareMessage
	}

	_, err = publisher.QueueDeclare(queue.queueName, true, false, false, false, nil)
	if err != nil {
		_ = publisher.Close()
		_ = connection.Close()
		return m.ErrMessageMiddlewareMessage
	}

	queue.connection = connection
	queue.publisher = publisher
	return nil
}

func (queue *QueueMiddleware) StartConsuming(func(m.Message, func(), func())) error {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	if queue.closed {
		return m.ErrMessageMiddlewareDisconnected
	}
	return m.ErrMessageMiddlewareMessage
}

func (queue *QueueMiddleware) StopConsuming() error {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	if queue.closed {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (queue *QueueMiddleware) Send(m.Message) error {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	if queue.closed {
		return m.ErrMessageMiddlewareDisconnected
	}
	return m.ErrMessageMiddlewareMessage
}

func (queue *QueueMiddleware) Close() error {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	if queue.closed {
		return nil
	}
	queue.closed = true

	var closeFailed bool
	if queue.publisher != nil && queue.publisher.Close() != nil {
		closeFailed = true
	}
	if queue.connection != nil && queue.connection.Close() != nil {
		closeFailed = true
	}

	if closeFailed {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

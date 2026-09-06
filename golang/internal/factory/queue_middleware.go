package factory

import (
	"fmt"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	queueName        string
	connection       *amqp.Connection
	publisher        *amqp.Channel
	consumer         *amqp.Channel
	consumerTag      string
	consumerSequence uint64
	mutex         sync.Mutex
	consumerMutex sync.Mutex
	closed        bool
	consuming     bool
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

func (queue *QueueMiddleware) StartConsuming(callback func(m.Message, func(), func())) error {
	consumer, deliveries, err := queue.startConsumer()
	if err != nil {
		return err
	}
	defer queue.releaseConsumer(consumer)

	return queue.consumeDeliveries(deliveries, callback)
}

func (queue *QueueMiddleware) startConsumer() (*amqp.Channel, <-chan amqp.Delivery, error) {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	if queue.closed || queue.connection.IsClosed() {
		return nil, nil, m.ErrMessageMiddlewareDisconnected
	}
	if queue.consuming {
		return nil, nil, m.ErrMessageMiddlewareMessage
	}

	consumer, err := queue.connection.Channel()
	if err != nil {
		return nil, nil, queue.consumptionError()
	}

	queue.consumerSequence++
	consumerTag := fmt.Sprintf("consumer-%d", queue.consumerSequence)
	deliveries, err := consumer.Consume(queue.queueName, consumerTag, false, false, false, false, nil)
	if err != nil {
		_ = consumer.Close()
		return nil, nil, queue.consumptionError()
	}

	queue.consumer = consumer
	queue.consumerTag = consumerTag
	queue.consuming = true
	return consumer, deliveries, nil
}

func (queue *QueueMiddleware) releaseConsumer(consumer *amqp.Channel) {
	queue.consumerMutex.Lock()
	_ = consumer.Close()
	queue.consumerMutex.Unlock()

	queue.mutex.Lock()
	defer queue.mutex.Unlock()
	if queue.consumer == consumer {
		queue.consumer = nil
		queue.consumerTag = ""
		queue.consuming = false
	}
}

func (queue *QueueMiddleware) consumeDeliveries(
	deliveries <-chan amqp.Delivery,
	callback func(m.Message, func(), func()),
) error {
	consumerErrors := make(chan error, 1)
	for {
		select {
		case <-consumerErrors:
			return queue.consumptionError()
		case delivery, ok := <-deliveries:
			if !ok {
				return queue.consumptionError()
			}

			queue.handleDelivery(delivery, callback, consumerErrors)
			//select por si llega un mensaje nuevo y aparte el callback falló
			select {
			case <-consumerErrors:
				return queue.consumptionError()
			default:
			}
		}
	}
}

func (queue *QueueMiddleware) handleDelivery(
	delivery amqp.Delivery,
	callback func(m.Message, func(), func()),
	consumerErrors chan<- error,
) {
	var resolution sync.Once
	resolve := func(ack bool) {
		resolution.Do(func() { // por si usan mal el middleware y llaman a los callbacks más de una vez
			queue.consumerMutex.Lock()
			var err error
			if ack {
				err = delivery.Ack(false)
			} else {
				err = delivery.Nack(false, true)
			}
			queue.consumerMutex.Unlock()
		
			if err != nil {
				select {
				case consumerErrors <- err:
				default:
				}
			}
		})
	}

	callback(
		m.Message{Body: string(delivery.Body)},
		func() { resolve(true) },
		func() { resolve(false) },
	)
}

func (queue *QueueMiddleware) consumptionError() error {
	if queue.connection.IsClosed() {
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

func (queue *QueueMiddleware) Send(message m.Message) error {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	if queue.closed || queue.connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	err := queue.publisher.Publish(
		"",
		queue.queueName,
		false,
		false,
		amqp.Publishing{Body: []byte(message.Body)},
	)
	if err != nil {
		if queue.connection.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	return nil
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

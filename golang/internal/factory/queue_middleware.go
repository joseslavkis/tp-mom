package factory

import (
	"fmt"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	queueName          string
	connection         *amqp.Connection
	publisher          *amqp.Channel
	consumer           *amqp.Channel
	consumerTag        string
	consumerSequence   uint64
	mutex              sync.Mutex
	consumerMutex      sync.Mutex
	closed             bool
	consuming          bool
	stopping           bool
	stopRequested      chan struct{}
	consumptionDone    chan struct{}
	stopError          error
	consumerCloseError error
	closeDone          chan struct{}
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

	_, err = publisher.QueueDeclare(
		queue.queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = publisher.Close()
		_ = connection.Close()
		return m.ErrMessageMiddlewareMessage
	}

	queue.connection = connection
	queue.publisher = publisher
	return nil
}

func (queue *QueueMiddleware) StartConsuming(
	callback func(m.Message, func(), func()),
) (result error) {
	consumer, deliveries, err := queue.startConsumer()
	if err != nil {
		return err
	}

	defer func() {
		if err := queue.releaseConsumer(consumer); result == nil && err != nil {
			result = err
		}
	}()

	return queue.consumeDeliveries(consumer, deliveries, callback)
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

	deliveries, err := consumer.Consume(
		queue.queueName,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = consumer.Close()
		return nil, nil, queue.consumptionError()
	}

	queue.consumer = consumer
	queue.consumerTag = consumerTag
	queue.consuming = true
	queue.stopping = false
	queue.stopRequested = make(chan struct{})
	queue.consumptionDone = make(chan struct{})
	queue.stopError = nil
	queue.consumerCloseError = nil

	return consumer, deliveries, nil
}

func (queue *QueueMiddleware) releaseConsumer(consumer *amqp.Channel) error {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	queue.consumerMutex.Lock()
	if !consumer.IsClosed() {
		queue.consumerCloseError = consumer.Close()
	}
	queue.consumerMutex.Unlock()

	if queue.consumer == consumer {
		queue.consumer = nil
		queue.consumerTag = ""
		queue.consuming = false
		close(queue.consumptionDone)
	}

	if queue.consumerCloseError != nil {
		return queue.consumptionError()
	}

	return nil
}

func (queue *QueueMiddleware) consumeDeliveries(
	consumer *amqp.Channel,
	deliveries <-chan amqp.Delivery,
	callback func(m.Message, func(), func()),
) error {
	consumerErrors := make(chan error, 1)

	queue.consumerMutex.Lock()
	consumerClosed := consumer.NotifyClose(make(chan *amqp.Error, 1))
	queue.consumerMutex.Unlock()

	queue.mutex.Lock()
	stopRequested := queue.stopRequested
	queue.mutex.Unlock()

	for {
		if stopped, err := queue.consumptionStatus(); stopped {
			return err
		}

		select {
		case <-stopRequested:
			_, err := queue.consumptionStatus()
			return err

		case <-consumerClosed:
			return queue.consumptionError()

		case <-consumerErrors:
			return queue.consumptionError()

		case delivery, ok := <-deliveries:
			if stopped, err := queue.consumptionStatus(); stopped {
				return err
			}

			if !ok {
				return queue.consumptionError()
			}

			queue.handleDelivery(delivery, callback, consumerErrors)

			// select por si llega un mensaje nuevo y aparte el callback falló
			select {
			case <-consumerErrors:
				return queue.consumptionError()
			default:
			}
		}
	}
}

func (queue *QueueMiddleware) consumptionStatus() (bool, error) {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	if queue.connection.IsClosed() {
		return true, m.ErrMessageMiddlewareDisconnected
	}

	if queue.stopping {
		return true, queue.stopError
	}

	return false, nil
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

	if queue.connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return queue.cancelConsumer()
}

func (queue *QueueMiddleware) cancelConsumer() error {
	if !queue.consuming {
		return nil
	}

	if queue.stopping {
		return queue.stopError
	}

	queue.stopping = true

	queue.consumerMutex.Lock()
	err := queue.consumer.Cancel(queue.consumerTag, false)
	queue.consumerMutex.Unlock()

	if err != nil {
		queue.stopError = queue.consumptionError()
	}

	close(queue.stopRequested)

	return queue.stopError
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
		amqp.Publishing{
			Body: []byte(message.Body),
		},
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

	if queue.closed {
		done := queue.closeDone
		queue.mutex.Unlock()
		<-done
		return nil
	}

	queue.closed = true
	queue.closeDone = make(chan struct{})

	closeFailed := queue.cancelConsumer() != nil
	done := queue.consumptionDone

	queue.mutex.Unlock()

	if done != nil {
		<-done
	}

	queue.mutex.Lock()
	defer queue.mutex.Unlock()
	defer close(queue.closeDone)

	if queue.consumerCloseError != nil {
		closeFailed = true
	}

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
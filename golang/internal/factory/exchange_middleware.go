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

	lifecycleMutex     sync.Mutex
	consumerMutex      sync.Mutex
	closed             bool
	consuming          bool
	stopping           bool
	releasing          bool
	consumer           *exchangeConsumption
	consumerSequence   uint64
	consumerCloseError error
	closeDone          chan struct{}
}

type exchangeConsumption struct {
	channel       *amqp.Channel
	tag           string
	channelClosed <-chan *amqp.Error
	stopRequested chan struct{}
	done          chan struct{}
	failureSignal chan struct{}
	resultMutex   sync.Mutex
	terminalError error
	finalized     bool
	released      bool
}

func (consumption *exchangeConsumption) recordFailure(err error) {
	if err == nil {
		return
	}

	consumption.resultMutex.Lock()
	defer consumption.resultMutex.Unlock()
	if consumption.finalized || consumption.terminalError == m.ErrMessageMiddlewareDisconnected {
		return
	}
	if consumption.terminalError == nil || err == m.ErrMessageMiddlewareDisconnected {
		consumption.terminalError = err
	}
	select {
	case consumption.failureSignal <- struct{}{}:
	default:
	}
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

func (exchange *ExchangeMiddleware) StartConsuming(callback func(m.Message, func(), func())) (result error) {
	if callback == nil {
		return m.ErrMessageMiddlewareMessage
	}

	consumption, deliveries, err := exchange.startConsumer()
	if err != nil {
		return err
	}
	defer func() {
		result = exchange.releaseConsumer(consumption)
	}()

	exchange.consumeDeliveries(consumption, deliveries, callback)
	return nil
}

func (exchange *ExchangeMiddleware) startConsumer() (*exchangeConsumption, <-chan amqp.Delivery, error) {
	exchange.lifecycleMutex.Lock()
	defer exchange.lifecycleMutex.Unlock()

	if exchange.closed || exchange.connection.IsClosed() {
		return nil, nil, m.ErrMessageMiddlewareDisconnected
	}
	if exchange.consuming {
		return nil, nil, m.ErrMessageMiddlewareMessage
	}

	channel, err := exchange.connection.Channel()
	if err != nil {
		return nil, nil, exchange.messageError()
	}

	queue, err := channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		_ = channel.Close()
		return nil, nil, exchange.messageError()
	}
	for _, routingKey := range exchange.routingKeys {
		if err := channel.QueueBind(queue.Name, routingKey, exchange.exchangeName, false, nil); err != nil {
			_ = channel.Close()
			return nil, nil, exchange.messageError()
		}
	}

	exchange.consumerSequence++
	consumerTag := fmt.Sprintf("consumer-%d", exchange.consumerSequence)
	deliveries, err := channel.Consume(queue.Name, consumerTag, false, false, false, false, nil)
	if err != nil {
		_ = channel.Close()
		return nil, nil, exchange.messageError()
	}

	consumption := &exchangeConsumption{
		channel:       channel,
		tag:           consumerTag,
		channelClosed: channel.NotifyClose(make(chan *amqp.Error, 1)),
		stopRequested: make(chan struct{}),
		done:          make(chan struct{}),
		failureSignal: make(chan struct{}, 1),
	}
	exchange.consumer = consumption
	exchange.consuming = true
	exchange.stopping = false
	exchange.releasing = false
	exchange.consumerCloseError = nil
	return consumption, deliveries, nil
}

func (exchange *ExchangeMiddleware) consumeDeliveries(
	consumption *exchangeConsumption,
	deliveries <-chan amqp.Delivery,
	callback func(m.Message, func(), func()),
) {
	for {
		if stopped, err := exchange.consumptionStatus(); stopped {
			consumption.recordFailure(err)
			return
		}

		select {
		case <-consumption.stopRequested:
			_, err := exchange.consumptionStatus()
			consumption.recordFailure(err)
			return
		case <-consumption.channelClosed:
			consumption.recordFailure(exchange.messageError())
			return
		case <-consumption.failureSignal:
			return
		case delivery, ok := <-deliveries:
			if stopped, err := exchange.consumptionStatus(); stopped {
				consumption.recordFailure(err)
				return
			}
			if !ok {
				consumption.recordFailure(exchange.messageError())
				return
			}
			exchange.handleDelivery(consumption, delivery, callback)
			select {
			case <-consumption.failureSignal:
				return
			default:
			}
		}
	}
}

func (exchange *ExchangeMiddleware) handleDelivery(
	consumption *exchangeConsumption,
	delivery amqp.Delivery,
	callback func(m.Message, func(), func()),
) {
	var resolution sync.Once
	resolve := func(ack bool) {
		resolution.Do(func() {
			exchange.consumerMutex.Lock()
			defer exchange.consumerMutex.Unlock()
			if consumption.released {
				return
			}

			var err error
			if ack {
				err = delivery.Ack(false)
			} else {
				err = delivery.Nack(false, true)
			}
			if err != nil {
				consumption.recordFailure(exchange.messageError())
			}
		})
	}

	callback(
		m.Message{Body: string(delivery.Body)},
		func() { resolve(true) },
		func() { resolve(false) },
	)
}

func (exchange *ExchangeMiddleware) consumptionStatus() (bool, error) {
	exchange.lifecycleMutex.Lock()
	defer exchange.lifecycleMutex.Unlock()
	if exchange.connection.IsClosed() {
		return true, m.ErrMessageMiddlewareDisconnected
	}
	if exchange.stopping {
		return true, nil
	}
	return false, nil
}

func (exchange *ExchangeMiddleware) releaseConsumer(consumption *exchangeConsumption) error {
	exchange.lifecycleMutex.Lock()
	exchange.releasing = true
	exchange.lifecycleMutex.Unlock()

	exchange.consumerMutex.Lock()
	var closeError error
	select {
	case <-consumption.channelClosed:
		consumption.recordFailure(exchange.messageError())
	default:
	}
	if consumption.channel.IsClosed() {
		consumption.recordFailure(exchange.messageError())
	} else {
		closeError = consumption.channel.Close()
		if closeError != nil {
			consumption.recordFailure(exchange.messageError())
		}
	}
	consumption.released = true
	exchange.consumerMutex.Unlock()

	exchange.lifecycleMutex.Lock()
	defer exchange.lifecycleMutex.Unlock()
	if exchange.connection.IsClosed() {
		consumption.recordFailure(m.ErrMessageMiddlewareDisconnected)
	}
	consumption.resultMutex.Lock()
	consumption.finalized = true
	result := consumption.terminalError
	consumption.resultMutex.Unlock()

	exchange.consumerCloseError = closeError
	exchange.consumer = nil
	exchange.consuming = false
	exchange.releasing = false
	close(consumption.done)
	return result
}

func (exchange *ExchangeMiddleware) StopConsuming() error {
	exchange.lifecycleMutex.Lock()
	defer exchange.lifecycleMutex.Unlock()

	if exchange.closed || exchange.connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return exchange.cancelConsumer()
}

func (exchange *ExchangeMiddleware) cancelConsumer() error {
	if !exchange.consuming || exchange.stopping || exchange.releasing {
		return nil
	}
	exchange.stopping = true

	exchange.consumerMutex.Lock()
	err := exchange.consumer.channel.Cancel(exchange.consumer.tag, false)
	exchange.consumerMutex.Unlock()
	if err != nil {
		mapped := exchange.messageError()
		exchange.consumer.recordFailure(mapped)
		close(exchange.consumer.stopRequested)
		return mapped
	}
	close(exchange.consumer.stopRequested)
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

	if exchange.closed {
		done := exchange.closeDone
		exchange.lifecycleMutex.Unlock()
		<-done
		return nil
	}
	exchange.closed = true
	exchange.closeDone = make(chan struct{})
	closeFailed := exchange.cancelConsumer() != nil
	var consumptionDone <-chan struct{}
	if exchange.consumer != nil {
		consumptionDone = exchange.consumer.done
	}
	exchange.lifecycleMutex.Unlock()

	if consumptionDone != nil {
		<-consumptionDone
	}

	exchange.lifecycleMutex.Lock()
	defer exchange.lifecycleMutex.Unlock()
	defer close(exchange.closeDone)
	if exchange.consumerCloseError != nil {
		closeFailed = true
	}

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

func (exchange *ExchangeMiddleware) messageError() error {
	if exchange.connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return m.ErrMessageMiddlewareMessage
}

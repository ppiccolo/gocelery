package gocelery

import (
	"encoding/json"
	"time"

	"github.com/streadway/amqp"
)

// AMQPExchange stores AMQP Exchange configuration
type AMQPExchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

// NewAMQPExchange creates new AMQPExchange
func NewAMQPExchange(config AMQPCeleryExchangeConfig) *AMQPExchange {
	return &AMQPExchange{
		Name:       config.Name,
		Type:       config.Type,
		Durable:    config.Durable,
		AutoDelete: config.AutoDelete,
	}
}

// AMQPQueue stores AMQP Queue configuration
type AMQPQueue struct {
	Name       string
	Durable    bool
	AutoDelete bool
}

// NewAMQPQueue creates new AMQPQueue
func NewAMQPQueue(config AMQPCeleryQueueConfig) *AMQPQueue {
	return &AMQPQueue{
		Name:       config.Name,
		Durable:    config.Durable,
		AutoDelete: config.AutoDelete,
	}
}

//AMQPCeleryBroker is RedisBroker for AMQP
type AMQPCeleryBroker struct {
	*amqp.Channel
	Connection       *amqp.Connection
	Exchange         *AMQPExchange
	Queue            *AMQPQueue
	consumingChannel <-chan amqp.Delivery
	rate             int
	ackLate          bool
}

type AMQPCeleryExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

type AMQPCeleryQueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
}

type AMQPCeleryBrokerConfig struct {
	AckLate        bool
	PrefetchCount  int
	Exchange       AMQPCeleryExchangeConfig
	Queue          AMQPCeleryQueueConfig
	CreateExchange bool
	CreateQueue    bool
}

// NewAMQPConnection creates new AMQP channel
func NewAMQPConnection(host string) (*amqp.Connection, *amqp.Channel) {
	connection, err := amqp.Dial(host)
	if err != nil {
		panic(err)
	}

	channel, err := connection.Channel()
	if err != nil {
		panic(err)
	}
	return connection, channel
}

// NewAMQPCeleryBrokerRaw creates new AMQPCeleryBroker using AMQP conn,channel,Exchange, and Queue
func NewAMQPCeleryBroker(conn *amqp.Connection, channel *amqp.Channel, config *AMQPCeleryBrokerConfig) *AMQPCeleryBroker {
	// ensure Exchange is initialized
	broker := &AMQPCeleryBroker{
		Channel:    channel,
		Connection: conn,
		Exchange:   NewAMQPExchange(config.Exchange),
		Queue:      NewAMQPQueue(config.Queue),
		rate:       config.PrefetchCount,
		ackLate:    config.AckLate,
	}
	if config.CreateExchange {
		if err := broker.CreateExchange(); err != nil {
			panic(err)
		}
	}
	if config.CreateQueue {
		if err := broker.CreateQueue(); err != nil {
			panic(err)
		}
	}
	if err := broker.Qos(broker.rate, 0, false); err != nil {
		panic(err)
	}

	return broker
}

// StartConsumingChannel spawns receiving channel on AMQP Queue
func (b *AMQPCeleryBroker) startConsumingChannel() error {
	channel, err := b.Consume(b.Queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	b.consumingChannel = channel
	return nil
}

func (b *AMQPCeleryBroker) GetExchange() string {
	return b.Exchange.Name
}

func (b *AMQPCeleryBroker) GetQueue() string {
	return b.Queue.Name
}

func (b *AMQPCeleryBroker) GetConnection() *amqp.Connection {
	return b.Connection
}

// SendCeleryMessage sends CeleryMessage to broker
func (b *AMQPCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	taskMessage := message.GetTaskMessage()
	//log.Printf("sending task ID %s\n", taskMessage.ID)

	//queueName := "celery"
	//_, err := b.QueueDeclare(
	//	queueName, // name
	//	true,      // durable
	//	false,     // autoDelete
	//	false,     // exclusive
	//	false,     // noWait
	//	nil,       // args
	//)
	//if err != nil {
	//	return err
	//}
	//err = b.ExchangeDeclare(
	//	"default",
	//	"direct",
	//	true,
	//	true,
	//	false,
	//	false,
	//	nil,
	//)
	//if err != nil {
	//	return err
	//}

	resBytes, err := json.Marshal(taskMessage)
	if err != nil {
		return err
	}

	publishMessage := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         resBytes,
	}

	return b.Publish(
		message.Properties.DeliveryInfo.Exchange,
		message.Properties.DeliveryInfo.RoutingKey,
		false,
		false,
		publishMessage,
	)
}

// GetTaskMessage retrieves task message from AMQP Queue
func (b *AMQPCeleryBroker) GetTaskMessage() (*TaskMessage, *amqp.Delivery, error) {

	if b.consumingChannel == nil {
		err := b.startConsumingChannel()
		if err != nil {
			return nil, nil, err
		}
	}

	delivery := <-b.consumingChannel

	if !b.ackLate {
		delivery.Ack(false)
	}

	var taskMessage TaskMessage
	if err := json.Unmarshal(delivery.Body, &taskMessage); err != nil {
		return nil, &delivery, err
	}
	return &taskMessage, &delivery, nil
}

// CreateExchange declares AMQP Exchange with stored configuration
func (b *AMQPCeleryBroker) CreateExchange() error {
	return b.ExchangeDeclare(
		b.Exchange.Name,
		b.Exchange.Type,
		b.Exchange.Durable,
		b.Exchange.AutoDelete,
		false,
		false,
		nil,
	)
}

// CreateQueue declares AMQP Queue with stored configuration
func (b *AMQPCeleryBroker) CreateQueue() error {
	_, err := b.QueueDeclare(
		b.Queue.Name,
		b.Queue.Durable,
		b.Queue.AutoDelete,
		false,
		false,
		nil,
	)
	return err
}

func (b *AMQPCeleryBroker) GetAckLate() bool {
	return b.ackLate
}

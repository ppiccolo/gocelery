package gocelery

import (
	"testing"
)

func getAMQPBroker() *AMQPCeleryBroker {

	connection, channel := NewAMQPConnection("amqp://10.10.40.130:5672")
	conf := &AMQPCeleryBrokerConfig{
		AckLate:       true,
		PrefetchCount: 0,
		Exchange: AMQPCeleryExchangeConfig{
			Name:       "a_test_exchange1",
			Type:       "topic",
			Durable:    true,
			AutoDelete: false,
		},
		Queue: AMQPCeleryQueueConfig{
			Name:       "a_test_queue1",
			AutoDelete: false,
			Durable:    true,
		},
		CreateExchange: true,
		CreateQueue:    true,
	}

	return NewAMQPCeleryBroker(connection, channel, conf)
}

func getCeleryClient() *CeleryClient {
	broker := getAMQPBroker()
	cc, _ := NewCeleryClient(broker, nil, 0)

	return cc
}

func TestClient(t *testing.T) {

	cc := getCeleryClient()

	for i := 0; i < 10; i++ {
		kwargs := map[string]interface{}{"ciao": i}

		_, _ = cc.ApplyAsync("ciao", cc.Broker.GetExchange(), "a_test_item", kwargs, nil)
	}

	_ = cc.Broker.GetConnection().Close()

	println("Ok")
}

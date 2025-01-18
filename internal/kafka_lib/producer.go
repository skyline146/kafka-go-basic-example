package kafka_lib

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer struct {
	producer *kafka.Producer
}

func NewProducer(addr string) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": addr,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{p}, nil
}

func (p *Producer) SendMessage(message, topic string) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Partition: kafka.PartitionAny,
			Topic:     &topic,
		},
		Value: []byte(message),
	}

	kafkaEventCh := make(chan kafka.Event)
	if err := p.producer.Produce(msg, kafkaEventCh); err != nil {
		return fmt.Errorf("failed sending message on produce: %w", err)
	}

	msgEvent := <-kafkaEventCh

	switch msgEvent := msgEvent.(type) {
	case *kafka.Message:
		return nil
	case kafka.Error:
		return fmt.Errorf("failed to send message to topic %s: %w", topic, msgEvent)
	default:
		return errors.New("unknown event type")
	}
}

func (p *Producer) Close() {
	p.producer.Flush(10 * 1000)
	p.producer.Close()
}

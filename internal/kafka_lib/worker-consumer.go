package kafka_lib

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type WorkerConsumer struct {
	id int
	consumer *kafka.Consumer
}

func NewWorkerConsumer(id int, addr, topic, group string) (*WorkerConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": addr,
		"group.id":          group,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	if err = consumer.Subscribe(topic, nil); err != nil {
		return nil, fmt.Errorf("failed when subscribe to topic `%s`: %w", topic, err)
	}

	return &WorkerConsumer{
		id: id,
		consumer: consumer,
	}, nil
}

func (wc *WorkerConsumer) StartWork(exit chan struct{}) {
	defer wc.consumer.Close()
	
	for {
		select {
		case <-exit:
			return

		default:
			kafkaMessage, err := wc.consumer.ReadMessage(time.Millisecond * 100)
			if err != nil {
				if err.(kafka.Error).IsTimeout() {
					// fmt.Printf("Worker consumer #%d was idle for N seconds, shutting down...\n", wc.id)
					// return
					continue
				}

				fmt.Println(err)
				continue
			}

			msg := string(kafkaMessage.Value)

			fmt.Printf("Worker consumer #%d received message `%s` in topic `%s`. Working...\n", wc.id, msg, kafkaMessage.TopicPartition)

			time.Sleep(time.Millisecond * 100 * time.Duration(rand.IntN(10)))

			fmt.Printf("Worker consumer #%d finished processing message `%s`\n", wc.id, msg)
		}
	}
}
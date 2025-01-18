package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"workers-kafka/internal/kafka_lib"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/joho/godotenv"
)

var (
	topic = "test-topic"
	group = "test-group"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Print("failed to load env file, reading from command line...")
	}

	KAFKA_ADDR := os.Getenv("KAFKA_ADDR")
	if KAFKA_ADDR == "" {
		log.Fatal("'KAFKA_ADDR' variable is undefined")
	}

	WORKERS_ENV := os.Getenv("WORKERS")
	if WORKERS_ENV == "" {
		log.Fatal("'WORKERS' variable is undefined")
	}

	WORKERS_COUNT, err := strconv.Atoi(WORKERS_ENV)
	if err != nil {
		log.Fatal("Invalid 'WORKERS' variable format")
	}

	initConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": KAFKA_ADDR,
		"group.id":          "init",
	})
	if err != nil {
		log.Fatal(fmt.Errorf("failed to create init consumer: %w", err))
	}

	metadata, err := initConsumer.GetMetadata(&topic, false, 1000)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to get topic metadata: %w", err))
	}

	if err = initConsumer.Close(); err != nil {
		log.Fatal(fmt.Errorf("failed to shut down init consumer: %w", err))
	}

	numOfPartitions := len(metadata.Topics[topic].Partitions)

	if WORKERS_COUNT > numOfPartitions {
		WORKERS_COUNT = numOfPartitions
	}

	var wg sync.WaitGroup
	wg.Add(WORKERS_COUNT)

	exit := make(chan struct{})

	for i := 1; i <= WORKERS_COUNT; i++ {
		go func() {
			defer wg.Done()

			workerConsumer, err := kafka_lib.NewWorkerConsumer(i, KAFKA_ADDR, topic, group)
			if err != nil {
				log.Fatal(err)
			}

			workerConsumer.StartWork(exit)
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	<-sigChan
	close(exit)

	//test

	fmt.Println("Shutting down... Waiting for goroutines to finish...")

	wg.Wait()
}

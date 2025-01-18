package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"workers-kafka/internal/kafka_lib"

	"github.com/joho/godotenv"
)

var topic = "test-topic"

func main() {
	err := godotenv.Load()
	if err != nil || os.Getenv("KAFKA_ADDR") == "" {
		log.Fatal("set the KAFKA_ADDR env")
	}

	KAFKA_ADDR := os.Getenv("KAFKA_ADDR")

	producer, err := kafka_lib.NewProducer(KAFKA_ADDR)
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	exit := make(chan struct{})

	go func() {
		i := 1
		for {
			select {
			case <-exit:
				return

			default:
				msg := fmt.Sprintf("message #%d", i)
				if err := producer.SendMessage(msg, topic); err != nil {
					log.Fatal(err)
				}

				fmt.Printf("Message `%s` successfully sent\n", msg)
			}

			i++
			time.Sleep(time.Millisecond * 500)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

    //test

	<-sigChan
	close(exit)
}

package handler

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2"
)

func GetData(c *fiber.Ctx) error {
	topic := "comments"
	worker, err := connectConsumer([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}

	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": err.Error(),
		})
		return err
	}
	fmt.Println("Consumer started ")

	// Count how many message processed
	msgCount := 0

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Get signal for finish
	dataCh := make(chan *sarama.ConsumerMessage)
	defer close(dataCh)
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				dataCh <- msg
				fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.Println("Interrupt is detected")
			}
		}
	}()

	msg := <-dataCh

	if err := worker.Close(); err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": err.Error(),
		})
		return err
	}

	var jsonMap map[string]interface{}
	json.Unmarshal([]byte(msg.Value), &jsonMap)

	err = c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"topic":   string(msg.Topic),
		"data":    jsonMap,
	})

	return err

}

type DataKafka struct {
	Topic   string `json:"topic"`
	Message string `json:"message"`
}

func formatData(transaction *sarama.ConsumerMessage) DataKafka {

	formater := DataKafka{
		Topic:   transaction.Topic,
		Message: string(transaction.Value),
	}

	return formater
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create new consumer
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

package router

import (
	"go-kafka-example/worker/handler"

	"github.com/gofiber/fiber/v2"
)

func Route(app fiber.Router) {
	app.Get("/get-data", handler.GetData) //using queues
}

// Start Zookeeper:
//     # Start the ZooKeeper service
//     bin/zookeeper-server-start.sh config/zookeeper.properties

// Start Apache Kafka:
//     # Start the Kafka broker service
//     bin/kafka-server-start.sh config/server.properties

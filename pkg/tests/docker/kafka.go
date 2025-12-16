package docker

import (
	"context"
	"log"
	"sync"

	kafka_test "github.com/testcontainers/testcontainers-go/modules/kafka"
)

func Kafka(ctx context.Context, wg *sync.WaitGroup) (kafkaUrl string, err error) {
	c, err := kafka_test.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		return kafkaUrl, err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("DEBUG: remove container kafka", c.Terminate(context.Background()))
	}()
	brokers, err := c.Brokers(ctx)
	if err != nil {
		return kafkaUrl, err
	}
	return brokers[0], err
}

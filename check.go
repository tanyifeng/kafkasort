package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
)

const addressConnect = "localhost:9092"

func main() {
	var total int
	flag.IntVar(&total, "total", 50, "Total messages number")
	flag.Parse()

	config := sarama.NewConfig()
	config.ChannelBufferSize = 1024
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner

	topics := []string{"id", "name", "continent"}

	for _, topic := range topics {
		client, err := sarama.NewClient([]string{addressConnect}, config)
		if err != nil {
			fmt.Println(err)
			return
		}

		consumer, err := sarama.NewConsumerFromClient(client)
		if err != nil {
			fmt.Println(err)
			return
		}

		partions, err := consumer.Partitions(topic)
		if err != nil {
			fmt.Println(err)
			return
		}

		pSize := len(partions)
		fmt.Printf("topic: %s\n", topic)
		fmt.Printf("partions: %s\n", pSize)
		fmt.Printf("data: \n")
		for _, partion := range partions {
			pc, err := consumer.ConsumePartition(topic, partion, sarama.OffsetOldest)
			if err != nil {
				fmt.Println(err)
				return
			}
			for i := 0; i < total; i++ {
				msg := <- pc.Messages()
				fmt.Println(string(msg.Value))
			}
		}
	}
}

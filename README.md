# go-confluent-kafka

A library provides consumer/producer to work with kafka, avro and schema registry in confluent

## Producer

```
package main

import (
	"flag"
	"fmt"
	"gitlab.com/ihsanul14/go-confluent-kafka"
	"time"
)

var kafkaServers = []string{"localhost:9092"}
var schemaRegistryServers = []string{"http://localhost:8081"}
var topic = "test"

func main() {
	var n int
	schema := `{
				"type": "record",
				"name": "Example",
				"fields": [
					{"name": "Id", "type": "string"},
					{"name": "Type", "type": "string"},
					{"name": "Data", "type": "string"}
				]
			}`
	producer, err := kafka.NewAvroProducer(kafkaServers, schemaRegistryServers)
	if err != nil {
		fmt.Printf("Could not create avro producer: %s", err)
	}
	flag.IntVar(&n, "n", 1, "number")
	flag.Parse()
	for i := 0; i < n; i++ {
		fmt.Println(i)
		addMsg(producer, schema)
	}
}

func addMsg(producer *kafka.AvroProducer, schema string) {
	value := `{
		"Id": "1",
		"Type": "example_type",
		"Data": "example_data"
	}`
	key := time.Now().String()
	err := producer.Add(topic, schema, []byte(value))
	fmt.Println(key)
	if err != nil {
		fmt.Printf("Could not add a msg: %s", err)
	}
}
```

## Consumer
```
package main

import (
	"fmt"
	"gitlab.com/ihsanul14/go-confluent-kafka"
	"github.com/bsm/sarama-cluster"
)

var kafkaServers = []string{"localhost:9092"}
var schemaRegistryServers = []string{"http://localhost:8081"}
var topic = "test"

func main() {
	consumerCallbacks := kafka.ConsumerCallbacks{
		OnDataReceived: func(msg kafka.Message) {
			fmt.Println(msg)
		},
		OnError: func(err error) {
			fmt.Println("Consumer error", err)
		},
		OnNotification: func(notification *cluster.Notification) {
			fmt.Println(notification)
		},
	}

	consumer, err := kafka.NewAvroConsumer(kafkaServers, schemaRegistryServers, topic, "consumer-group", consumerCallbacks)
	if err != nil {
		fmt.Println(err)
	}
	consumer.Consume()
}
```

### References

* Kafka [dangkaka](https://github.com/dangkaka/go-kafka-avro)
* Kafka [sarama](https://github.com/Shopify/sarama)
* Encodes and decodes Avro data [goavro](https://github.com/linkedin/goavro)
* Consumer group [sarama-cluster](https://github.com/bsm/sarama-cluster)
* [schema-registry](https://github.com/confluentinc/schema-registry)
* gitlab.com/mfahry/go-confluent-kafka


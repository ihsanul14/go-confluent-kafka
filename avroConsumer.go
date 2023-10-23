package kafka

import (
	"encoding/binary"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/linkedin/goavro"
)

type AvroConsumerConfig struct {
	KafkaServers          []string
	SchemaRegistryServers []string
	Topic                 string
	GroupId               string
	Callbacks             ConsumerCallbacks
	SASL                  *SASLConfig
}

type avroConsumer struct {
	Consumer             *cluster.Consumer
	SchemaRegistryClient *CachedSchemaRegistryClient
	callbacks            ConsumerCallbacks
}

type ConsumerCallbacks struct {
	OnDataReceived func(msg Message)
	OnError        func(err error)
	OnNotification func(notification *cluster.Notification)
}

type Message struct {
	SchemaId  int
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     string
}

// avroConsumer is a basic consumer to interact with schema registry, avro and kafka
func NewAvroConsumer(cfg AvroConsumerConfig) (*avroConsumer, error) {
	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	//read from beginning at the first time
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	topics := []string{cfg.Topic}
	consumer, err := cluster.NewConsumer(cfg.KafkaServers, cfg.GroupId, topics, config)
	if err != nil {
		return nil, err
	}

	schemaRegistryClient := NewCachedSchemaRegistryClient(cfg.SchemaRegistryServers, cfg.SASL)
	return &avroConsumer{
		consumer,
		schemaRegistryClient,
		cfg.Callbacks,
	}, nil
}

// GetSchemaId get schema id from schema-registry service
func (ac *avroConsumer) GetSchema(id int) (*goavro.Codec, error) {
	codec, err := ac.SchemaRegistryClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	return codec, nil
}

func (ac *avroConsumer) Consume() {
	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range ac.Consumer.Errors() {
			if ac.callbacks.OnError != nil {
				ac.callbacks.OnError(err)
			}
		}
	}()

	// consume notifications
	go func() {
		for notification := range ac.Consumer.Notifications() {
			if ac.callbacks.OnNotification != nil {
				ac.callbacks.OnNotification(notification)
			}
		}
	}()

	for {
		select {
		case m, ok := <-ac.Consumer.Messages():
			if ok {
				msg, err := ac.ProcessAvroMsg(m)
				if err != nil {
					ac.callbacks.OnError(err)
				}
				ac.Consumer.MarkOffset(m, "")
				if ac.callbacks.OnDataReceived != nil {
					ac.callbacks.OnDataReceived(msg)
				}
			}
		case <-signals:
			return
		}
	}
}

func (ac *avroConsumer) ProcessAvroMsg(m *sarama.ConsumerMessage) (Message, error) {
	schemaId := binary.BigEndian.Uint32(m.Value[1:5])
	codec, err := ac.GetSchema(int(schemaId))
	if err != nil {
		return Message{}, err
	}
	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(m.Value[5:])
	if err != nil {
		return Message{}, err
	}

	// Convert native Go form to textual Avro data
	textual, err := codec.TextualFromNative(nil, native)

	if err != nil {
		return Message{}, err
	}
	msg := Message{int(schemaId), m.Topic, m.Partition, m.Offset, string(m.Key), string(textual)}
	return msg, nil
}

func (ac *avroConsumer) Close() {
	ac.Consumer.Close()
}

package kafka

import (
	"crypto/tls"
	"encoding/binary"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

type AvroProducerConfig struct {
	KafkaServers          []string
	SchemaRegistryServers []string
	SASL                  *SASLConfig
}

type AvroProducer struct {
	producer             sarama.SyncProducer
	schemaRegistryClient *CachedSchemaRegistryClient
	SASL                 *SASLConfig
}

type SASLConfig struct {
	Username  string
	Password  string
	TLSConfig *tls.Config
}

// NewAvroProducer is a basic producer to interact with schema registry, avro and kafka
func NewAvroProducer(cfg AvroProducerConfig) (*AvroProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	if cfg.SASL != nil {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = cfg.SASL.Username
		config.Net.SASL.Password = cfg.SASL.Password
		config.Net.SASL.Handshake = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = cfg.SASL.TLSConfig
	}
	producer, err := sarama.NewSyncProducer(cfg.KafkaServers, config)
	if err != nil {
		return nil, err
	}
	schemaRegistryClient := NewCachedSchemaRegistryClient(cfg.SchemaRegistryServers, cfg.SASL)
	return &AvroProducer{producer, schemaRegistryClient, cfg.SASL}, nil
}

// GetSchemaId get schema id from schema-registry service
func (ap *AvroProducer) GetSchemaId(topic string, avroCodec *goavro.Codec) (int, error) {
	schemaId, err := ap.schemaRegistryClient.CreateSubject(topic, avroCodec)
	if err != nil {
		return 0, err
	}
	return schemaId, nil
}

func (ap *AvroProducer) Add(topic string, schema string, value []byte) error {
	avroCodec, err := goavro.NewCodec(schema)
	if err != nil {
		return err
	}
	schemaId, err := ap.GetSchemaId(topic, avroCodec)
	if err != nil {
		return err
	}
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(schemaId))

	native, _, err := avroCodec.NativeFromTextual(value)
	if err != nil {
		return err
	}

	// Convert native Go form to binary Avro data
	binaryValue, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		return err
	}

	var binaryMsg []byte
	// first byte is magic byte, always 0 for now
	binaryMsg = append(binaryMsg, byte(0))
	//4-byte schema ID as returned by the Schema Registry
	binaryMsg = append(binaryMsg, binarySchemaId...)
	//avro serialized data in Avro’s binary encoding
	binaryMsg = append(binaryMsg, binaryValue...)

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(binaryMsg),
	}
	_, _, err = ap.producer.SendMessage(msg)
	return err
}

func (ac *AvroProducer) Close() {
	ac.producer.Close()
}

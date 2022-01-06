package kafka

import (
	"crypto/tls"
	"encoding/binary"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

type AvroProducerPlainSASL struct {
	producer             sarama.SyncProducer
	schemaRegistryClient *CachedSchemaRegistryClientPlainSASL
}

// NewAvroProducerPlainSASL is a basic producer to interact with schema registry, avro and kafka
func NewAvroProducerPlainSASL(kafkaServers []string, schemaRegistryServers []string, Username string, Password string, tlsConfig *tls.Config) (*AvroProducerPlainSASL, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Net.SASL.Enable = true
	config.Net.SASL.User = Username
	config.Net.SASL.Password = Password
	config.Net.SASL.Handshake = true
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig
	producer, err := sarama.NewSyncProducer(kafkaServers, config)
	if err != nil {
		return nil, err
	}
	schemaRegistryClient := NewCachedSchemaRegistryClientPlainSASL(schemaRegistryServers, Username, Password)
	return &AvroProducerPlainSASL{producer, schemaRegistryClient}, nil
}

//GetSchemaId get schema id from schema-registry service
func (ap *AvroProducerPlainSASL) GetSchemaId(topic string, avroCodec *goavro.Codec) (int, error) {
	schemaId, err := ap.schemaRegistryClient.CreateSubject(topic, avroCodec)
	if err != nil {
		return 0, err
	}
	return schemaId, nil
}

func (ap *AvroProducerPlainSASL) Add(topic string, schema string, value []byte) error {
	avroCodec, err := goavro.NewCodec(schema)
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

func (ac *AvroProducerPlainSASL) Close() {
	ac.producer.Close()
}

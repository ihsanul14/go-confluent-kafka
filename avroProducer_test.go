package kafka

import (
	"testing"

	"github.com/Shopify/sarama/mocks"
)

func TestAvroProducer_Add(t *testing.T) {
	producerMock := mocks.NewSyncProducer(t, nil)
	producerMock.ExpectSendMessageAndSucceed()
	saslConfig := &SASLConfig{
		Username: "test",
	}
	schemaRegistryTestObject := createSchemaRegistryTestObject(t, "test", 1)
	schemaRegistryMock := NewCachedSchemaRegistryClient([]string{schemaRegistryTestObject.MockServer.URL}, saslConfig)

	avroProducer := &AvroProducer{producerMock, schemaRegistryMock, saslConfig}
	defer avroProducer.Close()
	err := avroProducer.Add("test", schemaRegistryTestObject.Codec.Schema(), []byte(`{"val":1}`))
	if nil != err {
		t.Errorf("Error adding msg: %v", err)
	}
}

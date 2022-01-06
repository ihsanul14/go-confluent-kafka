package kafka

import (
	"testing"

	"github.com/Shopify/sarama/mocks"
)

func TestAvroProducerPlainSASL_Add(t *testing.T) {
	producerMock := mocks.NewSyncProducer(t, nil)
	producerMock.ExpectSendMessageAndSucceed()
	schemaRegistryTestObject := createSchemaRegistryTestObject(t, "test", 1)
	schemaRegistryMock := NewCachedSchemaRegistryClientPlainSASL([]string{schemaRegistryTestObject.MockServer.URL}, "", "")
	avroProducerPlainSASL := &AvroProducerPlainSASL{producerMock, schemaRegistryMock}
	defer avroProducerPlainSASL.Close()
	err := avroProducerPlainSASL.Add("test", schemaRegistryTestObject.Codec.Schema(), []byte(`{"val":1}`))
	if nil != err {
		t.Errorf("Error adding msg: %v", err)
	}
}

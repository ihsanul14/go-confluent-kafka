package kafka

import (
	"sync"

	"github.com/linkedin/goavro"
)

// CachedSchemaRegistryClient is a schema registry client that will cache some data to improve performance
type CachedSchemaRegistryClientPlainSASL struct {
	SchemaRegistryClient *SchemaRegistryClientPlainSASL
	schemaCache          map[int]*goavro.Codec
	schemaCacheLock      sync.RWMutex
	schemaIdCache        map[string]int
	schemaIdCacheLock    sync.RWMutex
}

func NewCachedSchemaRegistryClientPlainSASL(connect []string, u string, p string) *CachedSchemaRegistryClientPlainSASL {
	SchemaRegistryClient := NewSchemaRegistryClientPlainSASL(connect, u, p)
	return &CachedSchemaRegistryClientPlainSASL{SchemaRegistryClient: SchemaRegistryClient, schemaCache: make(map[int]*goavro.Codec), schemaIdCache: make(map[string]int)}
}

func NewCachedSchemaRegistryClientWithRetriesPlainSASL(connect []string, retries int, u string, p string) *CachedSchemaRegistryClientPlainSASL {
	SchemaRegistryClient := NewSchemaRegistryClientWithRetriesPlainSASL(connect, retries, u, p)
	return &CachedSchemaRegistryClientPlainSASL{SchemaRegistryClient: SchemaRegistryClient, schemaCache: make(map[int]*goavro.Codec), schemaIdCache: make(map[string]int)}
}

// GetSchema will return and cache the codec with the given id
func (client *CachedSchemaRegistryClientPlainSASL) GetSchema(id int) (*goavro.Codec, error) {
	client.schemaCacheLock.RLock()
	cachedResult := client.schemaCache[id]
	client.schemaCacheLock.RUnlock()
	if nil != cachedResult {
		return cachedResult, nil
	}
	codec, err := client.SchemaRegistryClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	client.schemaCacheLock.Lock()
	client.schemaCache[id] = codec
	client.schemaCacheLock.Unlock()
	return codec, nil
}

// GetSubjects returns a list of subjects
func (client *CachedSchemaRegistryClientPlainSASL) GetSubjects() ([]string, error) {
	return client.SchemaRegistryClient.GetSubjects()
}

// GetVersions returns a list of all versions of a subject
func (client *CachedSchemaRegistryClientPlainSASL) GetVersions(subject string) ([]int, error) {
	return client.SchemaRegistryClient.GetVersions(subject)
}

// GetSchemaByVersion returns the codec for a specific version of a subject
func (client *CachedSchemaRegistryClientPlainSASL) GetSchemaByVersion(subject string, version int) (*goavro.Codec, error) {
	return client.SchemaRegistryClient.GetSchemaByVersion(subject, version)
}

// GetLatestSchema returns the highest version schema for a subject
func (client *CachedSchemaRegistryClientPlainSASL) GetLatestSchema(subject string) (*goavro.Codec, error) {
	return client.SchemaRegistryClient.GetLatestSchema(subject)
}

// CreateSubject will return and cache the id with the given codec
func (client *CachedSchemaRegistryClientPlainSASL) CreateSubject(subject string, codec *goavro.Codec) (int, error) {
	schemaJson := codec.Schema()
	client.schemaIdCacheLock.RLock()
	cachedResult, found := client.schemaIdCache[schemaJson]
	client.schemaIdCacheLock.RUnlock()
	if found {
		return cachedResult, nil
	}
	id, err := client.SchemaRegistryClient.CreateSubject(subject, codec)
	if err != nil {
		return 0, err
	}
	client.schemaIdCacheLock.Lock()
	client.schemaIdCache[schemaJson] = id
	client.schemaIdCacheLock.Unlock()
	return id, nil
}

// IsSchemaRegistered checks if a specific codec is already registered to a subject
func (client *CachedSchemaRegistryClientPlainSASL) IsSchemaRegistered(subject string, codec *goavro.Codec) (int, error) {
	return client.SchemaRegistryClient.IsSchemaRegistered(subject, codec)
}

// DeleteSubject deletes the subject, should only be used in development
func (client *CachedSchemaRegistryClientPlainSASL) DeleteSubject(subject string) error {
	return client.SchemaRegistryClient.DeleteSubject(subject)
}

// DeleteVersion deletes the a specific version of a subject, should only be used in development.
func (client *CachedSchemaRegistryClientPlainSASL) DeleteVersion(subject string, version int) error {
	return client.SchemaRegistryClient.DeleteVersion(subject, version)
}

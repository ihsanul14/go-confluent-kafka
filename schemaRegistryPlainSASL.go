package kafka

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"

	"github.com/linkedin/goavro"
)

// SchemaRegistryClientInterface defines the api for all clients interfacing with schema registry
type SchemaRegistryPlainSASLClientInterface interface {
	GetSchema(int) (*goavro.Codec, error)
	GetSubjects() ([]string, error)
	GetVersions(string) ([]int, error)
	GetSchemaByVersion(string, int) (*goavro.Codec, error)
	GetLatestSchema(string) (*goavro.Codec, error)
	CreateSubject(string, *goavro.Codec) (int, error)
	IsSchemaRegistered(string, *goavro.Codec) (int, error)
	DeleteSubject(string) error
	DeleteVersion(string, int) error
}

// SchemaRegistryClient is a basic http client to interact with schema registry
type SchemaRegistryClientPlainSASL struct {
	SchemaRegistryConnect []string
	httpClient            *http.Client
	retries               int
	Username              string
	Password              string
}

// NewSchemaRegistryClient creates a client to talk with the schema registry at the connect string
// By default it will retry failed requests (5XX responses and http errors) len(connect) number of times
func NewSchemaRegistryClientPlainSASL(connect []string, u string, p string) *SchemaRegistryClientPlainSASL {
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{
		Timeout:   timeout,
		Transport: tr,
	}
	return &SchemaRegistryClientPlainSASL{connect, client, len(connect), u, p}
}

// NewSchemaRegistryClientWithRetries creates an http client with a configurable amount of retries on 5XX responses
func NewSchemaRegistryClientWithRetriesPlainSASL(connect []string, retries int, u string, p string) *SchemaRegistryClientPlainSASL {
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{
		Timeout:   timeout,
		Transport: tr,
	}
	return &SchemaRegistryClientPlainSASL{connect, client, retries, u, p}
}

// GetSchema returns a goavro.Codec by unique id
func (client *SchemaRegistryClientPlainSASL) GetSchema(id int) (*goavro.Codec, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(schemaByID, id), nil)
	if nil != err {
		return nil, err
	}
	schema, err := parseSchema(resp)
	if nil != err {
		return nil, err
	}
	return goavro.NewCodec(schema.Schema)
}

// GetSubjects returns a list of all subjects in the schema registry
func (client *SchemaRegistryClientPlainSASL) GetSubjects() ([]string, error) {
	resp, err := client.httpCall("GET", subjects, nil)
	if nil != err {
		return []string{}, err
	}
	var result = []string{}
	err = json.Unmarshal(resp, &result)
	return result, err
}

// GetVersions returns a list of the versions of a subject
func (client *SchemaRegistryClientPlainSASL) GetVersions(subject string) ([]int, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(subjectVersions, subject), nil)
	if nil != err {
		return []int{}, err
	}
	var result = []int{}
	err = json.Unmarshal(resp, &result)
	return result, err
}

func (client *SchemaRegistryClientPlainSASL) getSchemaByVersionInternal(subject string, version string) (*goavro.Codec, error) {
	resp, err := client.httpCall("GET", fmt.Sprintf(subjectByVersion, subject, version), nil)
	if nil != err {
		return nil, err
	}
	var schema = new(schemaVersionResponse)
	err = json.Unmarshal(resp, &schema)
	if nil != err {
		return nil, err
	}

	return goavro.NewCodec(schema.Schema)
}

// GetSchemaByVersion returns a goavro.Codec for the version of the subject
func (client *SchemaRegistryClientPlainSASL) GetSchemaByVersion(subject string, version int) (*goavro.Codec, error) {
	return client.getSchemaByVersionInternal(subject, fmt.Sprintf("%d", version))
}

// GetLatestSchema returns a goavro.Codec for the latest version of the subject
func (client *SchemaRegistryClientPlainSASL) GetLatestSchema(subject string) (*goavro.Codec, error) {
	return client.getSchemaByVersionInternal(subject, latestVersion)
}

// CreateSubject adds a schema to the subject
func (client *SchemaRegistryClientPlainSASL) CreateSubject(subject string, codec *goavro.Codec) (int, error) {
	schema := schemaResponse{codec.Schema()}
	json, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}
	payload := bytes.NewBuffer(json)
	resp, err := client.httpCall("POST", fmt.Sprintf(subjectVersions, subject), payload)
	if err != nil {
		return 0, err
	}
	return parseID(resp)
}

// IsSchemaRegistered tests if the schema is registered, if so it returns the unique id of that schema
func (client *SchemaRegistryClientPlainSASL) IsSchemaRegistered(subject string, codec *goavro.Codec) (int, error) {
	schema := schemaResponse{codec.Schema()}
	json, err := json.Marshal(schema)
	if err != nil {
		return 0, err
	}
	payload := bytes.NewBuffer(json)
	resp, err := client.httpCall("POST", fmt.Sprintf(deleteSubject, subject), payload)
	if err != nil {
		return 0, err
	}
	return parseID(resp)
}

// DeleteSubject deletes a subject. It should only be used in development
func (client *SchemaRegistryClientPlainSASL) DeleteSubject(subject string) error {
	_, err := client.httpCall("DELETE", fmt.Sprintf(deleteSubject, subject), nil)
	return err
}

// DeleteVersion deletes a subject. It should only be used in development
func (client *SchemaRegistryClientPlainSASL) DeleteVersion(subject string, version int) error {
	_, err := client.httpCall("DELETE", fmt.Sprintf(subjectByVersion, subject, fmt.Sprintf("%d", version)), nil)
	return err
}

func (client *SchemaRegistryClientPlainSASL) httpCall(method, uri string, payload io.Reader) ([]byte, error) {
	nServers := len(client.SchemaRegistryConnect)
	offset := rand.Intn(nServers)
	for i := 0; ; i++ {
		url := fmt.Sprintf("%s%s", client.SchemaRegistryConnect[(i+offset)%nServers], uri)
		req, err := http.NewRequest(method, url, payload)
		req.SetBasicAuth(client.Username, client.Password)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", contentType)
		resp, err := client.httpClient.Do(req)
		if resp != nil {
			defer resp.Body.Close()
		}
		if i < client.retries && (err != nil || retriable(resp)) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !okStatus(resp) {
			return nil, newError(resp)
		}
		return ioutil.ReadAll(resp.Body)
	}
}

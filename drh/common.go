package drh

import (
	"encoding/json"
	"log"
)

var (
	// KB is 1024 Bytes
	KB int = 1 << 10

	// MB is 1024 KB
	MB int = 1 << 20
)

// Source is an interface represents a type of cloud storage services
// type Source interface {
// 	GetEndpointURL()
// }

// Job is ...
type Job interface {
	Run()
}

// Client is an interface used to contact with Cloud Storage Services
type Client interface {
	// GET
	ListObjects(continuationToken, prefix *string, maxKeys int32) ([]*Object, error)
	HeadObject(key string)
	GetObject(key string, size, start, chunkSize int64, version string) ([]byte, error)
	ListCommonPrefixes(depth int, maxKeys int32) (prefixes []*string)

	// PUT
	PutObject(key string, body []byte, storageClass string) (etag *string, err error)
	CreateMultipartUpload(key string) (uploadID *string, err error)
	CompleteMultipartUpload(key string, uploadID *string, parts []*Part) (etag *string, err error)
	UploadPart(key string, uploadID *string, body []byte, partNumber int) (etag *string, err error)
	ListParts(key, uploadID string)
	ListMultipartUploads(key string)
	AbortMultipartUpload(key string, uploadID *string)
}

// Object represents an object to be replicated.
type Object struct {
	Key  string
	Size int64
	// StorageClass string
}

// Helper function to convert Object into Json string
func (o *Object) toString() *string {
	// log.Printf("Convert %v to string", o)

	obj, err := json.Marshal(*o)

	if err != nil {
		log.Printf("Unable to convert object to json string - %s", err)
		return nil
	}

	str := string(obj)
	// msgStr := fmt.Sprintf(`{"key":"%s","size":%d,"version":"%s"}`, m.Key, m.Size, m.Version)
	return &str
}

// Helper function to create Object base on Json string
func newObject(str string) (o *Object) {

	o = new(Object)
	err := json.Unmarshal([]byte(str), o)

	if err != nil {
		log.Printf("Unable to convert string to object - %s", err.Error())
		return nil
	}
	// log.Printf("Key: %s, Size: %d\n", m.Key, m.Size)
	return
}

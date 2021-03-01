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

// Object represents an object to be replicated.
type Object struct {
	Key  string
	Size int64
	// StorageClass string
}

// Metadata info of object
type Metadata struct {
	ContentType string
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

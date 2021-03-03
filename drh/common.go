package drh

import (
	"encoding/json"
	"log"
	"net/url"
	"strconv"
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
	Key       string `json:"key"`
	Size      int64  `json:"size"`
	Sequencer string `json:"sequencer,omitempty"`
}

// S3Event represents a basic structure of a S3 Event Message
// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html for more details
type S3Event struct {
	Records []struct {
		EventSource, AwsRegion, EventTime, EventName string
		S3                                           struct {
			Object `json:"object"`
		}
	}
}

// Part represents a part for multipart upload
type Part struct {
	partNumber int
	etag       *string
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
func newObject(str *string) (o *Object) {

	o = new(Object)
	err := json.Unmarshal([]byte(*str), o)

	if err != nil {
		log.Printf("Unable to convert string to object - %s", err.Error())
		return nil
	}
	// log.Printf("Key: %s, Size: %d\n", m.Key, m.Size)
	return
}

// Helper function to create S3Event base on Json string
func newS3Event(str *string) (e *S3Event) {

	e = new(S3Event)
	err := json.Unmarshal([]byte(*str), e)

	if err != nil {
		log.Printf("Unable to convert string to S3 Event - %s", err.Error())
		return nil
	}
	// log.Printf("Key: %s, Size: %d\n", m.Key, m.Size)
	return
}

func getHex(str *string) int64 {
	i64, _ := strconv.ParseInt(*str, 16, 64)
	return i64
}

func unescape(str *string) string {
	output, _ := url.QueryUnescape(*str)
	return output
}

func escape(str *string) string {
	return url.QueryEscape(*str)
}

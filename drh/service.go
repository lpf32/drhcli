package drh

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Metadata info of object
type Metadata struct {
	ContentType string
}

// Item holds info about the items to be stored in DynamoDB
type Item struct {
	ObjectKey, StorageClass, DesBucket, DesKey, Status, Version string
	Size                                                        int32
	StartTime, EndTime                                          int
	ExtraInfo                                                   Metadata
}

// DBService is a wrapper service used to interact with Amazon DynamoDB
type DBService struct {
	tableName string
	client    *dynamodb.Client
	ctx       context.Context
}

// NewDBService is a ...
func NewDBService(ctx context.Context, tableName string) (*DBService, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalln("unable to load SDK config, " + err.Error())
		return nil, err
	}

	// Create an Amazon DynamoDB client.
	client := dynamodb.NewFromConfig(cfg)

	return &DBService{
		tableName: tableName,
		client:    client,
		ctx:       ctx,
	}, nil
}

// SqsService is a wrapper service used to interact with Amazon SQS
type SqsService struct {
	queueName, queueURL string
	client              *sqs.Client
	ctx                 context.Context
}

// NewSqsService is helper function used to create a SqsService instance
func NewSqsService(ctx context.Context, queueName string) (*SqsService, error) {

	// Only to use default config here
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal("configuration error, " + err.Error())
		return nil, err
	}

	client := sqs.NewFromConfig(cfg)
	input := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}

	result, err := client.GetQueueUrl(ctx, input)
	if err != nil {
		fmt.Println("Got an error getting the queue URL:")
		fmt.Println(err.Error())
		return nil, err
	}

	queueURL := *result.QueueUrl
	log.Println("Queue URL: " + queueURL)

	SqsService := SqsService{
		queueName: queueName,
		queueURL:  queueURL,
		client:    client,
		ctx:       ctx,
	}

	return &SqsService, nil
}

// Message is Default message format
type Message struct {
	key, version string
	size         int64
}

func (m *Message) toString() *string {
	// msg, err := json.Marshal(*m)

	// var msgStr string
	// if err != nil {
	// 	fmt.Printf("Error: %s", err)
	// 	msgStr = ""
	// }
	// msgStr = string(msg)

	msgStr := fmt.Sprintf("{\"key\":\"%s\",\"size\":%d,\"version\":\"%s\"}", m.key, m.size, m.version)
	return &msgStr
}

// SendMessage function sends 1 message at a time to the Queue
func (ss *SqsService) SendMessage(msg Message) {
	// log.Printf("Send Message to Queue")

	input := &sqs.SendMessageInput{
		QueueUrl:    &ss.queueURL,
		MessageBody: msg.toString(),
	}

	resp, err := ss.client.SendMessage(ss.ctx, input)
	if err != nil {
		fmt.Printf("Error: %s", err)
	}

	fmt.Println("Sent message with ID: " + *resp.MessageId)

}

// SendMessageInBatch function sends messages to the Queue in batch.
// Each batch can only contains up to 10 messages
func (ss *SqsService) SendMessageInBatch(msgs []Message) {
	log.Printf("Send Message to Queue in batch")

	entries := make([]types.SendMessageBatchRequestEntry, len(msgs), len(msgs))

	for id, msg := range msgs {
		idstr := strconv.Itoa(id)
		fmt.Printf("Id is %s\n", idstr)
		entry := types.SendMessageBatchRequestEntry{
			Id:          &idstr,
			MessageBody: msg.toString(),
		}
		entries[id] = entry
	}

	input := &sqs.SendMessageBatchInput{
		QueueUrl: &ss.queueURL,
		Entries:  entries,
	}

	resp, err := ss.client.SendMessageBatch(ss.ctx, input)

	if err != nil {
		log.Fatalf(err.Error())
	}

	fmt.Printf("Sent %d messages successfully\n", len(resp.Successful))
}

// ReceiveMessages function receives many messages in batch from the Queue
func (ss *SqsService) ReceiveMessages() {

}

// DeleteMessage function is used to delete message from the Queue
// Returns True if message is deleted successfully
func (ss *SqsService) DeleteMessage() (ok bool) {
	return true
}

// IsQueueEmpty is a function to check if the Queue is empty or not
func (ss *SqsService) IsQueueEmpty() bool {
	return true
}

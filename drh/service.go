package drh

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtype "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

// Item holds info about the items to be stored in DynamoDB
type Item struct {
	ObjectKey, JobStatus, Etag, UploadID string
	Size, StartTimestamp, EndTimestamp   int64
	StartTime, EndTime                   string
	// ExtraInfo               Metadata
}

// DBService is a wrapper service used to interact with Amazon DynamoDB
type DBService struct {
	tableName string
	client    *dynamodb.Client
}

// SqsService is a wrapper service used to interact with Amazon SQS
type SqsService struct {
	queueName, queueURL string
	client              *sqs.Client
}

// SsmService is a wrapper service used to interact with Amazon SSM
type SsmService struct {
	client *ssm.Client
}

// Message is Default message (body) format in SQS
// Assume Message can have different format comparing with drh.Object
// type Message struct {
// 	Key, Version string
// 	Size         int64
// }

// // Helper function to convert Message into Json string
// func (m *Message) toString() *string {

// 	msg, err := json.Marshal(*m)

// 	var msgStr string
// 	if err != nil {
// 		fmt.Printf("Error: %s", err)
// 		msgStr = ""
// 	}
// 	msgStr = string(msg)

// 	// msgStr := fmt.Sprintf(`{"key":"%s","size":%d,"version":"%s"}`, m.Key, m.Size, m.Version)
// 	return &msgStr
// }

// // Helper function to create message base on Json string
// func newMessage(str string) (m *Message) {

// 	m = new(Message)
// 	err := json.Unmarshal([]byte(str), m)

// 	if err != nil {
// 		log.Fatalf("Unalbe to convert string to message - %s", err.Error())
// 		return nil
// 	}
// 	// log.Printf("Key: %s, Size: %d\n", m.Key, m.Size)
// 	return
// }

// NewSsmService is a helper func to create a SsmService instance
func NewSsmService(ctx context.Context) (*SsmService, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		// Don't need to quit the process
		log.Printf("Failed to load default SDK config to create SSM client - %s\n", err.Error())
		return nil, err
	}

	// Create an Amazon DynamoDB client.
	client := ssm.NewFromConfig(cfg)

	return &SsmService{
		client: client,
	}, nil
}

// NewDBService is a helper func to create a DBService instance
func NewDBService(ctx context.Context, tableName string) (*DBService, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Failed to load default SDK config to create DynamoDB client - %s\n", err.Error())
	}

	// Create an Amazon DynamoDB client.
	client := dynamodb.NewFromConfig(cfg)

	return &DBService{
		tableName: tableName,
		client:    client,
	}, nil
}

// NewSqsService is a helper func to create a SqsService instance
func NewSqsService(ctx context.Context, queueName string) (*SqsService, error) {

	// Only to use default config here
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Failed to load default SDK config to create SQS client - %s\n", err.Error())
	}

	client := sqs.NewFromConfig(cfg)
	input := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}

	result, err := client.GetQueueUrl(ctx, input)
	if err != nil {
		log.Fatalf("Unable to get the queue URL, please make sure queue %s exists - %s\n", queueName, err.Error())
		// return nil, err
	}

	queueURL := *result.QueueUrl
	log.Println("Queue URL: " + queueURL)

	SqsService := SqsService{
		queueName: queueName,
		queueURL:  queueURL,
		client:    client,
	}

	return &SqsService, nil
}

// SendMessage function sends 1 message at a time to the Queue
func (ss *SqsService) SendMessage(ctx context.Context, o *Object) error {
	// log.Printf("Send Message to Queue")

	input := &sqs.SendMessageInput{
		QueueUrl:    &ss.queueURL,
		MessageBody: o.toString(),
	}

	resp, err := ss.client.SendMessage(ctx, input)
	if err != nil {
		log.Printf("Error: %s", err)
		return err
	}

	log.Println("Sent message with ID: " + *resp.MessageId)
	return nil
}

// SendMessageInBatch function sends messages to the Queue in batch.
// Each batch can only contains up to 10 messages
func (ss *SqsService) SendMessageInBatch(ctx context.Context, batch []*Object) {
	// log.Printf("Sending %d messages to Queue in batch ", len(batch))

	// Assume batch size <= 10
	entries := make([]types.SendMessageBatchRequestEntry, len(batch), len(batch))

	for id, o := range batch {

		idstr := strconv.Itoa(id)
		// fmt.Printf("Id is %s\n", idstr)
		entry := types.SendMessageBatchRequestEntry{
			Id:          &idstr,
			MessageBody: o.toString(),
		}
		entries[id] = entry
	}

	input := &sqs.SendMessageBatchInput{
		QueueUrl: &ss.queueURL,
		Entries:  entries,
	}

	_, err := ss.client.SendMessageBatch(ctx, input)

	if err != nil {
		log.Fatalf("Unable to send the messages in batch to SQS Queue - %s\n", err.Error())
	}

	// log.Printf("Sent %d messages successfully\n", len(resp.Successful))
}

// ReceiveMessages function receives many messages in batch from the Queue
func (ss *SqsService) ReceiveMessages(ctx context.Context) (obj *Object, receiptHandle *string) {

	input := &sqs.ReceiveMessageInput{
		QueueUrl: &ss.queueURL,
	}
	output, err := ss.client.ReceiveMessage(ctx, input)

	if err != nil {
		log.Fatalf("Unable to read message from Queue %s - %s", ss.queueName, err.Error())
	}

	// If no messages in the queue
	if output.Messages == nil {
		// fmt.Printf("No Messages\n")
		// msg = newMessage(*output.Messages[0].Body)
		return nil, nil
	}

	// log.Println("Message ID:     " + *output.Messages[0].MessageId)
	// log.Println("Message Handle: " + *output.Messages[0].ReceiptHandle)
	// log.Println("Message Body: " + *output.Messages[0].Body)

	// TODO: Support S3 Event Message
	// Need to check the format of the message body
	obj = newObject(*output.Messages[0].Body)
	receiptHandle = output.Messages[0].ReceiptHandle

	return
}

// DeleteMessage function is used to delete message from the Queue
// Returns True if message is deleted successfully
func (ss *SqsService) DeleteMessage(ctx context.Context, rh *string) (ok bool) {

	log.Printf("Delete Message from Queue\n")

	input := &sqs.DeleteMessageInput{
		QueueUrl:      &ss.queueURL,
		ReceiptHandle: rh,
	}

	_, err := ss.client.DeleteMessage(ctx, input)

	if err != nil {
		log.Printf("Unable to delete message from Queue %s - %s", ss.queueName, err.Error())
		return false
	}

	// log.Printf(output.ResultMetadata)
	return true
}

// ChangeVisibilityTimeout function is used to change the Visibility Timeout of a message
// func (ss *SqsService) ChangeVisibilityTimeout(rh *string, seconds int32) (ok bool) {
// 	input := &sqs.ChangeMessageVisibilityInput{
// 		QueueUrl:          &ss.queueURL,
// 		ReceiptHandle:     rh,
// 		VisibilityTimeout: seconds,
// 	}
// 	_, err := ss.client.ChangeMessageVisibility(ss.ctx, input)

// 	if err != nil {
// 		log.Printf("Unable to Change Visibility Timeout - %s", err.Error())
// 		return false
// 	}

// 	// log.Printf(output)
// 	return true
// }

// IsQueueEmpty is a function to check if the Queue is empty or not
func (ss *SqsService) IsQueueEmpty(ctx context.Context) bool {
	// TODO: Implement this.
	input := &sqs.GetQueueAttributesInput{
		QueueUrl: &ss.queueURL,
		AttributeNames: []types.QueueAttributeName{
			"ApproximateNumberOfMessages",
			"ApproximateNumberOfMessagesNotVisible",
		},
	}
	output, err := ss.client.GetQueueAttributes(ctx, input)

	if err != nil {
		log.Fatalf("Unable to read message from Queue %s - %s", ss.queueName, err.Error())
	}

	visible := output.Attributes["ApproximateNumberOfMessages"]
	notVisible := output.Attributes["ApproximateNumberOfMessagesNotVisible"]

	log.Printf("Queue %s has %s not visible message(s) and %s visable message(s)\n", ss.queueName, notVisible, visible)

	if visible == "0" && notVisible == "0" {
		return true
	}

	return false
}

// GetParameterValue is a function to check if the Queue is empty or not
func (s *SsmService) GetParameterValue(ctx context.Context, param *string, withDecryption bool) *string {
	log.Printf("Get Parameter Value of %s from SSM\n", *param)

	input := &ssm.GetParameterInput{
		Name:           param,
		WithDecryption: withDecryption,
	}

	output, err := s.client.GetParameter(ctx, input)

	if err != nil {
		log.Printf("Error getting Parameter Value of %s from SSM - %s", *param, err.Error())
		return nil
	}
	return output.Parameter.Value
}

// CreateItem is a function to ...
func (db *DBService) CreateItem(ctx context.Context, o *Object, uploadID *string) error {
	log.Printf("Create a record of %s in DynamoDB\n", o.Key)

	// item := make(map[string]dtype.AttributeValue)
	// item["objectKey"] = &dtype.AttributeValueMemberS{Value: o.Key}
	// item["size"] = &dtype.AttributeValueMemberN{Value: fmt.Sprintf("%d", o.Size)}
	// item["status"] = &dtype.AttributeValueMemberS{Value: "STARTED"}
	// item["startTime"] = &dtype.AttributeValueMemberS{Value: time.Now().Format("2006/01/02 15:04:05")}
	// item["startTimestamp"] = &dtype.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().Unix())}

	item := &Item{
		ObjectKey:      o.Key,
		Size:           o.Size,
		JobStatus:      "STARTED",
		StartTime:      time.Now().Format("2006/01/02 15:04:05"),
		StartTimestamp: time.Now().Unix(),
		// UploadID:       *uploadID,
	}
	itemAttr, err := attributevalue.MarshalMap(item)

	if err != nil {
		log.Printf("Unable to Marshal DynamoDB attributes for %s - %s\n", o.Key, err.Error())
	} else {
		input := &dynamodb.PutItemInput{
			TableName: &db.tableName,
			Item:      itemAttr,
		}

		_, err = db.client.PutItem(ctx, input)

		if err != nil {
			log.Printf("Error creating a record of %s in DynamoDB - %s\n", o.Key, err.Error())
			// return nil
		}
	}

	return err
}

// UpdateItem is a function to ...
func (db *DBService) UpdateItem(ctx context.Context, key *string, result *TransferResult) error {
	log.Printf("Update a record of %s in DynamoDB\n", *key)

	keyAttr := make(map[string]dtype.AttributeValue)
	keyAttr["ObjectKey"] = &dtype.AttributeValueMemberS{Value: *key}

	etag := ""

	if result.etag != nil {
		etag = *result.etag
	}

	expr := "set JobStatus = :s, Etag = :tg, EndTime = :et, EndTimeStamp = :etm"

	input := &dynamodb.UpdateItemInput{
		TableName: &db.tableName,
		Key:       keyAttr,
		ExpressionAttributeValues: map[string]dtype.AttributeValue{
			":s":   &dtype.AttributeValueMemberS{Value: result.status},
			":tg":  &dtype.AttributeValueMemberS{Value: etag},
			":et":  &dtype.AttributeValueMemberS{Value: time.Now().Format("2006/01/02 15:04:05")},
			":etm": &dtype.AttributeValueMemberS{Value: fmt.Sprintf("%d", time.Now().Unix())},
		},
		ReturnValues:     "UPDATED_NEW",
		UpdateExpression: &expr,
	}

	_, err := db.client.UpdateItem(ctx, input)

	if err != nil {
		log.Printf("Error updating a record of %s in DynamoDB - %s\n", *key, err.Error())
		// return nil
	}
	// item = &Item{}

	// err = attributevalue.UnmarshalMap(output.Attributes, item)
	// if err != nil {
	// 	log.Printf("failed to unmarshal Dynamodb Scan Items, %v", err)
	// }
	return err
}

// QueryItem is a function to ...
func (db *DBService) QueryItem(ctx context.Context, key *string) (item *Item) {
	log.Printf("Create a record of %s in DynamoDB\n", *key)

	input := &dynamodb.GetItemInput{
		TableName: &db.tableName,
		Key: map[string]dtype.AttributeValue{
			"ObjectKey": &dtype.AttributeValueMemberS{Value: *key},
		},
	}

	output, err := db.client.GetItem(ctx, input)

	if err != nil {
		log.Printf("Error getting a record of %s in DynamoDB - %s\n", *key, err.Error())
		// return nil
	}

	item = &Item{}

	err = attributevalue.UnmarshalMap(output.Item, item)
	if err != nil {
		log.Printf("failed to unmarshal Dynamodb Scan Items, %v", err)
	}
	return

}

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
	ObjectKey                                     string
	JobStatus, Etag, Sequencer                    string
	Size, StartTimestamp, EndTimestamp, SpentTime int64
	StartTime, EndTime                            string
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
	// log.Println("Queue URL: " + queueURL)

	SqsService := SqsService{
		queueName: queueName,
		queueURL:  queueURL,
		client:    client,
	}

	return &SqsService, nil
}

// SendMessage function sends 1 message at a time to the Queue
func (ss *SqsService) SendMessage(ctx context.Context, body *string) {
	// log.Printf("Sending 1 Message to Queue")

	input := &sqs.SendMessageInput{
		QueueUrl:    &ss.queueURL,
		MessageBody: body,
	}

	_, err := ss.client.SendMessage(ctx, input)
	if err != nil {
		log.Fatalf("Failed to send the messages in batch to SQS Queue - %s\n", err.Error())
	}

	// log.Println("Sent message with ID: " + *resp.MessageId)
}

// SendMessageInBatch function sends messages to the Queue in batch.
// Each batch can only contains up to 10 messages
func (ss *SqsService) SendMessageInBatch(ctx context.Context, batch []*string) {
	// log.Printf("Sending %d messages to Queue in batch ", len(batch))

	// Assume batch size <= 10
	entries := make([]types.SendMessageBatchRequestEntry, len(batch), len(batch))

	for id, body := range batch {

		idstr := strconv.Itoa(id)
		// fmt.Printf("Id is %s\n", idstr)
		entry := types.SendMessageBatchRequestEntry{
			Id:          &idstr,
			MessageBody: body,
		}
		entries[id] = entry
	}

	input := &sqs.SendMessageBatchInput{
		QueueUrl: &ss.queueURL,
		Entries:  entries,
	}

	// Sometimes, there will be unexpected exception on SendMessageBatch
	// The standard retryer doesn't work
	// Add another retry layer - each time 30 seconds
	retry := 0
	for retry <= 5 {
		_, err := ss.client.SendMessageBatch(ctx, input)

		if err != nil {
			retry++
			log.Printf("Failed to send the messages in batch to SQS Queue - %s - Retry %d time(s)\n", err.Error(), retry)
			time.Sleep(time.Second * 30)
		} else {
			return
		}
	}

	// log.Printf("Sent %d messages successfully\n", len(resp.Successful))
}

// ReceiveMessages function receives many messages in batch from the Queue
// Currently, only 1 message is returned, MaxNumberOfMessages is defaulted to 1
func (ss *SqsService) ReceiveMessages(ctx context.Context) (body, receiptHandle *string) {

	input := &sqs.ReceiveMessageInput{
		QueueUrl:        &ss.queueURL,
		WaitTimeSeconds: 20,
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

	body = output.Messages[0].Body
	receiptHandle = output.Messages[0].ReceiptHandle

	return
}

// DeleteMessage function is used to delete message from the Queue
// Returns True if message is deleted successfully
func (ss *SqsService) DeleteMessage(ctx context.Context, rh *string) (ok bool) {
	// log.Printf("Delete Message from Queue\n")

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
func (ss *SqsService) ChangeVisibilityTimeout(ctx context.Context, rh *string, seconds int32) (ok bool) {

	input := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &ss.queueURL,
		ReceiptHandle:     rh,
		VisibilityTimeout: seconds,
	}
	_, err := ss.client.ChangeMessageVisibility(ctx, input)

	if err != nil {
		log.Printf("Unable to Change Visibility Timeout - %s", err.Error())
		return false
	}

	// log.Printf(output)
	return true
}

// IsQueueEmpty is a function to check if the Queue is empty or not
func (ss *SqsService) IsQueueEmpty(ctx context.Context) (isEmpty bool) {
	isEmpty = false
	input := &sqs.GetQueueAttributesInput{
		QueueUrl: &ss.queueURL,
		AttributeNames: []types.QueueAttributeName{
			"ApproximateNumberOfMessages",
			"ApproximateNumberOfMessagesNotVisible",
		},
	}
	output, err := ss.client.GetQueueAttributes(ctx, input)

	if err != nil {
		log.Printf("Faided to get queue attributes from Queue %s, please try again later - %s", ss.queueName, err.Error())
		return
	}

	visible, _ := strconv.Atoi(output.Attributes["ApproximateNumberOfMessages"])
	notVisible, _ := strconv.Atoi(output.Attributes["ApproximateNumberOfMessagesNotVisible"])

	log.Printf("Queue %s has %d not visible message(s) and %d visable message(s)\n", ss.queueName, notVisible, visible)

	if visible+notVisible <= 1 {
		isEmpty = true
	}
	return
}

// GetParameterValue is a function to read the value of a parameter in System Manager Parameter Store
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

// PutItem is a function to creates a new item, or replaces an old item with a new item in DynamoDB
// Restart a transfer of an object will replace the old item with new info
func (db *DBService) PutItem(ctx context.Context, o *Object) error {
	// log.Printf("Put item for %s in DynamoDB\n", o.Key)

	item := &Item{
		ObjectKey:      o.Key,
		Size:           o.Size,
		Sequencer:      o.Sequencer,
		JobStatus:      "STARTED",
		StartTime:      time.Now().Format("2006/01/02 15:04:05"),
		StartTimestamp: time.Now().Unix(),
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
			log.Printf("Failed to put item for %s in DynamoDB - %s\n", o.Key, err.Error())
			// return nil
		}
	}

	return err
}

// UpdateItem is a function to update an item in DynamoDB
func (db *DBService) UpdateItem(ctx context.Context, key *string, result *TransferResult) error {
	// log.Printf("Update item for %s in DynamoDB\n", *key)

	etag := ""
	if result.etag != nil {
		etag = *result.etag
	}

	expr := "set JobStatus = :s, Etag = :tg, EndTime = :et, EndTimestamp = :etm, SpentTime = :etm - StartTimestamp"

	input := &dynamodb.UpdateItemInput{
		TableName: &db.tableName,
		Key: map[string]dtype.AttributeValue{
			"ObjectKey": &dtype.AttributeValueMemberS{Value: *key},
		},
		ExpressionAttributeValues: map[string]dtype.AttributeValue{
			":s":   &dtype.AttributeValueMemberS{Value: result.status},
			":tg":  &dtype.AttributeValueMemberS{Value: etag},
			":et":  &dtype.AttributeValueMemberS{Value: time.Now().Format("2006/01/02 15:04:05")},
			":etm": &dtype.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().Unix())},
		},
		ReturnValues:     "NONE",
		UpdateExpression: &expr,
	}

	_, err := db.client.UpdateItem(ctx, input)

	if err != nil {
		log.Printf("Failed to update item for %s in DynamoDB - %s\n", *key, err.Error())
		// return nil
	}
	// item = &Item{}

	// err = attributevalue.UnmarshalMap(output.Attributes, item)
	// if err != nil {
	// 	log.Printf("failed to unmarshal Dynamodb Scan Items, %v", err)
	// }
	return err
}

// UpdateSequencer is a function to update an item with new Sequencer in DynamoDB
func (db *DBService) UpdateSequencer(ctx context.Context, key, sequencer *string) error {
	// log.Printf("Update Sequencer for %s in DynamoDB\n", *key)

	expr := "set Sequencer = :s"

	input := &dynamodb.UpdateItemInput{
		TableName: &db.tableName,
		Key: map[string]dtype.AttributeValue{
			"ObjectKey": &dtype.AttributeValueMemberS{Value: *key},
		},
		ExpressionAttributeValues: map[string]dtype.AttributeValue{
			":s": &dtype.AttributeValueMemberS{Value: *sequencer},
		},
		ReturnValues:     "NONE",
		UpdateExpression: &expr,
	}

	_, err := db.client.UpdateItem(ctx, input)

	if err != nil {
		log.Printf("Failed to update item for %s with Sequencer %s in DynamoDB - %s\n", *key, *sequencer, err.Error())
		// return nil
	}
	// item = &Item{}

	// err = attributevalue.UnmarshalMap(output.Attributes, item)
	// if err != nil {
	// 	log.Printf("failed to unmarshal Dynamodb Scan Items, %v", err)
	// }
	return err
}

// QueryItem is a function to query an item by Key in DynamoDB
func (db *DBService) QueryItem(ctx context.Context, key *string) (*Item, error) {
	// log.Printf("Query item for %s in DynamoDB\n", *key)

	input := &dynamodb.GetItemInput{
		TableName: &db.tableName,
		Key: map[string]dtype.AttributeValue{
			"ObjectKey": &dtype.AttributeValueMemberS{Value: *key},
		},
	}

	output, err := db.client.GetItem(ctx, input)

	if err != nil {
		log.Printf("Error querying item for %s in DynamoDB - %s\n", *key, err.Error())
		return nil, err
	}

	if output.Item == nil {
		log.Printf("Item for %s does not exist in DynamoDB", *key)
		return nil, nil
	}

	item := &Item{}

	err = attributevalue.UnmarshalMap(output.Item, item)
	if err != nil {
		log.Printf("Failed to unmarshal Dynamodb Query result, %v", err)
	}
	return item, nil

}

package drh

import (
	"context"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

// Metadata info of object
type Metadata struct {
	ContentType string
}

// Item holds info about the items to be stored in DynamoDB
type Item struct {
	ObjectKey, StorageClass, DesBucket, DesKey, Status, Version string
	Size                                                        int64
	StartTime, EndTime                                          int
	ExtraInfo                                                   Metadata
}

// DBService is a wrapper service used to interact with Amazon DynamoDB
type DBService struct {
	tableName string
	client    *dynamodb.Client
	ctx       context.Context
}

// SqsService is a wrapper service used to interact with Amazon SQS
type SqsService struct {
	queueName, queueURL string
	client              *sqs.Client
	ctx                 context.Context
}

// SsmService is a wrapper service used to interact with Amazon KMS
type SsmService struct {
	// tableName string
	client *ssm.Client
	ctx    context.Context
}

// Message is Default message (body) format in SQS
// Assume Message can have different format comparing with drh.Object
type Message struct {
	Key, Version string
	Size         int64
}

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

// NewSsmService is a ...
func NewSsmService(ctx context.Context) (*SsmService, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Printf("unable to load SDK config to create SSM client - %s\n", err.Error())
		return nil, err
	}

	// Create an Amazon DynamoDB client.
	client := ssm.NewFromConfig(cfg)

	return &SsmService{
		client: client,
		ctx:    ctx,
	}, nil
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

// NewSqsService is helper function used to create a SqsService instance
func NewSqsService(ctx context.Context, queueName string) (*SqsService, error) {

	// Only to use default config here
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Error creating a SQS Client, unable to load default configuration - %s\n", err.Error())
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
		ctx:       ctx,
	}

	return &SqsService, nil
}

// SendMessage function sends 1 message at a time to the Queue
func (ss *SqsService) SendMessage(o *Object) error {
	// log.Printf("Send Message to Queue")

	input := &sqs.SendMessageInput{
		QueueUrl:    &ss.queueURL,
		MessageBody: o.toString(),
	}

	resp, err := ss.client.SendMessage(ss.ctx, input)
	if err != nil {
		log.Printf("Error: %s", err)
		return err
	}

	log.Println("Sent message with ID: " + *resp.MessageId)
	return nil
}

// SendMessageInBatch function sends messages to the Queue in batch.
// Each batch can only contains up to 10 messages
func (ss *SqsService) SendMessageInBatch(batch []*Object) {
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

	_, err := ss.client.SendMessageBatch(ss.ctx, input)

	if err != nil {
		log.Fatalf("Unable to send the messages in batch to SQS Queue - %s\n", err.Error())
	}

	// log.Printf("Sent %d messages successfully\n", len(resp.Successful))
}

// ReceiveMessages function receives many messages in batch from the Queue
func (ss *SqsService) ReceiveMessages() (obj *Object, receiptHandle *string) {

	input := &sqs.ReceiveMessageInput{
		QueueUrl: &ss.queueURL,
	}
	output, err := ss.client.ReceiveMessage(ss.ctx, input)

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

	obj = newObject(*output.Messages[0].Body)
	receiptHandle = output.Messages[0].ReceiptHandle

	return
}

// DeleteMessage function is used to delete message from the Queue
// Returns True if message is deleted successfully
func (ss *SqsService) DeleteMessage(rh *string) (ok bool) {

	log.Printf("Delete Message from Queue\n")

	input := &sqs.DeleteMessageInput{
		QueueUrl:      &ss.queueURL,
		ReceiptHandle: rh,
	}

	_, err := ss.client.DeleteMessage(ss.ctx, input)

	if err != nil {
		log.Printf("Unable to delete message from Queue %s - %s", ss.queueName, err.Error())
		return false
	}

	// log.Printf(output.ResultMetadata)
	return true
}

// ChangeVisibilityTimeout function is used to change the Visibility Timeout of a message
func (ss *SqsService) ChangeVisibilityTimeout(rh *string, seconds int32) (ok bool) {
	input := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &ss.queueURL,
		ReceiptHandle:     rh,
		VisibilityTimeout: seconds,
	}
	_, err := ss.client.ChangeMessageVisibility(ss.ctx, input)

	if err != nil {
		log.Printf("Unable to Change Visibility Timeout - %s", err.Error())
		return false
	}

	// log.Printf(output)
	return true

}

// IsQueueEmpty is a function to check if the Queue is empty or not
func (ss *SqsService) IsQueueEmpty() bool {
	return true
}

// GetParameterValue is a function to check if the Queue is empty or not
func (s *SsmService) GetParameterValue(param *string, withDecryption bool) *string {
	log.Printf("Get Parameter Value of %s from SSM\n", *param)

	input := &ssm.GetParameterInput{
		Name:           param,
		WithDecryption: withDecryption,
	}

	output, err := s.client.GetParameter(s.ctx, input)

	if err != nil {
		log.Printf("Error getting Parameter Value of %s from SSM - %s", *param, err.Error())
		return nil
	}
	return output.Parameter.Value
}

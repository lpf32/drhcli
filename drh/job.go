package drh

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	// Ignore do nothing
	Ignore = iota

	// Delete is an action to delete an object
	Delete

	// Transfer is an action to transfer an object
	Transfer
)

// Job is an interface of a process to run by this tool
// A Job must have a Run() method
type Job interface {
	Run(ctx context.Context)
}

// Finder is an implemenation of Job interface
// Finder compares the differences of source and destination and sends the delta to SQS
type Finder struct {
	srcClient, desClient Client
	sqs                  *SqsService
	cfg                  *JobConfig
}

// Worker is an implemenation of Job interface
// Worker is used to consume the messages from SQS and start the transferring
type Worker struct {
	srcClient, desClient Client
	cfg                  *JobConfig
	sqs                  *SqsService
	db                   *DBService
}

// TransferResult stores the result after transfer.
type TransferResult struct {
	status string
	etag   *string
	err    error
}

// helper function to check credentials
func getCredentials(ctx context.Context, param string, inCurrentAccount bool, sm *SsmService) *S3Credentials {
	cred := &S3Credentials{
		noSignRequest: false,
	}

	// No need to do anything if inCurrentAccount is true
	if !inCurrentAccount {
		if param == "" {
			// no credential is required.
			cred.noSignRequest = true
		} else {
			credStr := sm.GetParameterValue(ctx, &param, true)
			if credStr != nil {
				credMap := make(map[string]string)
				err := json.Unmarshal([]byte(*credStr), &credMap)
				if err != nil {
					log.Printf("Warning - Unable to parse the credentials string, please make sure the it is a valid json format. - %s\n", err.Error())
				} else {
					cred.accessKey = credMap["access_key_id"]
					cred.secretKey = credMap["secret_access_key"]
				}
				// log.Println(*credStr)
				// log.Println(credMap)
			} else {
				log.Printf("Credential parameter %s ignored, use default configuration\n", param)
			}
		}
	}
	return cred
}

// NewFinder creates a new Finder instance
func NewFinder(ctx context.Context, cfg *JobConfig) (f *Finder) {
	sqs, _ := NewSqsService(ctx, cfg.JobQueueName)
	sm, err := NewSsmService(ctx)
	if err != nil {
		log.Printf("Warning - Unable to load credentials, use default setting - %s\n", err.Error())
	}

	srcCred := getCredentials(ctx, cfg.SrcCredential, cfg.SrcInCurrentAccount, sm)
	desCred := getCredentials(ctx, cfg.DestCredential, cfg.DestInCurrentAccount, sm)

	// TODO: Add logic when destination prefix is not empty
	srcClient := NewS3Client(ctx, cfg.SrcBucketName, cfg.SrcBucketPrefix, cfg.SrcRegion, cfg.SrcType, srcCred)
	desClient := NewS3Client(ctx, cfg.DestBucketName, cfg.DestBucketPrefix, cfg.DestRegion, cfg.SrcType, desCred)

	f = &Finder{
		srcClient: srcClient,
		desClient: desClient,
		sqs:       sqs,
		cfg:       cfg,
	}
	return
}

// List objects in source bucket
func (f *Finder) getSourceObjects(ctx context.Context, token *string, prefix *string) []*Object {
	// log.Printf("Getting source list with token %s", *token)
	result, err := f.srcClient.ListObjects(ctx, token, prefix, f.cfg.MaxKeys)
	if err != nil {
		log.Printf("Fail to get source list - %s\n", err.Error())
		// Log the last token and exit
		log.Fatalf("The last token is %s\n", *token)
	}
	return result
}

// List objects in destination bucket, load the full list into a map
func (f *Finder) getTargetObjects(ctx context.Context, prefix *string) (objects map[string]*int64) {

	log.Printf("Getting target list in /%s\n", *prefix)

	token := ""
	objects = make(map[string]*int64)

	for token != "End" {
		jobs, err := f.desClient.ListObjects(ctx, &token, prefix, f.cfg.MaxKeys)
		if err != nil {
			log.Fatalf("Error listing objects in destination bucket - %s\n", err.Error())
		}
		// fmt.Printf("Size is %d\n", len(jobs))
		// fmt.Printf("Token is %s\n", token)

		for _, job := range jobs {
			// fmt.Printf("key is %s, size is %d\n", job.Key, job.Size)
			objects[job.Key] = &job.Size
		}
	}
	log.Printf("%d objects in /%s\n", len(objects), *prefix)
	return
}

// This function will compare source and target and get a list of delta,
// and then send delta to SQS Queue.
func (f *Finder) compareAndSend(ctx context.Context, prefix *string, msgCh chan struct{}, compareCh chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("Comparing in /%s\n", *prefix)
	target := f.getTargetObjects(ctx, prefix)

	token := ""
	i, j := 0, 0
	batch := make([]*string, f.cfg.MessageBatchSize)

	start := time.Now()
	log.Printf("Start comparing source with target...\n")

	for token != "End" {
		source := f.getSourceObjects(ctx, &token, prefix)

		for _, obj := range source {
			// TODO: Check if there is another way to compare
			// Currently, map is used to search if such object exists in target
			if tsize, found := target[obj.Key]; !found || *tsize != obj.Size {
				// log.Printf("Find a difference %s - %d\n", key, size)
				batch[i] = obj.toString()
				i++
				if i%f.cfg.MessageBatchSize == 0 {
					wg.Add(1)
					j++
					if j%100 == 0 {
						log.Printf("Found %d batches in prefix /%s\n", j, *prefix)
					}
					msgCh <- struct{}{}
					i = 0

					go func(batch []*string) {
						defer wg.Done()
						f.sqs.SendMessageInBatch(ctx, batch)
						<-msgCh
					}(batch)
				}
			}
		}
	}
	// For remainning objects.
	if i != 0 {
		j++
		wg.Add(1)
		msgCh <- struct{}{}
		go func(batch []*string) {
			defer wg.Done()
			f.sqs.SendMessageInBatch(ctx, batch[:i])
			<-msgCh
		}(batch)
	}

	end := time.Since(start)
	log.Printf("Sent %d batches in %v seconds", j, end)
	<-compareCh
}

// Run is main execution function for Finder.
func (f *Finder) Run(ctx context.Context) {

	if !f.sqs.IsQueueEmpty(ctx) {
		log.Fatalf("Queue might not be empty or Unknown error... Please try again later")
	}

	// Maximum number of queued messages to be sent to SQS
	var bufferSize int = 10000

	// Assume sending messages is slower than listing and comparing
	// Create a channel to block the process not to generate too many messages to be sent.
	msgCh := make(chan struct{}, bufferSize)

	// Maximum number of finder threads in parallel
	// Create a channel to block
	// Note that bigger number needs more memory
	compareCh := make(chan struct{}, f.cfg.FinderNumber)

	prefixes := f.srcClient.ListCommonPrefixes(ctx, f.cfg.FinderDepth, f.cfg.MaxKeys)

	var wg sync.WaitGroup

	start := time.Now()

	for _, p := range prefixes {
		compareCh <- struct{}{}
		wg.Add(1)
		go f.compareAndSend(ctx, p, msgCh, compareCh, &wg)
	}
	wg.Wait()

	end := time.Since(start)
	log.Printf("Finder Job Completed in %v seconds\n", end)
}

// NewWorker creates a new Worker instance
func NewWorker(ctx context.Context, cfg *JobConfig) (w *Worker) {
	log.Printf("Source Type is %s\n", cfg.SrcType)
	sqs, _ := NewSqsService(ctx, cfg.JobQueueName)

	db, _ := NewDBService(ctx, cfg.JobTableName)

	sm, err := NewSsmService(ctx)
	if err != nil {
		log.Printf("Warning - Unable to load credentials, use default setting - %s\n", err.Error())
	}

	srcCred := getCredentials(ctx, cfg.SrcCredential, cfg.SrcInCurrentAccount, sm)
	desCred := getCredentials(ctx, cfg.DestCredential, cfg.DestInCurrentAccount, sm)

	// TODO: Add logic when destination prefix is not empty
	srcClient := NewS3Client(ctx, cfg.SrcBucketName, cfg.SrcBucketPrefix, cfg.SrcRegion, cfg.SrcType, srcCred)
	desClient := NewS3Client(ctx, cfg.DestBucketName, cfg.DestBucketPrefix, cfg.DestRegion, cfg.SrcType, desCred)

	return &Worker{
		srcClient: srcClient,
		desClient: desClient,
		sqs:       sqs,
		db:        db,
		cfg:       cfg,
	}
}

// Run a Worker job
func (w *Worker) Run(ctx context.Context) {
	// log.Println("Start migration...")

	// A channel to block number of messages to be processed
	// Buffer size is cfg.WorkerNumber
	processCh := make(chan struct{}, w.cfg.WorkerNumber)

	// Channel to block number of objects/parts to be processed.
	// Buffer size is cfg.WorkerNumber * 2 - 1 (More buffer for multipart upload)
	transferCh := make(chan struct{}, w.cfg.WorkerNumber*2-1)

	// Channel to store transfer result
	// Buffer size is same as transfer Channel
	// resultCh := make(chan *TransferResult, w.cfg.WorkerNumber*2)

	for {
		msg, rh := w.sqs.ReceiveMessages(ctx)

		if msg == nil {
			log.Println("No messages, sleep...")
			time.Sleep(time.Second * 60)
			continue
		}

		obj, action := w.processMessage(ctx, msg, rh)
		if action == Transfer {
			processCh <- struct{}{}
			go w.startMigration(ctx, obj, rh, transferCh, processCh)
		}
		if action == Delete {
			processCh <- struct{}{}
			go w.startDelete(ctx, obj, rh, processCh)
		}
	}
}

// processMessage is a function to process the raw SQS message, return an Action Code to determine further actions.
// Action Code includes Transfer, Delete or Ignore
func (w *Worker) processMessage(ctx context.Context, msg, rh *string) (obj *Object, action int) {
	// log.Println("Processing Event Message...")
	action = Ignore // Default to ignore

	if strings.Contains(*msg, `"s3:TestEvent"`) {
		// Once S3 Notification is set up, a TestEvent message will be generated by service.
		// Delete the test message
		log.Println("Test Event Message received, deleting the message...")
		w.sqs.DeleteMessage(ctx, rh)
		return
	}

	// Simply check if msg body contains "eventSource" to determine if it's a event message
	// might need to change in the future
	if strings.Contains(*msg, `"eventSource":`) {

		event := newS3Event(msg)

		// log.Println(*event)
		// log.Printf("Event is %s", event.Records[0].EventName)

		// There shouldn't be more than 1 record in the event message
		if len(event.Records) > 1 {
			log.Println("Warning - Found event message with more than 1 record, Skipped...")
			return
		}

		if event.Records[0].EventSource != "aws:s3" {
			log.Println("Error - Event message from Unknown source, expect S3 event message only")
			return
		}

		log.Printf("Received an event message of %s, start processing...\n", event.Records[0].EventName)

		obj = &event.Records[0].S3.Object
		obj.Key = unescape(&obj.Key)
		seq := getHex(&event.Records[0].S3.Sequencer)

		var oldSeq int64 = 0
		// Get old sequencer from DynamoDB
		item, _ := w.db.QueryItem(ctx, &event.Records[0].S3.Key)
		if item != nil {
			oldSeq = getHex(&item.Sequencer)
		}

		// log.Printf("Seq is %d, Old Seq is %d\n", seq, oldSeq)

		// equals might be a retry
		if seq < oldSeq {
			log.Printf("Old Event, ignored")
			action = Ignore
		}

		if strings.HasPrefix(event.Records[0].EventName, "ObjectRemoved") {
			action = Delete
		} else if strings.HasPrefix(event.Records[0].EventName, "ObjectCreated") {
			action = Transfer
		} else {
			log.Printf("Unknown S3 Event %s, do nothing", event.Records[0].EventName)
		}
	} else {
		obj = newObject(msg)
		action = Transfer
	}
	return
}

// startMigration is a function to migrate an object from source to destination
func (w *Worker) startMigration(ctx context.Context, obj *Object, rh *string, transferCh chan struct{}, processCh <-chan struct{}) {

	log.Printf("Migrating from %s/%s to %s/%s\n", w.cfg.SrcBucketName, obj.Key, w.cfg.DestBucketName, obj.Key)

	// Log in DynamoDB
	w.db.PutItem(ctx, obj)

	var res *TransferResult
	if obj.Size <= int64(w.cfg.MultipartThreshold*MB) {
		res = w.migrateSmallFile(ctx, obj, transferCh)
	} else {
		res = w.migrateBigFile(ctx, obj, transferCh)
	}

	w.processResult(ctx, obj, rh, res)

	<-processCh

}

// startDelete is a function to delete an object from destination
func (w *Worker) startDelete(ctx context.Context, obj *Object, rh *string, processCh <-chan struct{}) {
	// log.Printf("Delete object from %s/%s\n", w.cfg.DestBucketName, obj.Key)

	// Currently, only Sequencer is updated with the latest one, no other info logged for delete action
	// This might be changed in future for debug purpose
	w.db.UpdateSequencer(ctx, &obj.Key, &obj.Sequencer)

	err := w.desClient.DeleteObject(ctx, &obj.Key)
	if err != nil {
		log.Printf("Failed to delete object from %s/%s - %s\n", w.cfg.DestBucketName, obj.Key, err.Error())
	} else {
		w.sqs.DeleteMessage(ctx, rh)
		log.Printf("----->Deleted 1 object %s successfully\n", obj.Key)
	}
	<-processCh

}

// processResult is a function to process transfer result, including delete the message, log to DynamoDB
func (w *Worker) processResult(ctx context.Context, obj *Object, rh *string, res *TransferResult) {
	// log.Println("Processing result...")

	log.Printf("----->Transferred 1 object %s with status %s\n", obj.Key, res.status)
	w.db.UpdateItem(ctx, &obj.Key, res)

	if res.status == "DONE" {
		w.sqs.DeleteMessage(ctx, rh)
	}
}

// Internal func to deal with the transferring of small file.
// Simply transfer the whole object
func (w *Worker) migrateSmallFile(ctx context.Context, obj *Object, transferCh chan struct{}) *TransferResult {

	// Add a transferring record
	transferCh <- struct{}{}

	result := w.transfer(ctx, obj, nil, 0, obj.Size, 0)
	// log.Printf("Completed the transfer of %s with etag %s\n", obj.Key, *result.etag)

	// Remove the transferring record  after transfer is completed
	<-transferCh

	return result

}

// Internal func to deal with the transferring of large file.
// First need to create/get an uploadID, then use UploadID to upload each parts
// Finally, need to combine all parts into a single file.
func (w *Worker) migrateBigFile(ctx context.Context, obj *Object, transferCh chan struct{}) *TransferResult {

	var err error
	var parts map[int]*Part

	uploadID := w.desClient.GetUploadID(ctx, &obj.Key)

	// If uploadID Found, use list parts to get all existing parts.
	// Else Create a new upload ID
	if uploadID != nil {
		// log.Printf("Found upload ID %s", *uploadID)
		parts = w.desClient.ListParts(ctx, &obj.Key, uploadID)

	} else {
		// TODO: Get metadata first by HeadObject
		// Add metadata to CreateMultipartUpload func.
		uploadID, err = w.desClient.CreateMultipartUpload(ctx, &obj.Key)
		if err != nil {
			log.Printf("Failed to create upload ID - %s for %s\n", err.Error(), obj.Key)
			return &TransferResult{
				status: "ERROR",
				err:    err,
			}
		}
	}

	allParts, err := w.startMultipartUpload(ctx, obj, uploadID, parts, transferCh)
	if err != nil {
		return &TransferResult{
			status: "ERROR",
			err:    err,
		}
	}

	etag, err := w.desClient.CompleteMultipartUpload(ctx, &obj.Key, uploadID, allParts)
	if err != nil {
		log.Printf("Failed to complete upload for %s - %s\n", obj.Key, err.Error())
		w.desClient.AbortMultipartUpload(ctx, &obj.Key, uploadID)
		return &TransferResult{
			status: "ERROR",
			err:    err,
		}

	}
	// log.Printf("Completed the transfer of %s with etag %s\n", obj.Key, *etag)
	return &TransferResult{
		status: "DONE",
		etag:   etag,
		err:    nil,
	}
}

// A func to get total number of parts required based on object size
// Auto extend chunk size if total parts are greater than MaxParts (10000)
func (w *Worker) getTotalParts(size int64) (totalParts, chunkSize int) {
	// Max number of Parts allowed by Amazon S3 is 10000
	maxParts := 10000

	chunkSize = w.cfg.ChunkSize * MB
	totalParts = int(size/int64(chunkSize)) + 1

	if totalParts > maxParts {
		totalParts = maxParts
		chunkSize = int(size/int64(maxParts)) + 1024
	}
	return
}

// A func to perform multipart upload
func (w *Worker) startMultipartUpload(ctx context.Context, obj *Object, uploadID *string, parts map[int](*Part), transferCh chan struct{}) ([]*Part, error) {

	totalParts, chunkSize := w.getTotalParts(obj.Size)
	// log.Printf("Total parts are %d for %s\n", totalParts, obj.Key)

	var wg sync.WaitGroup

	partCh := make(chan *Part, totalParts)
	partErrorCh := make(chan error, totalParts) // Capture Errors

	for i := 0; i < totalParts; i++ {
		partNumber := i + 1

		if part, found := parts[partNumber]; found {
			// log.Printf("Part %d found with etag %s, no need to transfer again\n", partNumber, *part.etag)
			// Simply put the part info to the channel
			partCh <- part
		} else {
			// If not, upload the part
			wg.Add(1)
			transferCh <- struct{}{}

			go func(i int) {
				defer wg.Done()
				result := w.transfer(ctx, obj, uploadID, int64(i*chunkSize), int64(chunkSize), partNumber)

				if result.err != nil {
					partErrorCh <- result.err
				} else {
					part := &Part{
						partNumber: i + 1,
						etag:       result.etag,
					}
					partCh <- part
				}

				<-transferCh
			}(i)
		}
	}

	wg.Wait()
	close(partErrorCh)
	close(partCh)

	for err := range partErrorCh {
		// returned when at least 1 error
		return nil, err
	}

	allParts := make([]*Part, totalParts)
	for i := 0; i < totalParts; i++ {
		// The list of parts must be in ascending order
		p := <-partCh
		allParts[p.partNumber-1] = p

	}

	return allParts, nil
}

// transfer is a process to download data from source and upload to destination
func (w *Worker) transfer(ctx context.Context, obj *Object, uploadID *string, start, chunkSize int64, partNumber int) (result *TransferResult) {
	var etag *string
	var err error

	if start+chunkSize > obj.Size {
		chunkSize = obj.Size - start
	}

	log.Printf("----->Downloading %d Bytes from %s/%s\n", chunkSize, w.cfg.SrcBucketName, obj.Key)

	// TODO: Add metadata to GetObject result
	body, err := w.srcClient.GetObject(ctx, &obj.Key, obj.Size, start, chunkSize, "null")
	if err != nil {
		// status = "ERROR"
		return &TransferResult{
			status: "ERROR",
			err:    err,
		}
	}

	// Use PutObject for single object upload
	// Use UploadPart for multipart upload
	if uploadID != nil {
		log.Printf("----->Uploading %d Bytes to %s/%s - Part %d\n", chunkSize, w.cfg.DestBucketName, obj.Key, partNumber)
		etag, err = w.desClient.UploadPart(ctx, &obj.Key, uploadID, body, partNumber)

	} else {
		log.Printf("----->Uploading %d Bytes to %s/%s\n", chunkSize, w.cfg.DestBucketName, obj.Key)
		etag, err = w.desClient.PutObject(ctx, &obj.Key, body, w.cfg.DestStorageClass)
	}

	if err != nil {
		return &TransferResult{
			status: "ERROR",
			err:    err,
		}
	}

	log.Printf("----->Completed %d Bytes from %s/%s to %s/%s\n", chunkSize, w.cfg.SrcBucketName, obj.Key, w.cfg.DestBucketName, obj.Key)
	return &TransferResult{
		status: "DONE",
		etag:   etag,
	}

}

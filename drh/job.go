package drh

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"
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

// Part represents a part for multipart upload
type Part struct {
	partNumber int
	etag       *string
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
	// log.Printf("Cred is %v\n", srcCred)
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
	batch := make([]*Object, f.cfg.MessageBatchSize)
	// var wg sync.WaitGroup

	start := time.Now()
	log.Printf("Start comparing source with target...\n")

	for token != "End" {
		source := f.getSourceObjects(ctx, &token, prefix)

		for _, obj := range source {
			// TODO: Check if there is another way to compare
			// Currently, map is used to search if such object exists in target
			if tsize, found := target[obj.Key]; !found || *tsize != obj.Size {
				// log.Printf("Find a difference %s - %d\n", key, size)
				batch[i] = obj
				i++
				if i%f.cfg.MessageBatchSize == 0 {
					wg.Add(1)
					j++
					if j%100 == 0 {
						log.Printf("Found %d batches in prefix /%s\n", j, *prefix)
					}
					msgCh <- struct{}{}
					i = 0

					go func(batch []*Object) {
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
		go func(batch []*Object) {
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
	// Buffer size is cfg.WorkerNumber * 2 (More buffer for multipart upload)
	transferCh := make(chan struct{}, w.cfg.WorkerNumber*2)

	// Channel to store transfer result
	// Buffer size is same as transfer Channel
	resultCh := make(chan *TransferResult, w.cfg.WorkerNumber*2)

	for {
		obj, rh := w.sqs.ReceiveMessages(ctx)

		if obj == nil {
			log.Println("No messages, sleep...")
			time.Sleep(time.Second * 60)
			continue
		}

		// TODO: Start a heart beat thread
		log.Printf("Received message with key %s, start processing...\n", obj.Key)

		processCh <- struct{}{}
		go w.startMigration(ctx, obj, transferCh, resultCh)
		go w.processResult(ctx, obj, rh, processCh, resultCh)
	}
}

// To extend message visibility before timeout
// func (w *Worker) heartBeat(rh *string) {
// 	time.Sleep(time.Second * 870)

// 	i := 0
// 	// Extends 15 minutes everytime. Maximum timeout is 12 hours.
// 	for i < 4*12 {
// 		i++
// 		w.sqs.ChangeVisibilityTimeout(rh, int32(870+15*60*i))
// 		time.Sleep(time.Second * 15 * 60)
// 	}

// }

// startMigration is a function to
func (w *Worker) processResult(ctx context.Context, obj *Object, rh *string, processCh <-chan struct{}, resultCh <-chan *TransferResult) {
	// log.Println("Start migration...")

	res := <-resultCh

	if res.status == "DONE" {
		w.sqs.DeleteMessage(ctx, rh)
		// log.Printf("Delete Message%s", *rh)
	}

	w.db.UpdateItem(ctx, &obj.Key, res)

	log.Printf("Complete one job %s with status %s\n", obj.Key, res.status)
	<-processCh

}

// startMigration is a function to
func (w *Worker) startMigration(ctx context.Context, obj *Object, transferCh chan struct{}, resultCh chan<- *TransferResult) {
	// log.Println("Start migration...")

	log.Printf("Migrating from %s/%s to %s/%s\n", w.cfg.SrcBucketName, obj.Key, w.cfg.DestBucketName, obj.Key)

	// w.db.CreateItem(obj, nil)
	item := w.db.QueryItem(ctx, &obj.Key)
	log.Printf("Found Item %s", item.JobStatus)

	if item.JobStatus == "STARTED" {
		log.Println("Item already started, quit...")
	} else {

		if obj.Size <= int64(w.cfg.MultipartThreshold*MB) {
			w.migrateSmallFile(ctx, obj, transferCh, resultCh)
		} else {
			w.migrateBigFile(ctx, obj, transferCh, resultCh)
		}
	}

}

func (w *Worker) migrateSmallFile(ctx context.Context, obj *Object, transferCh chan struct{}, resultCh chan<- *TransferResult) {

	var etag *string
	var err error
	status := "DONE"

	w.db.CreateItem(ctx, obj, nil)

	// Add a transferring record
	transferCh <- struct{}{}

	log.Printf("----->Downloading %d Bytes from %s/%s\n", obj.Size, w.cfg.SrcBucketName, obj.Key)

	body, err := w.srcClient.GetObject(ctx, obj.Key, obj.Size, 0, obj.Size, "null")
	if err != nil {
		status = "ERROR"

	} else {
		etag, err = w.desClient.PutObject(ctx, obj.Key, body, w.cfg.DestStorageClass)
		if err != nil {
			status = "ERROR"
		}
	}

	resultCh <- &TransferResult{
		status: status,
		etag:   etag,
		err:    err,
	}
	// Remove the transferring record  after transfer is completed
	<-transferCh
}

func (w *Worker) migrateBigFile(ctx context.Context, obj *Object, transferCh chan struct{}, resultCh chan<- *TransferResult) {
	// log.Println("Download and Upload big file")

	chunkSize := w.cfg.ChunkSize * MB

	var wg sync.WaitGroup

	var etag *string
	var err error
	status := "DONE"

	// TODO: Check if upload ID already existed?
	// If Yes, need to use list parts to get all existing parts.
	// log.Println("Upload ID can not be found")

	// Else Create a new upload ID
	uploadID, err := w.desClient.CreateMultipartUpload(ctx, obj.Key)

	if err != nil {
		log.Printf("Unable to create upload ID - %s for %s\n", err.Error(), obj.Key)
	}

	w.db.CreateItem(ctx, obj, uploadID)

	totalParts := int(obj.Size/int64(chunkSize)) + 1

	log.Printf("Total parts are %d - for %s\n", totalParts, obj.Key)

	wg.Add(totalParts)

	// parts := make([]*Part, totalParts)
	partCh := make(chan *Part, totalParts)

	for i := 0; i < totalParts; i++ {

		partNumber := i + 1

		transferCh <- struct{}{}

		go func(i int) {

			defer wg.Done()

			var _etag *string
			var err error

			log.Printf("----->Downloading %d Bytes from %s/%s\n", chunkSize, w.cfg.SrcBucketName, obj.Key)

			body, err := w.srcClient.GetObject(ctx, obj.Key, obj.Size, int64(i*chunkSize), int64(chunkSize), "null")
			if err != nil {
				// log.Fatalln(err.Error())
				status = "ERROR"
			} else {
				log.Printf("----->Uploading %d Bytes to %s/%s - Part %d\n", chunkSize, w.cfg.DestBucketName, obj.Key, partNumber)
				_etag, err = w.desClient.UploadPart(ctx, obj.Key, uploadID, body, partNumber)
				if err != nil {
					// log.Fatalln(err.Error())
					status = "ERROR"
				}
				log.Printf("----->Upload completed, etag is %s\n", *_etag)

			}

			part := &Part{
				partNumber: i + 1,
				etag:       _etag,
			}
			partCh <- part
			<-transferCh
		}(i)
	}

	wg.Wait()

	// TODO: Check this.
	parts := make([]*Part, totalParts)
	for i := 0; i < totalParts; i++ {
		// The list of parts must be in ascending order
		p := <-partCh
		parts[p.partNumber-1] = p

	}

	etag, err = w.desClient.CompleteMultipartUpload(ctx, obj.Key, uploadID, parts)
	if err != nil {
		log.Printf("Complete upload failed - %s\n", err.Error())
		w.desClient.AbortMultipartUpload(ctx, obj.Key, uploadID)
		status = "ERROR"
	} else {
		log.Printf("Complete one job %s with etag %s\n", obj.Key, *etag)
	}

	resultCh <- &TransferResult{
		status: status,
		etag:   etag,
		err:    err,
	}
}

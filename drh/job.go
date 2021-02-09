package drh

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// Finder struct
type Finder struct {
	srcClient, desClient Client
	sqs                  *SqsService
}

func (f *Finder) getSourceObjects(token *string) (objects map[string]int64) {

	log.Printf("Getting source list with token %s", *token)

	objects = make(map[string]int64)

	if *token != "End" {
		jobs, err := f.srcClient.ListObjects(token)
		if err != nil {
			log.Fatal(err.Error())
			log.Fatalf("Fail to create client - %s", err.Error())
		}

		// fmt.Printf("Size is %d\n", len(jobs))
		// fmt.Printf("Token is %s\n", *token)

		if len(jobs) == 0 {
			return nil
		}

		for _, job := range jobs {
			// fmt.Printf("key is %s, size is %d\n", job.Key, job.Size)
			objects[job.Key] = job.Size
		}

		return
	}
	return nil
}

func (f *Finder) getTargetObjects() (objects map[string]int64) {

	log.Printf("Getting target list")

	token := ""

	objects = make(map[string]int64)

	for token != "End" {
		jobs, err := f.desClient.ListObjects(&token)
		if err != nil {
			log.Fatal(err.Error())
			log.Fatalf("Fail to create client - %s", err.Error())
		}

		// fmt.Printf("Size is %d\n", len(jobs))
		// fmt.Printf("Token is %s\n", token)

		for _, job := range jobs {
			fmt.Printf("key is %s, size is %d\n", job.Key, job.Size)
			objects[job.Key] = job.Size
		}
	}
	return

}

// CompareAndSend is main execution function.
// This function will compare source and target and get a list of delta,
// and then send delta to SQS Queue.
func (f *Finder) CompareAndSend() {

	target := f.getTargetObjects()

	log.Printf("Start Comparing...\n")

	token := ""
	var wg sync.WaitGroup
	for token != "End" {
		source := f.getSourceObjects(&token)

		for key, size := range source {
			if tsize, found := target[key]; !found || tsize != size {
				log.Printf("Find a difference %s - %d\n", key, size)

				msg := Message{
					key:     key,
					size:    size,
					version: "null",
				}

				// msgs = append(msgs, msg)
				wg.Add(1)
				go func(msg Message) {
					defer wg.Done()
					// fmt.Printf("Message is %s\n", *msg.toString())
					f.sqs.SendMessage(msg)
				}(msg)

			}
		}
		// fmt.Printf("Find a delta of %d", len(msgs))
		// wg.Add(1)
		// go func(msgs []Message) {
		// 	defer wg.Done()
		// 	f.sqs.SendMessageInBatch(msgs)
		// }(msgs)

		wg.Wait()
	}

}

// InitFinder creates a new finder
func InitFinder(ctx context.Context, cfg *JobConfig) (f *Finder) {

	log.Printf("Finder from %s - %s to %s - %s\n", cfg.SrcBucketName, cfg.SrcBucketPrefix, cfg.DestBucketName, cfg.DestBucketPrefix)
	log.Printf("Finder from %s to %s\n", cfg.SrcRegion, cfg.DestRegion)

	srcClient := NewS3Client(ctx, cfg.SrcBucketName, cfg.SrcBucketPrefix, cfg.SrcRegion)

	desClient := NewS3Client(ctx, cfg.DestBucketName, cfg.DestBucketPrefix, cfg.DestRegion)

	sqs, _ := NewSqsService(ctx, cfg.JobQueueName)

	f = &Finder{
		srcClient: srcClient,
		desClient: desClient,
		sqs:       sqs,
	}

	return
}

// Migrator is a struct ..
type Migrator struct {
	srcClient, desClient Client
	cfg                  *JobConfig
	sqs, db              string
}

// StartMigration is a function to
func (m *Migrator) StartMigration() {

}

func (m *Migrator) migrateSmallFile() {

}

func (m *Migrator) migrateBigFile() {

}

package drh

const (
	// MaxRetries when failed used globally
	// No need an option of this.
	MaxRetries int = 5

	// DefaultMaxKeys is the maximum number of keys returned per listing request, default is 1000
	DefaultMaxKeys int32 = 1000

	// DefaultMultipartThreshold is the threshold size (in MB) to determine to use multipart upload or not.
	// When object size is greater or equals to MultipartThreshold, multipart upload will be used.
	DefaultMultipartThreshold int = 10

	// DefaultChunkSize is the chunk size (in MB) for each part when using multipart upload
	DefaultChunkSize int = 5

	// DefaultMaxParts the maximum number of parts is 10000 for multipart upload
	DefaultMaxParts int = 10000

	// DefaultMessageBatchSize the number of messages in a batch to send to SQS Queue
	DefaultMessageBatchSize int = 10

	// DefaultFinderDepth the depth of sub sub folders to compare in parallel. 0 means comparing all objects together with no parallelism.
	DefaultFinderDepth int = 0

	// DefaultFinderNumber is the number of finder threads to run in parallel
	DefaultFinderNumber int = 1

	// DefaultWorkerNumber is the number of worker threads to run in parallel
	DefaultWorkerNumber int = 4
)

// JobOptions is General Job Info
type JobOptions struct {
	ChunkSize, MultipartThreshold, MessageBatchSize, FinderDepth, FinderNumber, WorkerNumber int
	MaxKeys                                                                                  int32
}

// JobConfig is General Job Info
type JobConfig struct {
	SrcType, SrcBucket, SrcPrefix, SrcRegion, SrcCredential              string
	DestBucket, DestPrefix, DestRegion, DestCredential, DestStorageClass string
	JobTableName, JobQueueName                                           string
	SrcInCurrentAccount, DestInCurrentAccount                            bool
	*JobOptions
}

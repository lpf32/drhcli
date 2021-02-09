package drh

const (
	// MaxRetries when replication failed.
	// MaxRetries int = 5

	// MaxThreads is max
	// MaxThreads int = 10

	// MaxKeys is the maximum number of keys returned per listing request, default is 1000
	MaxKeys int32 = 1000

	// MultipartThreshold is the threshold size (in MB) to determine to use multipart upload or not.
	// When object size is greater or equals to MultipartThreshold, multipart upload will be used.
	MultipartThreshold int = 10

	// ChunkSize is the chunk size (in MB) for each part when using multipart upload
	ChunkSize int = 5

	// MaxParts the maximum number of parts is 10000 for multipart upload
	MaxParts int = 10000
)

// JobConfig is General Job Info
type JobConfig struct {
	SrcType, SrcBucketName, SrcBucketPrefix, SrcRegion, SrcCredential              string
	DestBucketName, DestBucketPrefix, DestRegion, DestCredential, DestStorageClass string
	JobTableName, JobQueueName                                                     string
	ChunkSize, MultipartThreshold, MaxKeys                                         int
}

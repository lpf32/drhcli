package drh

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Object represents an object to be migrated.
type Object struct {
	Key  string
	Size int64
	// StorageClass string
}

// Client is an interface used to contact with Cloud Storage Services
type Client interface {
	// GET
	ListObjects(continuationToken *string) ([]Object, error)
	HeadObject(key string)
	GetObject(key string, size, start, chunkSize int, version string) ([]byte, error)

	// PUT
	PutObject(key string, body []byte, storageClass string) string
	CreateMultipartUpload(key string)
	CompleteMultipartUpload(key string)
	UploadPart(key string)
	ListParts(key string)
	ListMultipartUploads(key string)
}

// S3Client is an implementation of Client interface for Amazon S3
type S3Client struct {
	bucket, prefix string
	client         *s3.Client
	ctx            context.Context
}

func getConfig(ctx context.Context, region string) aws.Config {
	config, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	// config, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal("Unable to get the config")
		return aws.Config{}
	}
	return config
}

// NewS3Client create a S3Client instance
func NewS3Client(ctx context.Context, bucket, prefix, region string) *S3Client {

	config := getConfig(ctx, region)

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(config)

	return &S3Client{
		bucket: bucket,
		prefix: prefix,
		client: client,
		ctx:    ctx,
	}

}

// GetObject is a function to get (download) object from Amazon S3
func (c *S3Client) GetObject(key string, size, start, chunkSize int, version string) ([]byte, error) {
	log.Printf("S3> Get Object from S3")

	bodyRange := fmt.Sprintf("bytes=%d-%d", start, start+chunkSize-1)
	input := &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    &key,
		Range:  &bodyRange,
	}

	output, err := c.client.GetObject(c.ctx, input)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	defer output.Body.Close()

	// Read response body
	buf := new(bytes.Buffer)
	buf.ReadFrom(output.Body)
	s := buf.Bytes()
	return s, nil

}

// ListObjects is a function to list objects from Amazon S3
func (c *S3Client) ListObjects(continuationToken *string) ([]Object, error) {
	log.Println("S3> Get Object from S3")

	input := &s3.ListObjectsV2Input{
		Bucket:  &c.bucket,
		Prefix:  &c.prefix,
		MaxKeys: MaxKeys,
	}

	if *continuationToken != "" {
		input.ContinuationToken = continuationToken
	}

	output, err := c.client.ListObjectsV2(c.ctx, input)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	if output.IsTruncated {
		*continuationToken = *output.NextContinuationToken
	} else {
		*continuationToken = "End"
	}

	// TODO double check this.
	len := len(output.Contents)
	jobs := make([]Object, 0, len)

	for _, object := range output.Contents {
		//log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
		jobs = append(jobs, Object{
			Key:  *object.Key,
			Size: object.Size,
			// StorageClass: string(object.StorageClass),
		})
	}
	return jobs, nil

}

// HeadObject is a function to get extra metadata from Amazon S3
func (c *S3Client) HeadObject(key string) {
	log.Println("S3> Head Object from S3")

}

// PutObject is a function to put (upload) an object to Amazon S3
func (c *S3Client) PutObject(key string, body []byte, storageClass string) string {
	log.Println("S3> Put Object to S3")

	md5Bytes := md5.Sum(body)
	// contentMD5 := hex.EncodeToString(md5Bytes[:])
	contentMD5 := base64.StdEncoding.EncodeToString(md5Bytes[:])

	fmt.Println(contentMD5)

	reader := bytes.NewReader(body)

	input := &s3.PutObjectInput{
		Bucket:     &c.bucket,
		Key:        &key,
		Body:       reader,
		ContentMD5: &contentMD5,
		// StorageClass: types.StorageClass(storageClass),
	}

	output, err := c.client.PutObject(c.ctx, input)
	if err != nil {
		fmt.Println("Got error uploading file:")
		fmt.Println(err)
		// return
	}
	fmt.Println(output.ETag)
	return *output.ETag

}

// UploadPart is
func (c *S3Client) UploadPart(key string) {
	log.Println("S3> Get Object from S3")
}

// CompleteMultipartUpload is
func (c *S3Client) CompleteMultipartUpload(key string) {
	log.Println("S3> Get Object from S3")
}

// CreateMultipartUpload is
func (c *S3Client) CreateMultipartUpload(key string) {
	log.Println("S3> Get Object from S3")
}

// ListParts is
func (c *S3Client) ListParts(key string) {
	log.Println("S3> Get Object from S3")
}

// ListMultipartUploads is
func (c *S3Client) ListMultipartUploads(key string) {
	log.Println("S3> Get Object from S3")
}

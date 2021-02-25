package drh

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Client is an implementation of Client interface for Amazon S3
type S3Client struct {
	bucket, prefix, region, sourceType string
	client                             *s3.Client
	ctx                                context.Context
}

// S3Credentials is
type S3Credentials struct {
	accessKey, secretKey string
	noSignRequest        bool
}

// Get Endpoint URL for S3 Compliant storage service.
func getEndpointURL(region, sourceType string) (url string) {
	switch sourceType {
	case "Aliyun_OSS":
		url = fmt.Sprintf("https://oss-%s.aliyuncs.com", region)
	case "Tencent_COS":
		url = fmt.Sprintf("https://cos.%s.myqcloud.com", region)
	case "Qiniu_Kodo":
		url = fmt.Sprintf("https://s3-%s.qiniucs.com", region)
	case "Google_GCS":
		url = "https://storage.googleapis.com"
	default:
		url = ""
	}
	return url

}

// func getOptions(ctx context.Context, region, sourceType string, cred *S3Credentials) s3.Options {

// 	retryer := retry.AddWithMaxBackoffDelay(retry.NewStandard(), time.Second*5)

// 	options := s3.Options{
// 		Region:  region,
// 		Retryer: retryer,
// 	}

// 	url := getEndpointURL(region, sourceType)
// 	if url != "" {
// 		options.EndpointResolver = s3.EndpointResolverFromURL(url)
// 	}
// 	if cred.noSignRequest {
// 		log.Println("noSignRequest")
// 		options.Credentials = aws.AnonymousCredentials{}
// 	} else if cred.accessKey != "" {
// 		log.Printf("Sign with key %s in region %s\n", cred.accessKey, region)
// 		options.Credentials = aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(cred.accessKey, cred.secretKey, ""))
// 	} else {
// 		options.Credentials = aws.NewConfig().Credentials
// 	}

// 	return options
// }

// NewS3Client create a S3Client instance
func NewS3Client(ctx context.Context, bucket, prefix, region, sourceType string, cred *S3Credentials) *S3Client {

	config, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	// config, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("Error creating a S3 Client, unable to load default configuration - %s\n", err.Error())
	}

	client := s3.NewFromConfig(config, func(o *s3.Options) {
		retryer := retry.AddWithMaxBackoffDelay(retry.NewStandard(), time.Second*5)
		o.Retryer = retryer
		url := getEndpointURL(region, sourceType)
		if url != "" {
			o.EndpointResolver = s3.EndpointResolverFromURL(url)
		}
		if cred.noSignRequest {
			// log.Println("noSignRequest")
			o.Credentials = aws.AnonymousCredentials{}
		}
		if cred.accessKey != "" {
			// log.Printf("Sign with key %s in region %s\n", cred.accessKey, region)
			o.Credentials = aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(cred.accessKey, cred.secretKey, ""))
		}

	})
	return &S3Client{
		bucket:     bucket,
		prefix:     prefix,
		client:     client,
		ctx:        ctx,
		region:     region,
		sourceType: sourceType,
	}

}

// GetObject is a function to get (download) object from Amazon S3
func (c *S3Client) GetObject(key string, size, start, chunkSize int64, version string) ([]byte, error) {
	// log.Printf("S3> Downloading %s with %d bytes start from %d\n", key, size, start)

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
	// buf := new(bytes.Buffer)
	// buf.ReadFrom(output.Body)
	// s := buf.Bytes()
	s, err := io.ReadAll(output.Body)

	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return s, nil

}

func (c *S3Client) listObjectFn(continuationToken, prefix, delimiter *string, maxKeys int32) (*s3.ListObjectsV2Output, error) {

	input := &s3.ListObjectsV2Input{
		Bucket:    &c.bucket,
		Prefix:    prefix,
		MaxKeys:   maxKeys,
		Delimiter: delimiter,
	}

	if *continuationToken != "" {
		input.ContinuationToken = continuationToken
	}

	// start := time.Now()

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

	// end := time.Since(start)
	// log.Printf("Time for api request in %v seconds", end)

	return output, nil
}

// Recursively list sub directories
func (c *S3Client) listPrefixFn(depth int, prefix *string, maxKeys int32, levelCh chan<- *string, wg *sync.WaitGroup) {

	defer wg.Done()

	if depth == 0 {
		levelCh <- prefix
		return
	}
	continuationToken := ""
	delimiter := "/"

	for continuationToken != "End" {
		output, _ := c.listObjectFn(&continuationToken, prefix, &delimiter, maxKeys)

		log.Printf("Getting %d prefixes in /%s\n", len(output.CommonPrefixes), *prefix)

		if len(output.CommonPrefixes) == 0 {
			levelCh <- prefix
		} else {
			for _, cp := range output.CommonPrefixes {
				wg.Add(1)
				go c.listPrefixFn(depth-1, cp.Prefix, maxKeys, levelCh, wg)
			}
		}

	}
}

// ListCommonPrefixes is a function to list common prefixes.
func (c *S3Client) ListCommonPrefixes(depth int, maxKeys int32) (prefixes []*string) {
	log.Printf("List Prefixes /%s with depths %d\n", c.prefix, depth)
	var wg sync.WaitGroup

	if depth == 0 {
		prefixes = append(prefixes, &c.prefix)
		return
	}

	levelCh := make(chan *string, 10)
	wg.Add(1)
	go c.listPrefixFn(depth, &c.prefix, maxKeys, levelCh, &wg)
	wg.Wait()
	close(levelCh)

	for cp := range levelCh {
		log.Printf("Common Prefix /%s\n", *cp)
		prefixes = append(prefixes, cp)
	}
	return

}

// ListObjects is a function to list objects from Amazon S3
func (c *S3Client) ListObjects(continuationToken, prefix *string, maxKeys int32) ([]*Object, error) {

	// log.Printf("S3> list objects in bucket %s/%s from S3\n", c.bucket, *prefix)

	delimiter := ""

	output, err := c.listObjectFn(continuationToken, prefix, &delimiter, maxKeys)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	// TODO double check this.
	len := len(output.Contents)
	result := make([]*Object, len, len)

	for i, obj := range output.Contents {
		// log.Printf("key=%s size=%d", aws.ToString(obj.Key), obj.Size)
		result[i] = &Object{
			Key:  *obj.Key,
			Size: obj.Size,
		}
	}

	return result, nil

}

// HeadObject is a function to get extra metadata from Amazon S3
func (c *S3Client) HeadObject(key string) {
	log.Printf("S3> Get extra metadata info for %s\n", key)

	input := &s3.HeadObjectInput{}

	output, err := c.client.HeadObject(c.ctx, input)
	if err != nil {
		log.Fatal(err)
		// return err
	}

	log.Printf("S3> Content type is %s\n", *output.ContentType)

}

// PutObject is a function to put (upload) an object to Amazon S3
func (c *S3Client) PutObject(key string, body []byte, storageClass string) (etag *string, err error) {
	// log.Printf("S3> Uploading object %s to bucket %s\n", key, c.bucket)

	md5Bytes := md5.Sum(body)
	// contentMD5 := hex.EncodeToString(md5Bytes[:])
	contentMD5 := base64.StdEncoding.EncodeToString(md5Bytes[:])

	// fmt.Println(contentMD5)

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
		log.Printf("S3> Got an error uploading file - %s\n", err.Error())
		// return nil, err
	} else {
		_etag := strings.Trim(*output.ETag, "\"")
		etag = &_etag
		// fmt.Println(output.ETag)
	}

	return

}

// UploadPart is
func (c *S3Client) UploadPart(key string, uploadID *string, body []byte, partNumber int) (etag *string, err error) {
	// log.Printf("S3> Uploading part for %s with part number %d", key, partNumber)

	md5Bytes := md5.Sum(body)
	// contentMD5 := hex.EncodeToString(md5Bytes[:])
	contentMD5 := base64.StdEncoding.EncodeToString(md5Bytes[:])

	// fmt.Println(contentMD5)

	reader := bytes.NewReader(body)

	input := &s3.UploadPartInput{
		Bucket:     &c.bucket,
		Key:        &key,
		Body:       reader,
		PartNumber: int32(partNumber),
		UploadId:   uploadID,
		ContentMD5: &contentMD5,
	}

	output, err := c.client.UploadPart(c.ctx, input)
	if err != nil {
		log.Fatal(err)
		// return nil, err
	} else {
		_etag := strings.Trim(*output.ETag, "\"")
		etag = &_etag
		// fmt.Printf("S3> Upload Part (%d) completed - etag is %s\n", partNumber, *output.ETag)
	}

	return
}

// CompleteMultipartUpload is
func (c *S3Client) CompleteMultipartUpload(key string, uploadID *string, parts []*Part) (etag *string, err error) {
	log.Printf("S3> Complete Multipart Uploads for %s\n", key)

	// Need to convert drh.Part to types.CompletedPart
	// var completedPart []types.CompletedPart
	completedPart := make([]types.CompletedPart, len(parts))

	for i, part := range parts {
		cp := types.CompletedPart{
			PartNumber: int32(part.partNumber),
			ETag:       part.etag,
		}
		completedPart[i] = cp
	}
	// log.Println("Completed parts are:")
	// log.Println(completedPart)

	input := &s3.CompleteMultipartUploadInput{
		Bucket:          &c.bucket,
		Key:             &key,
		UploadId:        uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedPart},
	}

	output, err := c.client.CompleteMultipartUpload(c.ctx, input)
	if err != nil {
		log.Fatal(err)
	} else {
		// etag = output.ETag
		_etag := strings.Trim(*output.ETag, "\"")
		etag = &_etag
		// log.Printf("S3> Completed multipart uploads for %s - etag is %s\n", key, *output.ETag)
	}

	return

}

// CreateMultipartUpload is
func (c *S3Client) CreateMultipartUpload(key string) (uploadID *string, err error) {
	log.Printf("S3> Create Multipart Upload for %s\n", key)

	input := &s3.CreateMultipartUploadInput{
		Bucket: &c.bucket,
		Key:    &key,
		// StorageClass: "s",
		// metadata: "s",
	}

	output, err := c.client.CreateMultipartUpload(c.ctx, input)
	if err != nil {
		log.Fatal(err)
	} else {
		uploadID = output.UploadId
		// log.Printf("S3> Create Multipart Upload for %s - upload id is %s\n", key, *output.UploadId)
	}
	return
}

// ListParts is
func (c *S3Client) ListParts(key, uploadID string) {
	log.Printf("S3> List Parts from S3")

	input := &s3.ListPartsInput{
		Bucket:   &c.bucket,
		Key:      &key,
		UploadId: &uploadID,
	}

	output, err := c.client.ListParts(c.ctx, input)
	if err != nil {
		log.Fatal(err)
		// return nil, err
	}

	for _, part := range output.Parts {
		fmt.Println(part.PartNumber)
		fmt.Println(part.ETag)
	}

	// fmt.Printf("Completed multipart uploads - etag is %s\n", *output.UploadId)
	// return output.UploadId, nil

}

// ListMultipartUploads is
func (c *S3Client) ListMultipartUploads(key string) {
	log.Println("S3> ListMultipartUploads")

	input := &s3.ListMultipartUploadsInput{
		Bucket: &c.bucket,
		Prefix: &c.prefix,
		// ...
	}

	output, err := c.client.ListMultipartUploads(c.ctx, input)
	if err != nil {
		log.Fatal(err)
		// return nil, err
	}

	for _, upload := range output.Uploads {
		fmt.Println(upload.Key)
		fmt.Println(upload.UploadId)
	}
}

// AbortMultipartUpload is
func (c *S3Client) AbortMultipartUpload(key string, uploadID *string) {
	log.Println("S3> Abort Multipart Upload")

	input := &s3.AbortMultipartUploadInput{
		Bucket:   &c.bucket,
		Key:      &key,
		UploadId: uploadID,
	}

	_, err := c.client.AbortMultipartUpload(c.ctx, input)
	if err != nil {
		log.Fatal(err)
		// return nil, err
	}
	fmt.Printf("S3> Abort multipart upload for %s with upload id %s}", key, key)

	// for _, upload := range output.Uploads {
	// 	fmt.Println(upload.Key)
	// 	fmt.Println(upload.UploadId)
	// }
}

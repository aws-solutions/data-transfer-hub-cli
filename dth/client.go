/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dth

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Client is an interface used to contact with Cloud Storage Services
type Client interface {
	// READ
	HeadObject(ctx context.Context, key *string) *Metadata
	GetObject(ctx context.Context, key *string, size, start, chunkSize int64, version string) ([]byte, error)
	ListObjects(ctx context.Context, continuationToken, prefix *string, maxKeys int32) ([]*Object, error)
	ListCommonPrefixes(ctx context.Context, depth int, maxKeys int32) (prefixes []*string)
	ListParts(ctx context.Context, key, uploadID *string) (parts map[int]*Part)
	GetUploadID(ctx context.Context, key *string) (uploadID *string)
	ListSelectedPrefixes(ctx context.Context, key *string) (prefixes []*string)

	// WRITE
	PutObject(ctx context.Context, key *string, body []byte, storageClass, acl *string, meta *Metadata) (etag *string, err error)
	CreateMultipartUpload(ctx context.Context, key, storageClass, acl *string, meta *Metadata) (uploadID *string, err error)
	CompleteMultipartUpload(ctx context.Context, key, uploadID *string, parts []*Part) (etag *string, err error)
	UploadPart(ctx context.Context, key *string, body []byte, uploadID *string, partNumber int) (etag *string, err error)
	AbortMultipartUpload(ctx context.Context, key, uploadID *string) (err error)
	DeleteObject(ctx context.Context, key *string) (err error)
}

// S3Client is an implementation of Client interface for Amazon S3
type S3Client struct {
	bucket, prefix, prefixList, region, sourceType string
	client                                         *s3.Client
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

// NewS3Client creates a S3Client instance
func NewS3Client(ctx context.Context, bucket, prefix, prefixList, endpoint, region, sourceType string, cred *S3Credentials) *S3Client {

	cfg := loadDefaultConfig(ctx)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// retryer := retry.AddWithMaxBackoffDelay(retry.NewStandard(), time.Second*5)
		// o.Retryer = retryer
		if region != "" {
			o.Region = region
		}
		var url string
		if endpoint != "" {
			// if endpoint is provided, use the endpoint.
			if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
				url = endpoint
			} else {
				url = "https://" + endpoint // Default to https://
			}
		} else {
			url = getEndpointURL(region, sourceType)
		}

		if url != "" {
			log.Printf("S3> Source Endpoint URL is set to %s\n", url)
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
		prefixList: prefixList,
		client:     client,
		region:     region,
		sourceType: sourceType,
	}

}

// GetObject is a function to get (download) object from Amazon S3
func (c *S3Client) GetObject(ctx context.Context, key *string, size, start, chunkSize int64, version string) ([]byte, error) {
	// log.Printf("S3> Downloading %s with %d bytes start from %d\n", key, size, start)
	if size == 0 { // for prefix
		return nil, nil
	}
	bodyRange := fmt.Sprintf("bytes=%d-%d", start, start+chunkSize-1)
	input := &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    key,
		Range:  &bodyRange,
	}

	output, err := c.client.GetObject(ctx, input)
	if err != nil {
		log.Printf("S3> Unable to download %s with %d bytes start from %d - %s\n", *key, size, start, err.Error())
		return nil, err
	}

	defer output.Body.Close()

	// Read response body
	s, err := io.ReadAll(output.Body)

	if err != nil {
		log.Printf("S3> Unable to read the content of %s - %s\n", *key, err.Error())
		return nil, err
	}
	return s, nil

}

// Internal func to call API to list objects.
func (c *S3Client) listObjectFn(ctx context.Context, continuationToken, prefix, delimiter *string, maxKeys int32) (*s3.ListObjectsV2Output, error) {

	input := &s3.ListObjectsV2Input{
		Bucket:    &c.bucket,
		Prefix:    prefix,
		MaxKeys:   maxKeys,
		Delimiter: delimiter,
		EncodingType: "url",
	}

	if *continuationToken != "" {
		input.ContinuationToken = continuationToken
	}

	// start := time.Now()
	output, err := c.client.ListObjectsV2(ctx, input)
	if err != nil {
		log.Printf("Unable to list objects in /%s - %s\n", *prefix, err.Error())
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
func (c *S3Client) listPrefixFn(ctx context.Context, depth int, prefix *string, maxKeys int32, levelCh chan<- *string, listCh chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	listCh <- struct{}{}

	if depth == 0 {
		levelCh <- prefix
		return
	}
	continuationToken := ""
	delimiter := "/"

	i := 0

	for continuationToken != "End" {
		output, err := c.listObjectFn(ctx, &continuationToken, prefix, &delimiter, maxKeys)
		if err != nil {
			log.Fatalf("Failed to list prefixes in /%s for bucket %s, quit the process. Please try again later.", *prefix, c.bucket)
		}
		for _, cp := range output.CommonPrefixes {
			i++
			wg.Add(1)
			go c.listPrefixFn(ctx, depth-1, cp.Prefix, maxKeys, levelCh, listCh, wg)
		}

	}
	if i == 0 {
		levelCh <- prefix
	}
	<-listCh
}

// ListCommonPrefixes is a function to list common prefixes.
func (c *S3Client) ListCommonPrefixes(ctx context.Context, depth int, maxKeys int32) (prefixes []*string) {
	log.Printf("List common prefixes from /%s with depths %d\n", c.prefix, depth)
	var wg sync.WaitGroup

	if depth == 0 {
		prefixes = append(prefixes, &c.prefix)
		return
	}

	// Maximum around 100K
	levelCh := make(chan *string, 1<<20)

	// Restrict the number of list func happened concurrently.
	listCh := make(chan struct{}, 200)

	wg.Add(1)
	go c.listPrefixFn(ctx, depth, &c.prefix, maxKeys, levelCh, listCh, &wg)
	wg.Wait()
	close(levelCh)
	close(listCh)

	for cp := range levelCh {
		log.Printf("Common Prefix /%s\n", *cp)
		prefixes = append(prefixes, cp)
	}
	return

}

// ListObjects is a function to list objects from Amazon S3
func (c *S3Client) ListObjects(ctx context.Context, continuationToken, prefix *string, maxKeys int32) ([]*Object, error) {

	// log.Printf("S3> list objects in bucket %s/%s from S3\n", c.bucket, *prefix)
	delimiter := ""

	output, err := c.listObjectFn(ctx, continuationToken, prefix, &delimiter, maxKeys)
	if err != nil {
		log.Printf("S3> Unable to list object in /%s - %s\n", *prefix, err.Error())
		return nil, err
	}

	length := len(output.Contents)
	result := make([]*Object, 0, length)

	for _, obj := range output.Contents {
		// log.Printf("key=%s size=%d", *obj.Key, obj.Size)
		if obj.StorageClass == "GLACIER" || obj.StorageClass == "DEEP_ARCHIVE" {
			continue
		}
		escapedPrefix, err := url.QueryUnescape(*obj.Key)
		if err != nil {
			escapedPrefix = *obj.Key
		}
		result = append(result, &Object{
			Key:  escapedPrefix,
			Size: obj.Size,
		})
	}

	return result, nil

}

// HeadObject is a function to get extra metadata from Amazon S3
func (c *S3Client) HeadObject(ctx context.Context, key *string) *Metadata {
	// log.Printf("S3> Get extra metadata info for %s\n", *key)

	input := &s3.HeadObjectInput{
		Bucket: &c.bucket,
		Key:    key,
	}

	output, err := c.client.HeadObject(ctx, input)
	if err != nil {
		log.Printf("Failed to head object for %s - %s\n", *key, err.Error())
		return nil
	}

	return &Metadata{
		ContentType:     output.ContentType,
		ContentLanguage: output.ContentLanguage,
		ContentEncoding: output.ContentEncoding,
		CacheControl:    output.CacheControl,
		Metadata:        output.Metadata,
	}

}

// ListSelectedPrefixes is a function to list prefixes from a customized list file.
func (c *S3Client) ListSelectedPrefixes(ctx context.Context, key *string) (prefixes []*string) {

	downloader := manager.NewDownloader(c.client)

	getBuf := manager.NewWriteAtBuffer([]byte{})

	input := &s3.GetObjectInput{
		Bucket: &c.bucket,
		Key:    key,
	}

	dounload_start := time.Now()
	log.Printf("Start downloading the Prefix List File.")
	_, err := downloader.Download(ctx, getBuf, input)
	download_end := time.Since(dounload_start)
	if err != nil {
		fmt.Print(err)
	} else {
		log.Printf("Download the Prefix List File Completed in %v\n", download_end)
	}
	start := time.Now()
	prefixes_value := make([]string, 0, 100000000)

	for i, m := range strings.Split(string(getBuf.Bytes()), "\n") {
		if i > 100000000 {
			log.Printf("The number of prefixes in the list file is larger than 100,000,000, please seperate the file.")
			return
		}
		if len(m) > 0 {
			prefixes_value = append(prefixes_value, m)
			prefixes = append(prefixes, &prefixes_value[i])
		}
	}
	log.Printf("Got %d prefixes from the customized list file.", len(prefixes))
	end := time.Since(start)
	log.Printf("Getting Prefixes List Job Completed in %v\n", end)
	return
}

// PutObject is a function to put (upload) an object to Amazon S3
func (c *S3Client) PutObject(ctx context.Context, key *string, body []byte, storageClass, acl *string, meta *Metadata) (etag *string, err error) {
	// log.Printf("S3> Uploading object %s to bucket %s\n", key, c.bucket)

	md5Bytes := md5.Sum(body)
	// contentMD5 := hex.EncodeToString(md5Bytes[:])
	contentMD5 := base64.StdEncoding.EncodeToString(md5Bytes[:])
	// fmt.Println(contentMD5)
	reader := bytes.NewReader(body)

	if *acl == "" {
		*acl = string(types.ObjectCannedACLBucketOwnerFullControl)
	}

	input := &s3.PutObjectInput{
		Bucket:       &c.bucket,
		Key:          key,
		Body:         reader,
		ContentMD5:   &contentMD5,
		StorageClass: types.StorageClass(*storageClass),
		ACL:          types.ObjectCannedACL(*acl),
	}
	if meta != nil {
		input.ContentType = meta.ContentType
		input.ContentEncoding = meta.ContentEncoding
		input.ContentLanguage = meta.ContentLanguage
		input.CacheControl = meta.CacheControl
		input.Metadata = meta.Metadata
	}

	output, err := c.client.PutObject(ctx, input)
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

// DeleteObject is to abort failed multipart upload
func (c *S3Client) DeleteObject(ctx context.Context, key *string) (err error) {
	log.Printf("S3> Delete Object %s from Bucket %s\n", *key, c.bucket)

	input := &s3.DeleteObjectInput{
		Bucket: &c.bucket,
		Key:    key,
	}
	_, err = c.client.DeleteObject(ctx, input)
	if err != nil {
		log.Printf("S3> Failed to delete object %s - %s\n", *key, err.Error())
		return err
	}
	return nil
}

// CreateMultipartUpload is a function to initilize a multipart upload process.
// This func returns an upload ID used to indicate the multipart upload.
// All parts will be uploaded with this upload ID, after that, all parts by this ID will be combined to create the full object.
func (c *S3Client) CreateMultipartUpload(ctx context.Context, key, storageClass, acl *string, meta *Metadata) (uploadID *string, err error) {
	// log.Printf("S3> Create Multipart Upload for %s\n", *key)
	if *acl == "" {
		*acl = string(types.ObjectCannedACLBucketOwnerFullControl)
	}

	input := &s3.CreateMultipartUploadInput{
		Bucket:       &c.bucket,
		Key:          key,
		StorageClass: types.StorageClass(*storageClass),
		ACL:          types.ObjectCannedACL(*acl),
	}
	if meta != nil {
		input.ContentType = meta.ContentType
		input.ContentEncoding = meta.ContentEncoding
		input.ContentLanguage = meta.ContentLanguage
		input.CacheControl = meta.CacheControl
		input.Metadata = meta.Metadata
	}

	output, err := c.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		log.Printf("S3> Unable to create multipart upload for %s - %s\n", *key, err.Error())
	} else {
		uploadID = output.UploadId
		// log.Printf("S3> Create Multipart Upload for %s - upload id is %s\n", key, *output.UploadId)
	}
	return
}

// UploadPart is
func (c *S3Client) UploadPart(ctx context.Context, key *string, body []byte, uploadID *string, partNumber int) (etag *string, err error) {
	// log.Printf("S3> Uploading part for %s with part number %d", key, partNumber)

	md5Bytes := md5.Sum(body)
	// contentMD5 := hex.EncodeToString(md5Bytes[:])
	contentMD5 := base64.StdEncoding.EncodeToString(md5Bytes[:])

	// fmt.Println(contentMD5)

	reader := bytes.NewReader(body)

	input := &s3.UploadPartInput{
		Bucket:     &c.bucket,
		Key:        key,
		Body:       reader,
		PartNumber: int32(partNumber),
		UploadId:   uploadID,
		ContentMD5: &contentMD5,
	}

	output, err := c.client.UploadPart(ctx, input)
	if err != nil {
		log.Printf("S3> Failed to upload part for %s - %s\n", *key, err.Error())
		// return nil, err
	} else {
		_etag := strings.Trim(*output.ETag, "\"")
		etag = &_etag
		// log.Printf("S3> Upload Part (%d) completed - etag is %s\n", partNumber, _etag)
	}

	return
}

// CompleteMultipartUpload is a function to combine all parts uploaded and create the full object.
func (c *S3Client) CompleteMultipartUpload(ctx context.Context, key, uploadID *string, parts []*Part) (etag *string, err error) {
	// log.Printf("S3> Complete Multipart Uploads for %s\n", key)

	// Need to convert dth.Part to types.CompletedPart
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
		Key:             key,
		UploadId:        uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedPart},
	}

	output, err := c.client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		log.Printf("S3> Unable to complete multipart upload for %s - %s\n", *key, err.Error())
	} else {
		// etag = output.ETag
		_etag := strings.Trim(*output.ETag, "\"")
		etag = &_etag
		// log.Printf("S3> Completed multipart uploads for %s - etag is %s\n", key, *output.ETag)
	}

	return

}

// ListParts returns list of parts by upload ID in a map
func (c *S3Client) ListParts(ctx context.Context, key, uploadID *string) (parts map[int]*Part) {
	// log.Printf("S3> List Parts for %s - with upload ID %s\n", *key, *uploadID)
	input := &s3.ListPartsInput{
		Bucket:   &c.bucket,
		Key:      key,
		UploadId: uploadID,
		MaxParts: 1000,
	}

	parts = make(map[int]*Part, 10000)

	for {
		output, err := c.client.ListParts(ctx, input)
		if err != nil {
			log.Printf("Failed to list parts for %s - %s\n", *key, err.Error())
			break
		}

		for _, part := range output.Parts {
			// log.Printf("Found Part %d - etag %s", part.PartNumber, *part.ETag)
			etag := strings.Trim(*part.ETag, "\"")
			parts[int(part.PartNumber)] = &Part{
				partNumber: int(part.PartNumber),
				etag:       &etag,
			}
		}
		if !output.IsTruncated {
			break
		}
		input.PartNumberMarker = output.NextPartNumberMarker
	}
	log.Printf("Totally %d part(s) found for %s\n", len(parts), *key)

	return
}

// GetUploadID use ListMultipartUploads to get the last unfinished upload ID by key
func (c *S3Client) GetUploadID(ctx context.Context, key *string) (uploadID *string) {
	// log.Printf("S3> Get upload ID for %s\n", *key)

	input := &s3.ListMultipartUploadsInput{
		Bucket: &c.bucket,
		Prefix: key, // Limit to the key only
		// MaxUploads: 1,
	}

	output, err := c.client.ListMultipartUploads(ctx, input)
	if err != nil {
		log.Printf("S3> Failed to list multipart uploads - %s\n", err.Error())
		return nil
	}

	// for _, upload := range output.Uploads {
	// 	log.Printf("Found upload ID is %s for object %s - time %v\n", *upload.UploadId, *upload.Key, *upload.Initiated)
	// 	c.AbortMultipartUpload(ctx, key, upload.UploadId)
	// }

	if output.Uploads != nil {
		return output.Uploads[len(output.Uploads)-1].UploadId
	}
	return nil
}

// AbortMultipartUpload is to abort failed multipart upload
func (c *S3Client) AbortMultipartUpload(ctx context.Context, key, uploadID *string) (err error) {
	// log.Printf("S3> Abort multipart upload for %s with upload id %s\n", key, *uploadID)

	input := &s3.AbortMultipartUploadInput{
		Bucket:   &c.bucket,
		Key:      key,
		UploadId: uploadID,
	}
	_, err = c.client.AbortMultipartUpload(ctx, input)
	if err != nil {
		log.Printf("S3> Failed to abort multipart upload for %s - %s\n", *key, err.Error())
		return err
	}

	return nil
}

func urlEncodePath(path string) string {
	return (&url.URL{Path: path}).EscapedPath()
}
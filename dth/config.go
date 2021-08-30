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
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	mw "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/smithy-go/middleware"
)

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
	IncludeMetadata                                                                          bool
}

// JobConfig is General Job Info
type JobConfig struct {
	SrcType, SrcBucket, SrcPrefix, SrcPrefixList, SrcRegion, SrcEndpoint, SrcCredential string
	DestBucket, DestPrefix, DestRegion, DestCredential, DestStorageClass, DestAcl       string
	JobTableName, JobQueueName                                                          string
	SrcInCurrentAccount, DestInCurrentAccount, SkipCompare                              bool
	*JobOptions
}

// loadDefaultConfig reads the SDK's default external configurations with custom user agent for API tracking purpose
func loadDefaultConfig(ctx context.Context) (cfg aws.Config) {
	cfg, err := config.LoadDefaultConfig(
		ctx,
		// config.WithClientLogMode(aws.LogRequest|aws.LogRequestWithBody),
		config.WithAPIOptions([]func(*middleware.Stack) error{
			mw.AddUserAgentKey("AWSSOLUTION/SO8001/v2.0.0"),
		}),
	)
	if err != nil {
		log.Fatalf("Failed to load default SDK config to create client - %s\n", err.Error())
	}
	return cfg
}

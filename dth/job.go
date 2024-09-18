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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	// Ignore do nothing
	Ignore = iota

	// Delete is an action to delete an object
	Delete

	// Transfer is an action to transfer an object
	Transfer

	// TransferSinglePart is an action to transfer a single part of an giant object
	TransferSinglePart
)

// Job is an interface of a process to run by this tool
// A Job must have a Run() method
type Job interface {
	Run(ctx context.Context)
}

// Finder is an implemenation of Job interface
// Finder compares the differences of source and destination and sends the delta to SQS
type Finder struct {
	srcClient, desClient, ec2RoleS3Client Client
	sqs                                   *SqsService
	cfg                                   *JobConfig
	sfn                                   *SfnService
}

// Worker is an implemenation of Job interface
// Worker is used to consume the messages from SQS and start the transferring
type Worker struct {
	srcClient, desClient Client
	cfg                  *JobConfig
	sqs                  *SqsService
	db                   *DBService
	splitPartDb          *DBService
	sfn                  *SfnService
}

// TransferResult stores the result after transfer.
type TransferResult struct {
	status          string
	splitPartsCount int
	etag            *string
	err             error
}

type PartTransferJobParams struct {
	SrcBucket       string
	DstBucket       string
	ObjectKey       string
	PartNumber      int
	TotalPartsCount int
	StartByte       int64
	EndByte         int64
	UploadID        string
	BodyRange       string
}

type PartTransferResult struct {
	ObjectKey       string
	PartNumber      int
	TotalPartsCount int
	UploadID        string
	ETag            string
	Status          string
	Error           string
}

// helper function to check credentials
func getCredentials(ctx context.Context, param string, inCurrentAccount bool, sm *SecretService) *S3Credentials {
	cred := &S3Credentials{
		noSignRequest: false,
	}

    if param == "" {
        // no credential is required if not in current account
        cred.noSignRequest = !inCurrentAccount
    } else {
        credStr := sm.GetSecret(ctx, &param)
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
	return cred
}

// NewFinder creates a new Finder instance
func NewFinder(ctx context.Context, cfg *JobConfig) (f *Finder) {
	sqs, _ := NewSqsService(ctx, cfg.JobQueueName)
	sfn, _ := NewSfnService(ctx, cfg.SfnArn)
	sm, err := NewSecretService(ctx)
	if err != nil {
		log.Printf("Warning - Unable to load credentials, use default setting - %s\n", err.Error())
	}

	srcCred := getCredentials(ctx, cfg.SrcCredential, cfg.SrcInCurrentAccount, sm)
	desCred := getCredentials(ctx, cfg.DestCredential, cfg.DestInCurrentAccount, sm)

	srcClient := NewS3Client(ctx, cfg.SrcBucket, cfg.SrcPrefix, cfg.SrcPrefixList, cfg.SrcEndpoint, cfg.SrcRegion, cfg.SrcType, srcCred)
	desClient := NewS3Client(ctx, cfg.DestBucket, cfg.DestPrefix, "", "", cfg.DestRegion, "Amazon_S3", desCred)

	ec2RoleS3Client := NewS3ClientWithEC2Role(ctx, cfg.SrcPrefixListBucket, cfg.SrcPrefixList)

	if srcClient != nil {
		srcClient.isSrcClient = true
	}

	if cfg.PayerRequest {
		srcClient.isPayerRequest = true
	}

	SRC_CRED = srcCred
	DST_CRED = desCred

	f = &Finder{
		srcClient:       srcClient,
		desClient:       desClient,
		ec2RoleS3Client: ec2RoleS3Client,
		sfn:             sfn,
		sqs:             sqs,
		cfg:             cfg,
	}
	return
}

func RunTicker(ctx context.Context, cfg *JobConfig) {
	t := &JobTicker{}
	t.updateTimer()
	for {
		<-t.timer.C
		log.Printf("ticker update S3 Credentials")
		updateCreds(ctx, cfg)
		t.updateTimer()
	}
}

func updateCreds(ctx context.Context, cfg *JobConfig) {
	if cfg == nil {
		log.Print("nil config")
		return
	}
	sm, err := NewSecretService(ctx)
	if err != nil {
		log.Printf("Warning - Unable to load credentials, use default setting - %s\n", err.Error())
	}
	SRC_CRED = getCredentials(ctx, cfg.SrcCredential, cfg.SrcInCurrentAccount, sm)
	DST_CRED = getCredentials(ctx, cfg.DestCredential, cfg.DestInCurrentAccount, sm)
	log.Print("src_cred and dst_cred has been updated")
}

// Run is main execution function for Finder.
func (f *Finder) Run(ctx context.Context) {

	if !f.sqs.IsQueueEmpty(ctx) {
		log.Fatalf("Queue might not be empty ... Please try again later")
	}

	if !f.sfn.IsNoRunningTask(ctx) {
		log.Fatalf("There might still be ongoing tasks to merge giant objects ...  Please try again later")
	}

	// Maximum number of queued batches to be sent to SQS
	var bufferSize int = 500

	// Assume sending messages is slower than listing and comparing
	// Create a channel to block the process not to generate too many messages to be sent.
	batchCh := make(chan struct{}, bufferSize)

	// Channel to buffer the messages
	msgCh := make(chan *string, bufferSize*f.cfg.MessageBatchSize)

	// Maximum number of finder threads in parallel
	// Create a channel to block
	// Note that bigger number needs more memory
	compareCh := make(chan struct{}, f.cfg.FinderNumber)

	var prefixes []*string
	log.Printf("Prefix List File: %s", f.cfg.SrcPrefixList)

	if f.cfg.PayerRequest {
		log.Printf("Enable Payer Request Mode")
	}

	if f.cfg.SrcPrefixListBucket != "" && len(f.cfg.SrcPrefixList) > 0 {
		prefixes = f.ec2RoleS3Client.ListSelectedPrefixesFromThirdBucket(ctx, &f.cfg.SrcPrefixListBucket, &f.cfg.SrcPrefixList)
	} else if len(f.cfg.SrcPrefixList) > 0 {
		prefixes = f.srcClient.ListSelectedPrefixes(ctx, &f.cfg.SrcPrefixList)
	} else {
		prefixes = f.srcClient.ListCommonPrefixes(ctx, f.cfg.FinderDepth, f.cfg.MaxKeys)
	}
	var wg sync.WaitGroup
	var totalObjCount int64 = 0

	start := time.Now()

	for _, p := range prefixes {
		compareCh <- struct{}{}
		log.Printf("prefix: %s", *p)
		wg.Add(1)
		if f.cfg.SkipCompare {
			go f.directSend(ctx, p, batchCh, msgCh, compareCh, &wg, &totalObjCount)
		} else {
			go f.compareAndSend(ctx, p, batchCh, msgCh, compareCh, &wg, &totalObjCount)
		}
	}
	wg.Wait()

	close(batchCh)
	close(msgCh)
	close(compareCh)

	end := time.Since(start)
	log.Printf("Finder Job Completed in %v, found %d objects in total\n", end, totalObjCount)
}

// List objects in destination bucket, load the full list into a map
func (f *Finder) getTargetObjects(ctx context.Context, prefix *string) (objects map[string]*int64) {

	destPrefix := appendPrefix(prefix, &f.cfg.DestPrefix)
	log.Printf("Scanning in destination prefix /%s\n", *destPrefix)

	token := ""
	objects = make(map[string]*int64, 1<<17)

	i := 0
	batch := 10
	for token != "End" {
		tar, err := f.desClient.ListObjects(ctx, &token, destPrefix, f.cfg.MaxKeys)
		if err != nil {
			log.Fatalf("Error listing objects in destination bucket - %s\n", err.Error())
		}
		// fmt.Printf("Size is %d\n", len(jobs))
		// fmt.Printf("Token is %s\n", token)

		for _, obj := range tar {
			// fmt.Printf("key is %s, size is %d\n", job.Key, job.Size)
			srcKey := removePrefix(&obj.Key, &f.cfg.DestPrefix)
			objects[*srcKey] = &obj.Size
		}
		i++
		if (i % batch) == 0 {
			log.Printf("Scanned %d objects...", i*1000)
		}
	}
	log.Printf("Totally %d objects in destination prefix /%s\n", len(objects), *destPrefix)
	return
}

// This function will compare source and target and get a list of delta,
// and then send delta to SQS Queue.
func (f *Finder) compareAndSend(ctx context.Context, prefix *string, batchCh chan struct{}, msgCh chan *string, compareCh chan struct{}, wg *sync.WaitGroup, totalObjCount *int64) {
	defer wg.Done()

	log.Printf("Comparing within prefix /%s\n", *prefix)
	target := f.getTargetObjects(ctx, prefix)

	token := ""
	i, j := 0, 0
	var objCount int64 = 0
	retry := 0
	// batch := make([]*string, f.cfg.MessageBatchSize)

	log.Printf("Start comparing and sending...\n")
	// start := time.Now()

	for token != "End" {
		// source := f.getSourceObjects(ctx, &token, prefix)
		source, err := f.srcClient.ListObjects(ctx, &token, prefix, f.cfg.MaxKeys)
		if err != nil {
			log.Printf("Fail to get source list - %s\n", err.Error())
			//
			log.Printf("Sleep for 1 minute and try again...")
			retry++

			if retry <= MaxRetries {
				time.Sleep(time.Minute * 1)
				continue
			} else {
				log.Printf("Still unable to list source list after %d retries\n", MaxRetries)
				// Log the last token and exit
				log.Fatalf("The last token is %s\n", token)
			}

		}

		// if a successful list, reset to 0
		retry = 0

		for _, obj := range source {
			// TODO: Check if there is another way to compare
			// Currently, map is used to search if such object exists in target
			if tsize, found := target[obj.Key]; !found || *tsize != obj.Size {
				// log.Printf("Find a difference %s - %d\n", key, size)
				// batch[i] = obj.toString()
				msgCh <- obj.toString()
				i++
				if i%f.cfg.MessageBatchSize == 0 {
					wg.Add(1)
					objCount += int64(f.cfg.MessageBatchSize)
					j++
					if j%100 == 0 {
						log.Printf("Found %d batches in prefix /%s\n", j, *prefix)
					}
					batchCh <- struct{}{}

					// start a go routine to send messages in batch
					go func(i int) {
						defer wg.Done()
						batch := make([]*string, i)
						for a := 0; a < i; a++ {
							batch[a] = <-msgCh
						}

						f.sqs.SendMessageInBatch(ctx, batch)
						<-batchCh
					}(f.cfg.MessageBatchSize)
					i = 0
				}
			}
		}
	}
	// For remainning objects.
	if i != 0 {
		j++
		objCount += int64(i)
		wg.Add(1)
		batchCh <- struct{}{}
		go func(i int) {
			defer wg.Done()
			batch := make([]*string, i)
			for a := 0; a < i; a++ {
				batch[a] = <-msgCh
			}

			f.sqs.SendMessageInBatch(ctx, batch)
			<-batchCh
		}(i)
	}
	atomic.AddInt64(totalObjCount, objCount)

	// end := time.Since(start)
	// log.Printf("Compared and Sent %d batches in %v", j, end)
	log.Printf("Completed in prefix /%s, found %d batches (%d objects) in total", *prefix, j, objCount)
	<-compareCh
}

// This function will send the task to SQS Queue directly, without comparison.
func (f *Finder) directSend(ctx context.Context, prefix *string, batchCh chan struct{}, msgCh chan *string, compareCh chan struct{}, wg *sync.WaitGroup, totalObjCount *int64) {
	defer wg.Done()

	log.Printf("Scanning prefix /%s\n", *prefix)

	token := ""
	i, j := 0, 0
	var objCount int64 = 0
	retry := 0
	// batch := make([]*string, f.cfg.MessageBatchSize)

	log.Printf("Start sending without comparison ...\n")
	// start := time.Now()

	for token != "End" {
		// source := f.getSourceObjects(ctx, &token, prefix)
		source, err := f.srcClient.ListObjects(ctx, &token, prefix, f.cfg.MaxKeys)
		if err != nil {
			log.Printf("Fail to get source list - %s\n", err.Error())
			//
			log.Printf("Sleep for 1 minute and try again...")
			retry++

			if retry <= MaxRetries {
				time.Sleep(time.Minute * 1)
				continue
			} else {
				log.Printf("Still unable to list source list after %d retries\n", MaxRetries)
				// Log the last token and exit
				log.Fatalf("The last token is %s\n", token)
			}

		}

		// if a successful list, reset to 0
		retry = 0

		for _, obj := range source {
			// TODO: Check if there is another way to compare
			// Currently, map is used to search if such object exists in target
			msgCh <- obj.toString()
			i++
			if i%f.cfg.MessageBatchSize == 0 {
				wg.Add(1)
				objCount += int64(f.cfg.MessageBatchSize)
				j++
				if j%100 == 0 {
					log.Printf("Found %d batches in prefix /%s\n", j, *prefix)
				}
				batchCh <- struct{}{}

				// start a go routine to send messages in batch
				go func(i int) {
					defer wg.Done()
					batch := make([]*string, i)
					for a := 0; a < i; a++ {
						batch[a] = <-msgCh
					}

					f.sqs.SendMessageInBatch(ctx, batch)
					<-batchCh
				}(f.cfg.MessageBatchSize)
				i = 0
			}
		}
	}
	// For remainning objects.
	if i != 0 {
		j++
		objCount += int64(i)
		wg.Add(1)
		batchCh <- struct{}{}
		go func(i int) {
			defer wg.Done()
			batch := make([]*string, i)
			for a := 0; a < i; a++ {
				batch[a] = <-msgCh
			}

			f.sqs.SendMessageInBatch(ctx, batch)
			<-batchCh
		}(i)
	}
	atomic.AddInt64(totalObjCount, objCount)

	// end := time.Since(start)
	// log.Printf("Compared and Sent %d batches in %v", j, end)
	log.Printf("Completed in prefix /%s, found %d batches (%d objects) in total", *prefix, j, objCount)
	<-compareCh
}

// NewWorker creates a new Worker instance
func NewWorker(ctx context.Context, cfg *JobConfig) (w *Worker) {
	log.Printf("Source Type is %s\n", cfg.SrcType)
	sqs, _ := NewSqsService(ctx, cfg.JobQueueName)

	db, _ := NewDBService(ctx, cfg.JobTableName)
	splitPartDb, _ := NewDBService(ctx, cfg.SinglePartTableName)

	sfn, _ := NewSfnService(ctx, cfg.SfnArn)

	sm, err := NewSecretService(ctx)
	if err != nil {
		log.Printf("Warning - Unable to load credentials, use default setting - %s\n", err.Error())
	}

	srcCred := getCredentials(ctx, cfg.SrcCredential, cfg.SrcInCurrentAccount, sm)
	desCred := getCredentials(ctx, cfg.DestCredential, cfg.DestInCurrentAccount, sm)

	srcClient := NewS3Client(ctx, cfg.SrcBucket, cfg.SrcPrefix, cfg.SrcPrefixList, cfg.SrcEndpoint, cfg.SrcRegion, cfg.SrcType, srcCred)
	desClient := NewS3Client(ctx, cfg.DestBucket, cfg.DestPrefix, "", "", cfg.DestRegion, "Amazon_S3", desCred)

	if srcClient != nil {
		srcClient.isSrcClient = true
	}

	if cfg.PayerRequest {
		srcClient.isPayerRequest = true
	}

	SRC_CRED = srcCred
	DST_CRED = desCred

	return &Worker{
		srcClient:   srcClient,
		desClient:   desClient,
		sqs:         sqs,
		db:          db,
		splitPartDb: splitPartDb,
		sfn:         sfn,
		cfg:         cfg,
	}
}

// Run a Worker job
func (w *Worker) Run(ctx context.Context) {
	// log.Println("Start Worker Job...")

	buffer := w.cfg.WorkerNumber
	if buffer <= 0 {
		buffer = 1 // Minimum 1
	}
	if buffer > 100 {
		buffer = 100 // Maximum 100
	}

	// A channel to block number of messages to be processed
	// Buffer size is cfg.WorkerNumber
	processCh := make(chan struct{}, buffer)

	// Channel to block number of objects/parts to be processed.
	// Buffer size is cfg.WorkerNumber * 2 - 1 (More buffer for multipart upload)
	transferCh := make(chan struct{}, buffer*2-1)

	for {
		msg, rh := w.sqs.ReceiveMessages(ctx)

		if msg == nil {
			log.Println("No messages, sleep...")
			time.Sleep(time.Second * 60)
			continue
		}

		obj, action := w.processMessage(ctx, msg, rh)
		if obj == nil { // Empty message
			continue
		}

		destKey := appendPrefix(&obj.Key, &w.cfg.DestPrefix)

		if action == Transfer || action == TransferSinglePart {
			processCh <- struct{}{}
			go w.startMigration(ctx, obj, rh, destKey, transferCh, processCh)
		}
		if action == Delete {
			processCh <- struct{}{}
			go w.startDelete(ctx, obj, rh, destKey, processCh)
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
	} else if strings.Contains(*msg, `"bodyRange":`) {
		// Simply check if msg body contains "bodyRange" to determine if it's a message for single part transfer
		singlePartTransferEvent := newSinglePartTransferEvent(msg)
		obj = new(Object)
		obj.Key = singlePartTransferEvent.ObjectKey // we do not need to use unescape again
		obj.Size = 0                                // set 0 for single part transfer
		obj.PartNumber = singlePartTransferEvent.PartNumber
		obj.TotalPartsCount = singlePartTransferEvent.TotalPartsCount
		obj.UploadID = singlePartTransferEvent.UploadID
		obj.BodyRange = singlePartTransferEvent.BodyRange
		action = TransferSinglePart
	} else {
		obj = newObject(msg)
		action = Transfer
	}
	return
}

// startMigration is a function to migrate an object from source to destination
func (w *Worker) startMigration(ctx context.Context, obj *Object, rh, destKey *string, transferCh chan struct{}, processCh <-chan struct{}) {

	ctx1, cancelHB := context.WithCancel(ctx)

	log.Printf("Migrating from %s/%s to %s/%s\n", w.cfg.SrcBucket, obj.Key, w.cfg.DestBucket, *destKey)

	// Start a heart beat
	go w.heartBeat(ctx1, &obj.Key, rh)

	var res *TransferResult

	// handle single part transfer
	if obj.BodyRange != "" {
		// Log in DynamoDB
		w.splitPartDb.PutSinglePartItem(ctx, obj)
		res = w.migrateSinglePart(ctx, obj, destKey, transferCh)
	} else {
		// Log in DynamoDB
		w.db.PutItem(ctx, obj)

		if obj.Size <= int64(w.cfg.MultipartThreshold*MB) {
			res = w.migrateSmallFile(ctx, obj, destKey, transferCh)
		} else if obj.Size < int64(w.cfg.GiantFileThreshold*MB) {
			res = w.migrateBigFile(ctx, obj, destKey, transferCh)
		} else {
			log.Printf("Migrating GiantFile with size %d\n", obj.Size)
			res = w.migrateGiantFile(ctx, obj, destKey, transferCh)
		}
	}

	w.processResult(ctx, obj, rh, res)

	// Cancel heart beat once done.
	cancelHB()

	<-processCh

}

// startDelete is a function to delete an object from destination
func (w *Worker) startDelete(ctx context.Context, obj *Object, rh, destKey *string, processCh <-chan struct{}) {
	// log.Printf("Delete object from %s/%s\n", w.cfg.DestBucket, obj.Key)

	// Currently, only Sequencer is updated with the latest one, no other info logged for delete action
	// This might be changed in future for debug purpose
	w.db.UpdateSequencer(ctx, &obj.Key, &obj.Sequencer)

	err := w.desClient.DeleteObject(ctx, destKey)
	if err != nil {
		log.Printf("Failed to delete object from %s/%s - %s\n", w.cfg.DestBucket, *destKey, err.Error())
	} else {
		w.sqs.DeleteMessage(ctx, rh)
		log.Printf("----->Deleted 1 object %s successfully\n", *destKey)
	}
	<-processCh

}

// processResult is a function to process transfer result, including delete the message, log to DynamoDB
// For status == PART_DONE, we do not update the main DDB, it will be updated by multi-part controller Lambda
func (w *Worker) processResult(ctx context.Context, obj *Object, rh *string, res *TransferResult) {
	// log.Println("Processing result...")

	if res.status == "SPLIT_DONE" {
		log.Printf("----->Split 1 object %s into %d parts\n", obj.Key, res.splitPartsCount)
		w.db.UpdateItem(ctx, &obj.Key, res)
	} else if res.status == "PART_DONE" || res.status == "PART_ERROR" || res.status == "PART_CANCEL" {
		log.Printf("----->(Multi-part) Transferred 1 part %s with status %s\n", obj.Key, res.status)
		w.splitPartDb.UpdateSinglePartItem(ctx, obj, res)
	} else {
		log.Printf("----->Transferred 1 object %s with status %s\n", obj.Key, res.status)
		w.db.UpdateItem(ctx, &obj.Key, res)
	}

	if res.status == "ERROR" {
		if strings.Contains(res.err.Error(), "403") {
			log.Printf("Authentication failed, will update credentials")
			updateCreds(ctx, w.cfg)
		}
	}

	if res.status == "DONE" || res.status == "CANCEL" || res.status == "SPLIT_DONE" ||
		res.status == "PART_DONE" || res.status == "PART_CANCEL" {
		w.sqs.DeleteMessage(ctx, rh)
	}
}

// heartBeat is to extend the visibility timeout before timeout happends
func (w *Worker) heartBeat(ctx context.Context, key, rh *string) {
	timeout := 15 // Assume default time out is 15 minutes

	// log.Printf("Heart Beat %d for %s", 1, *key)
	interval := 60
	i := 1
	time.Sleep(time.Second * 50) // 10 seconds ahead, buffer for api call
	for {
		select {
		case <-ctx.Done():
			// log.Printf("Received Cancel of heart beat for %s", *key)
			return
		default:
			i++
			// log.Printf("Heart Beat %d for %s", i, *key)
			if i%timeout == 0 {
				sec := int32((i + timeout) * interval)
				log.Printf("Change timeout for %s to %d seconds", *key, sec)
				w.sqs.ChangeVisibilityTimeout(ctx, rh, sec)
			}

			time.Sleep(time.Second * time.Duration(interval))
		}
	}
}

// Internal func to deal with the transferring of small file.
// Simply transfer the whole object
func (w *Worker) migrateSmallFile(ctx context.Context, obj *Object, destKey *string, transferCh chan struct{}) *TransferResult {

	// Add a transferring record
	transferCh <- struct{}{}

	var meta *Metadata
	if w.cfg.IncludeMetadata {
		meta = w.srcClient.HeadObject(ctx, &obj.Key)
	}

	result := w.transfer(ctx, obj, destKey, 0, obj.Size, nil, 0, meta)
	// log.Printf("Completed the transfer of %s with etag %s\n", obj.Key, *result.etag)

	// Remove the transferring record  after transfer is completed
	<-transferCh

	return result

}

// Internal func to deal with the transferring of a single part of a large file.
// Simply transfer a single part
func (w *Worker) migrateSinglePart(ctx context.Context, obj *Object, destKey *string, transferCh chan struct{}) *TransferResult {

	// Add a transferring record
	transferCh <- struct{}{}

	result := w.transferSinglePart(ctx, obj, destKey)

	// Remove the transferring record  after transfer is completed
	<-transferCh

	return result
}

// Internal func to calculate the appropriate part size for cluster concurrent transfer.
// Considering the size of the cluster and the maximum size of a single S3 object is 5TB,
// set the maximum number of part fragments to 10000, and set the minimum part size to 5MB.
// This function will calculate the total part and the corresponding part size under the above restrictions.
func (w *Worker) calculatePartSize(totalSize int64) (int, int64) {
	// Max number of Parts allowed by Amazon S3 is 10000
	maxPartFragments := 10000
	minPartSize := int64(5 * 1024 * 1024) // 5MB in bytes

	// Calculate the maximum allowed part size based on the maximum number of part fragments
	maxAllowedPartSize := (totalSize / int64(maxPartFragments)) + 1

	// Ensure the calculated part size is within the allowed range
	partSize := maxAllowedPartSize
	if partSize < minPartSize {
		partSize = minPartSize
	}

	// Calculate the number of parts based on the adjusted part size
	numParts := int(math.Ceil(float64(totalSize) / float64(partSize)))

	return numParts, partSize
}

// Internal func to split large object and send sub parts transfer job messages back to sqs.
// First need to create/get an uploadID, then use UploadID and to create parts transfer job messages.
// Finally, send all the transfer job messages back to sqs.
func (w *Worker) generateMultiPartTransferJobs(ctx context.Context, obj *Object, destKey *string) (int, error) {
	var err error
	totalSize := obj.Size
	totalPartsCount, partSize := w.calculatePartSize(totalSize)

	// log.Printf("totalPartsCount: %d, partSize: %d\n", totalPartsCount, partSize)

	// Get existing upload ID or initiate multipart upload
	uploadID := w.desClient.GetUploadID(ctx, destKey)
	if uploadID == nil {
		// Add metadata to CreateMultipartUpload func.
		var meta *Metadata
		if w.cfg.IncludeMetadata {
			meta = w.srcClient.HeadObject(ctx, &obj.Key)
		}

		uploadID, err = w.desClient.CreateMultipartUpload(
			ctx, destKey, &w.cfg.DestStorageClass, &w.cfg.DestAcl, &w.cfg.DestSSEType, &w.cfg.DestSSEKMSKeyId, meta,
		)
		if err != nil {
			log.Printf("Failed to create upload ID - %s for %s\n", err.Error(), *destKey)
			return 0, err
		}
	}

	// Create and send transfer job messages for each part
	for partNumber := 1; partNumber <= totalPartsCount; partNumber++ {
		startByte := int64(int64(partNumber-1) * partSize)
		endByte := int64(int64(partNumber)*partSize) - 1
		if endByte > totalSize {
			endByte = totalSize
		}

		params := PartTransferJobParams{
			SrcBucket:       w.cfg.SrcBucket,
			DstBucket:       w.cfg.DestBucket,
			ObjectKey:       obj.Key,
			PartNumber:      partNumber,
			TotalPartsCount: totalPartsCount,
			StartByte:       startByte,
			EndByte:         endByte,
			UploadID:        *uploadID,
			BodyRange:       fmt.Sprintf("bytes=%d-%d", startByte, endByte),
		}

		// Send transfer job message to SQS
		w.sendTransferJobMessage(ctx, params)
	}
	err = w.InvokeStepFunction(ctx, *uploadID, totalPartsCount, obj.Key)

	if err != nil {
		log.Printf("Failed to invoke Step Function - %s\n", err.Error())
		return 0, err
	}
	return totalPartsCount, nil
}

// Internal func to send single part transfer job message to sqs.
func (w *Worker) sendTransferJobMessage(ctx context.Context, params PartTransferJobParams) {
	// Create a message body with transfer job parameters
	messageBody := fmt.Sprintf(
		`{
			"objectKey": "%s",
			"partNumber": %d,
			"totalPartsCount": %d,
			"uploadID": "%s",
			"bodyRange": "%s"
		}`,
		params.ObjectKey,
		params.PartNumber,
		params.TotalPartsCount,
		params.UploadID,
		params.BodyRange,
	)

	// Send message to SQS
	w.sqs.SendMessage(ctx, &messageBody)
}

// Internal func to deal with the transferring of giant file.
func (w *Worker) migrateGiantFile(ctx context.Context, obj *Object, destKey *string, transferCh chan struct{}) *TransferResult {
	var err error
	var splitPartsCount int
	splitPartsCount, err = w.generateMultiPartTransferJobs(ctx, obj, destKey)

	if err != nil {
		log.Printf("Failed to create upload ID - %s for %s\n", err.Error(), *destKey)
		return &TransferResult{
			status: "ERROR",
			err:    err,
		}
	}

	return &TransferResult{
		status:          "SPLIT_DONE",
		splitPartsCount: splitPartsCount,
		etag:            nil,
		err:             err,
	}
}

// Internal func to deal with the transferring of large file.
// First need to create/get an uploadID, then use UploadID to upload each parts
// Finally, need to combine all parts into a single file.
func (w *Worker) migrateBigFile(ctx context.Context, obj *Object, destKey *string, transferCh chan struct{}) *TransferResult {

	var err error
	var parts map[int]*Part

	uploadID := w.desClient.GetUploadID(ctx, destKey)

	// If uploadID Found, use list parts to get all existing parts.
	// Else Create a new upload ID
	if uploadID != nil {
		// log.Printf("Found upload ID %s", *uploadID)
		parts = w.desClient.ListParts(ctx, destKey, uploadID)

	} else {
		// Add metadata to CreateMultipartUpload func.
		var meta *Metadata
		if w.cfg.IncludeMetadata {
			meta = w.srcClient.HeadObject(ctx, &obj.Key)
		}

		uploadID, err = w.desClient.CreateMultipartUpload(
			ctx, destKey, &w.cfg.DestStorageClass, &w.cfg.DestAcl, &w.cfg.DestSSEType, &w.cfg.DestSSEKMSKeyId, meta,
		)
		if err != nil {
			log.Printf("Failed to create upload ID - %s for %s\n", err.Error(), *destKey)
			return &TransferResult{
				status: "ERROR",
				err:    err,
			}
		}
	}

	allParts, err := w.startMultipartUpload(ctx, obj, destKey, uploadID, parts, transferCh)
	if err != nil {
		return &TransferResult{
			status: "ERROR",
			err:    err,
		}
	}

	etag, err := w.desClient.CompleteMultipartUpload(ctx, destKey, uploadID, allParts)
	if err != nil {
		log.Printf("Failed to complete upload for %s - %s\n", obj.Key, err.Error())
		w.desClient.AbortMultipartUpload(ctx, destKey, uploadID)
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

	if int64(maxParts*chunkSize) < size {
		chunkSize = int(size/int64(maxParts)) + 1
	}
	totalParts = int(math.Ceil(float64(size) / float64(chunkSize)))
	// log.Printf("Total parts: %d, chunk size: %d", totalParts, chunkSize)
	return
}

// A func to perform multipart upload
func (w *Worker) startMultipartUpload(ctx context.Context, obj *Object, destKey, uploadID *string, parts map[int](*Part), transferCh chan struct{}) ([]*Part, error) {

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
				result := w.transfer(ctx, obj, destKey, int64(i*chunkSize), int64(chunkSize), uploadID, partNumber, nil)

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
func (w *Worker) transfer(ctx context.Context, obj *Object, destKey *string, start, chunkSize int64, uploadID *string, partNumber int, meta *Metadata) (result *TransferResult) {
	var etag *string
	var err error

	if start+chunkSize > obj.Size {
		chunkSize = obj.Size - start
	}

	log.Printf("----->Downloading %d Bytes from %s/%s\n", chunkSize, w.cfg.SrcBucket, obj.Key)

	body, err := w.srcClient.GetObject(ctx, &obj.Key, obj.Size, start, chunkSize, "null")
	if err != nil {

		var ae *types.NoSuchKey
		if errors.As(err, &ae) {
			log.Printf("No such key %s, the object might be deleted. Cancelling...", obj.Key)
			return &TransferResult{
				status: "CANCEL",
				err:    err,
			}
		}
		// status = "ERROR"
		return &TransferResult{
			status: "ERROR",
			err:    err,
		}
	}

	// destKey := appendPrefix(&obj.Key, &w.cfg.DestPrefix)
	// Use PutObject for single object upload
	// Use UploadPart for multipart upload
	if uploadID != nil {
		log.Printf("----->Uploading %d Bytes to %s/%s - Part %d\n", chunkSize, w.cfg.DestBucket, *destKey, partNumber)
		etag, err = w.desClient.UploadPart(ctx, destKey, body, uploadID, partNumber)

	} else {
		log.Printf("----->Uploading %d Bytes to %s/%s\n", chunkSize, w.cfg.DestBucket, *destKey)
		etag, err = w.desClient.PutObject(
			ctx, destKey, body, &w.cfg.DestStorageClass, &w.cfg.DestAcl,
			&w.cfg.DestSSEType, &w.cfg.DestSSEKMSKeyId, meta,
		)
	}

	body = nil // release memory
	if err != nil {
		return &TransferResult{
			status: "ERROR",
			err:    err,
		}
	}

	log.Printf("----->Completed %d Bytes from %s/%s to %s/%s\n", chunkSize, w.cfg.SrcBucket, obj.Key, w.cfg.DestBucket, *destKey)
	return &TransferResult{
		status: "DONE",
		etag:   etag,
	}

}

// transfer is a process to download data from source and upload to destination
func (w *Worker) transferSinglePart(ctx context.Context, obj *Object, destKey *string) (result *TransferResult) {
	var etag *string
	var err error

	log.Printf("----->Downloading single part %s from %s/%s - Part %d\n", obj.BodyRange, w.cfg.SrcBucket, obj.Key, obj.PartNumber)

	body, err := w.srcClient.GetObjectPart(ctx, &obj.Key, obj.BodyRange)
	if err != nil {

		var ae *types.NoSuchKey
		if errors.As(err, &ae) {
			log.Printf("No such key %s, the object might be deleted. Cancelling...", obj.Key)
			return &TransferResult{
				status: "PART_CANCEL",
				err:    err,
			}
		}
		// status = "ERROR"
		return &TransferResult{
			status: "PART_ERROR",
			err:    err,
		}
	}

	// Use UploadPart for multipart upload
	log.Printf("----->Uploading range: %s to %s/%s - Part %d\n", obj.BodyRange, w.cfg.DestBucket, *destKey, obj.PartNumber)
	etag, err = w.desClient.UploadPart(ctx, destKey, body, &obj.UploadID, obj.PartNumber)

	body = nil // release memory

	if err != nil {
		return &TransferResult{
			status: "PART_ERROR",
			err:    err,
		}
	}

	completedBytes := calculateCompletedBytes(obj.BodyRange)

	log.Printf("----->Completed %d Bytes for range: %s from %s/%s to %s/%s - Part %d\n", completedBytes, obj.BodyRange, w.cfg.SrcBucket, obj.Key, w.cfg.DestBucket, *destKey, obj.PartNumber)
	return &TransferResult{
		status: "PART_DONE",
		etag:   etag,
	}
}

func (w *Worker) InvokeStepFunction(ctx context.Context, uploadID string, totalPartsCount int, objectKey string) error {
	log.Printf("Invoke Step Function for %s with uploadID %s and totalPartsCount %d\n", objectKey, uploadID, totalPartsCount)
	err := w.sfn.InvokeStepFunction(ctx, uploadID, totalPartsCount, objectKey)
	return err
}

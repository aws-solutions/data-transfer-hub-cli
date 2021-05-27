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
	"reflect"
	"testing"
)

func TestNewObject(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name  string
		args  args
		wantO *Object
	}{
		{
			name: "Valid Json",
			args: args{
				str: `{"key":"ABC","size":100}`,
			},
			wantO: &Object{
				Key:  "ABC",
				Size: 100,
			},
		},
		{
			name: "Invalid Json",
			args: args{
				str: "haha",
			},
			wantO: nil,
		},
		{
			name: "More attributes",
			args: args{
				str: `{
                    "key": "foo/bar.png",
                    "size": 69913,
                    "eTag": "b371f3a529a6ff3ecaa0e3ddb9b64a9e",
                    "sequencer": "005FD83DA108ECB135"
                }`,
			},
			wantO: &Object{
				Key:       "foo/bar.png",
				Size:      69913,
				Sequencer: "005FD83DA108ECB135",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotO := newObject(&tt.args.str); !reflect.DeepEqual(gotO, tt.wantO) {
				t.Errorf("newObject() = %v, want %v", gotO, tt.wantO)
			}
		})
	}
}

func TestObjectToString(t *testing.T) {
	output := `{"key":"ABC","size":100}`
	output2 := `{"key":"ABC","size":100,"sequencer":"005FD83DA108ECB135"}`

	tests := []struct {
		name string
		obj  *Object
		want *string
	}{
		{
			name: "Test Omitempty",
			obj: &Object{
				Key:  "ABC",
				Size: 100,
			},
			want: &output,
		},
		{
			name: "Test Full",
			obj: &Object{
				Key:       "ABC",
				Size:      100,
				Sequencer: "005FD83DA108ECB135",
			},
			want: &output2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// o := &Object{
			// 	Key:  tt.fields.Key,
			// 	Size: tt.fields.Size,
			// }
			if got := tt.obj.toString(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Object.toString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewS3Event(t *testing.T) {
	input := `{  
		"Records":[  
		   {  
			  "eventVersion":"2.2",
			  "eventSource":"aws:s3",
			  "awsRegion":"us-west-2",
			  "eventTime":"The time, in ISO-8601 format, for example, 1970-01-01T00:00:00.000Z, when Amazon S3 finished processing the request",
			  "eventName":"ObjectCreated:Put",
			  "userIdentity":{  
				 "principalId":"Amazon-customer-ID-of-the-user-who-caused-the-event"
			  },
			  "requestParameters":{  
				 "sourceIPAddress":"ip-address-where-request-came-from"
			  },
			  "responseElements":{  
				 "x-amz-request-id":"Amazon S3 generated request ID",
				 "x-amz-id-2":"Amazon S3 host that processed the request"
			  },
			  "s3":{  
				 "s3SchemaVersion":"1.0",
				 "configurationId":"ID found in the bucket notification configuration",
				 "bucket":{  
					"name":"bucket-name",
					"ownerIdentity":{  
					   "principalId":"Amazon-customer-ID-of-the-bucket-owner"
					},
					"arn":"bucket-ARN"
				 },
				 "object":{  
					"key":"object-key",
					"size": 1234567,
					"eTag":"object eTag",
					"versionId":"object version if bucket is versioning-enabled, otherwise null",
					"sequencer": "a string representation of a hexadecimal value used to determine event sequence, only used with PUTs and DELETEs"
				 }
			  },
			  "glacierEventData": {
				 "restoreEventData": {
					"lifecycleRestorationExpiryTime": "The time, in ISO-8601 format, for example, 1970-01-01T00:00:00.000Z, of Restore Expiry",
					"lifecycleRestoreStorageClass": "Source storage class for restore"
				 }
			  }
		   }
		]
	}`

	got := newS3Event(&input)
	if len(got.Records) != 1 {
		t.Errorf("Expected input message has 1 record, got %d", len(got.Records))
	}

	if got.Records[0].EventName != "ObjectCreated:Put" {
		t.Errorf("Expected input message has event name of %s, got %s", "ObjectCreated:Put", got.Records[0].EventName)
	}

	if got.Records[0].S3.Object.Key != "object-key" {
		t.Errorf("Expected input message has object with key %s, got %s", "object-key", got.Records[0].S3.Key)
	}

}

func TestGetHex(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "Test Convert",
			args: args{"005FD83DA108ECB135"},
			want: 6906337790421414197,
		},
		{
			name: "Test Zero",
			args: args{""},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getHex(&tt.args.str); got != tt.want {
				t.Errorf("getHex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUunescape(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test",
			args: args{"foo+%282%29.jpeg"},
			want: "foo (2).jpeg",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := unescape(&tt.args.str); got != tt.want {
				t.Errorf("unquote() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEscape(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test",
			args: args{"foo (2).jpeg"},
			want: "foo+%282%29.jpeg",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escape(&tt.args.str); got != tt.want {
				t.Errorf("escape() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRemovePrefix(t *testing.T) {
	type args struct {
		key    string
		prefix string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test normal prefix",
			args: args{
				key:    "foo/bar.png",
				prefix: "foo",
			},
			want: "bar.png",
		},
		{
			name: "Test empty prefix",
			args: args{
				key:    "foo/bar.png",
				prefix: "",
			},
			want: "foo/bar.png",
		},
		{
			name: "Test duplicate prefix",
			args: args{
				key:    "foo/foo/bar.png",
				prefix: "foo",
			},
			want: "foo/bar.png",
		},
		{
			name: "Test prefix with /",
			args: args{
				key:    "abc/foo/bar.png",
				prefix: "abc/",
			},
			want: "foo/bar.png",
		},
		{
			name: "Test prefix with / only",
			args: args{
				key:    "foo/bar.png",
				prefix: "/",
			},
			want: "foo/bar.png",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := removePrefix(&tt.args.key, &tt.args.prefix); *got != tt.want {
				t.Errorf("removePrefix() = %v, want %v", *got, tt.want)
			}
		})
	}
}

func TestAppendPrefix(t *testing.T) {
	type args struct {
		key    string
		prefix string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Test normal prefix",
			args: args{
				key:    "bar.png",
				prefix: "foo",
			},
			want: "foo/bar.png",
		},
		{
			name: "Test empty prefix",
			args: args{
				key:    "foo/bar.png",
				prefix: "",
			},
			want: "foo/bar.png",
		},
		{
			name: "Test prefix with /",
			args: args{
				key:    "foo/bar.png",
				prefix: "abc/",
			},
			want: "abc/foo/bar.png",
		},
		{
			name: "Test prefix with / only",
			args: args{
				key:    "foo/bar.png",
				prefix: "/",
			},
			want: "foo/bar.png",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := appendPrefix(&tt.args.key, &tt.args.prefix); *got != tt.want {
				t.Errorf("appendPrefix() = %v, want %v", *got, tt.want)
			}
		})
	}
}

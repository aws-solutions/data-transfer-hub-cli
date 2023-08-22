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
	"testing"
)

func TestCalculatePartSize(t *testing.T) {
	w := Worker{}

	tests := []struct {
		totalSize        int64
		expectedParts    int
		expectedPartSize int64
	}{
		{1024 * 1024 * 1024 * 1, 21, 52428800},            // 1GB
		{1024 * 1024 * 1024 * 5, 103, 52428800},           // 5GB
		{1024 * 1024 * 1024 * 10, 205, 52428800},          // 10GB
		{1024 * 1024 * 1024 * 100, 2048, 52428800},        // 100GB
		{1024 * 1024 * 1024 * 1024, 10000, 109951163},     // 1TB
		{1024 * 1024 * 1024 * 1024 * 5, 10000, 549755814}, // 5TB
	}

	for _, test := range tests {
		numParts, partSize := w.calculatePartSize(test.totalSize)
		if numParts != test.expectedParts || partSize != test.expectedPartSize {
			t.Errorf("For total size %d, expected parts: %d, expected part size: %d; got parts: %d, part size: %d, part size in MB: %d",
				test.totalSize, test.expectedParts, test.expectedPartSize, numParts, partSize, partSize/1024/1024)
		}
	}
}

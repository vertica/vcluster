/*
 (c) Copyright [2023] Open Text.
 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package vclusterops

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildQueryParams(t *testing.T) {
	queryParams := make(map[string]string)

	// empty query params should produce an empty string
	queryParamString := buildQueryParamString(queryParams)
	assert.Empty(t, queryParamString)

	// non-empty query params should produce a string like
	// "?key1=value1&key2=value2"
	queryParams["key1"] = "value1"
	queryParams["key2"] = "value2"
	queryParamString = buildQueryParamString(queryParams)
	assert.Equal(t, queryParamString, "?key1=value1&key2=value2")

	// query params with special characters, such as %
	// which is used by the create depot endpoint
	queryParams = make(map[string]string)
	queryParams["size"] = "10%"
	queryParams["path"] = "/the/depot/path"
	queryParamString = buildQueryParamString(queryParams)
	// `/` is escaped with `%2F`
	// `%` is escaped with `%25`
	assert.Equal(t, queryParamString, "?path=%2Fthe%2Fdepot%2Fpath&size=10%25")
}

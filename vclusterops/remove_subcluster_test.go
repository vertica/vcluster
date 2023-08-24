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

const dbName = "test_db"

func TestRemoveSubcluster(t *testing.T) {
	options := VRemoveScOptionsFactory()
	*options.HonorUserInput = true
	options.RawHosts = []string{"vnode1", "vnode2"}
	options.Password = new(string)

	// options without db name, sc name, data path, and depot path
	err := options.validateParseOptions()
	assert.ErrorContains(t, err, "must specify a database name")

	// input db name
	*options.Name = dbName
	err = options.validateParseOptions()
	assert.ErrorContains(t, err, "must specify a subcluster name")

	// input sc name
	options.SubclusterToRemove = new(string)
	*options.SubclusterToRemove = "sc1"
	err = options.validateParseOptions()
	assert.ErrorContains(t, err, "must specify an absolute data path")

	// input data path
	*options.DataPrefix = defaultPath
	err = options.validateParseOptions()
	assert.ErrorContains(t, err, "must specify an absolute depot path")

	// input depot path
	*options.DepotPrefix = defaultPath
	err = options.validateParseOptions()
	assert.NoError(t, err)
}

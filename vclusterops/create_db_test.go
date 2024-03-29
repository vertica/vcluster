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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const defaultPath = "/data"

func TestValidateDepotSize(t *testing.T) {
	res, err := validateDepotSize("-19%")
	assert.Equal(t, res, false)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "it is less than 0%")

	res, err = validateDepotSize("119%")
	assert.Equal(t, res, false)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "it is greater than 100%")

	res, err = validateDepotSize("+19%")
	assert.Equal(t, res, true)
	assert.Nil(t, err)

	res, err = validateDepotSize("19%")
	assert.Equal(t, res, true)
	assert.Nil(t, err)

	res, err = validateDepotSize("-119K")
	assert.Equal(t, res, false)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "it is <= 0")

	res, err = validateDepotSize("+119T")
	assert.Equal(t, res, true)
	assert.Nil(t, err)
}

func TestWriteClusterConfig(t *testing.T) {
	const dbName = "practice_db"
	const scName = "default_subcluster"

	// generate a YAML file based on a stub vdb
	vdb := VCoordinationDatabase{}
	vdb.Name = dbName
	vdb.CatalogPrefix = defaultPath
	vdb.DataPrefix = defaultPath
	vdb.DepotPrefix = defaultPath
	vdb.HostList = []string{"ip_1", "ip_2", "ip_3"}
	vdb.HostNodeMap = makeVHostNodeMap()
	for i, h := range vdb.HostList {
		n := VCoordinationNode{}
		n.Name = fmt.Sprintf("node_name_%d", i+1)
		n.Address = h
		n.Subcluster = scName
		vdb.HostNodeMap[h] = &n
	}
	vdb.IsEon = true

	tmp, err := os.CreateTemp("", "cluster-config-test.*.yaml")
	assert.NoError(t, err)
	tmp.Close()
	os.Remove(tmp.Name())

	err = vdb.WriteClusterConfig(tmp.Name(), vlog.Printer{})
	assert.NoError(t, err)
	defer os.Remove(tmp.Name())

	// compare the generated file with expected output
	actualBytes, err := os.ReadFile(tmp.Name())
	assert.NoError(t, err)
	expectedBytes, err := os.ReadFile("test_data/" + ConfigFileName)
	assert.NoError(t, err)
	assert.True(t, bytes.Equal(actualBytes, expectedBytes))

	// now write the config file again
	// a backup file should be generated
	err = vdb.WriteClusterConfig(tmp.Name(), vlog.Printer{})
	assert.NoError(t, err)
	bkpName := fmt.Sprintf("%s/%s", filepath.Dir(tmp.Name()), ConfigBackupName)
	err = util.CanReadAccessDir(bkpName)
	assert.NoError(t, err)
	defer os.Remove(bkpName)

	// clean up
	defer os.RemoveAll(dbName)
}

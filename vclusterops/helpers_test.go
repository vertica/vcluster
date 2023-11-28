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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// positive test case for updateCatalogPathMapFromCatalogEditor
func TestForupdateCatalogPathMapFromCatalogEditorPositive(t *testing.T) {
	// prepare data for nmaVDB
	mockNmaVNode1 := &nmaVNode{CatalogPath: "/data/test_db/v_test_db_node0001_catalog/Catalog", Address: "192.168.1.101"}
	mockNmaVNode2 := &nmaVNode{CatalogPath: "/Catalog/data/test_db/v_test_db_node0002_catalog/Catalog", Address: "192.168.1.102"}
	mockNmaVNode3 := &nmaVNode{CatalogPath: "/data/test_db/v_test_db_node0003_catalog/Catalog", Address: "192.168.1.103"}
	mockHostNodeMap := map[string]*nmaVNode{"192.168.1.101": mockNmaVNode1, "192.168.1.102": mockNmaVNode2, "192.168.1.103": mockNmaVNode3}
	mockNmaVDB := &nmaVDatabase{HostNodeMap: mockHostNodeMap}
	host := []string{"192.168.1.101", "192.168.1.102", "192.168.1.103"}
	mockCatalogPath := make(map[string]string)
	err := updateCatalogPathMapFromCatalogEditor(host, mockNmaVDB, mockCatalogPath)
	assert.NoError(t, err)
	assert.Equal(t, mockCatalogPath["192.168.1.101"], "/data/test_db/v_test_db_node0001_catalog")
	assert.Equal(t, mockCatalogPath["192.168.1.102"], "/Catalog/data/test_db/v_test_db_node0002_catalog")
	assert.Equal(t, mockCatalogPath["192.168.1.103"], "/data/test_db/v_test_db_node0003_catalog")
}

// negative test case for updateCatalogPathMapFromCatalogEditor
func TestForupdateCatalogPathMapFromCatalogEditorNegative(t *testing.T) {
	// prepare data for nmaVDB
	mockNmaVNode1 := &nmaVNode{CatalogPath: "/data/test_db/v_test_db_node0001_catalog/Catalog", Address: "192.168.1.101"}
	mockNmaVNode2 := &nmaVNode{CatalogPath: "/data/test_db/v_test_db_node0002_catalog/Catalog", Address: "192.168.1.102"}
	mockHostNodeMap := map[string]*nmaVNode{"192.168.1.101": mockNmaVNode1, "192.168.1.102": mockNmaVNode2}
	mockNmaVDB := &nmaVDatabase{HostNodeMap: mockHostNodeMap}
	host := []string{"192.168.1.101", "192.168.1.103"}
	mockCatalogPath := make(map[string]string)
	err := updateCatalogPathMapFromCatalogEditor(host, mockNmaVDB, mockCatalogPath)
	assert.ErrorContains(t, err, "fail to get catalog path from host 192.168.1.103")
	host = make([]string, 0)
	err = updateCatalogPathMapFromCatalogEditor(host, mockNmaVDB, mockCatalogPath)
	assert.ErrorContains(t, err, "fail to get host with highest catalog version")
}

func TestForgetInitiatorHost(t *testing.T) {
	nodesList1 := []string{"10.0.0.0", "10.0.0.1", "10.0.0.2"}
	hostsToSkip1 := []string{"10.0.0.10", "10.0.0.11"}
	hostsToSkip2 := []string{"10.0.0.0", "10.0.0.1"}

	// successfully picks an initiator
	initiatorHost, _ := getInitiatorHost(nodesList1, hostsToSkip1)
	assert.Equal(t, initiatorHost, "10.0.0.0")
	initiatorHost, _ = getInitiatorHost(nodesList1, hostsToSkip2)
	assert.Equal(t, initiatorHost, "10.0.0.2")

	// returns empty string because there is no primary up node that is not
	// in the list of hosts to skip.
	hostsToSkip1 = nodesList1
	initiatorHost, _ = getInitiatorHost(nodesList1, hostsToSkip1)
	assert.Equal(t, initiatorHost, "")
}

func TestForgetCatalogPath(t *testing.T) {
	nodeName := "v_vertdb_node0001"
	fullPath := fmt.Sprintf("/data/vertdb/%s_catalog/Catalog", nodeName)
	expPath := fmt.Sprintf("/data/vertdb/%s_catalog", nodeName)

	catalogPath := getCatalogPath(fullPath)
	assert.Equal(t, catalogPath, expPath)

	catalogPath = getCatalogPath(catalogPath)
	assert.Equal(t, catalogPath, expPath)
}

func TestValidateHostMap(t *testing.T) {
	host1 := "192.168.0.1"
	host2 := "192.168.0.2"
	host3 := "192.168.0.3"
	twoHosts := []string{host1, host2}
	threeHosts := []string{host1, host2, host3}
	oneMap := map[string]string{
		host1: "foo",
	}
	twoMap := map[string]string{
		host1: "foo",
		host2: "bar",
	}
	threeMap := map[string]string{
		host1: "foo",
		host2: "bar",
		host3: "foobar",
	}

	// test empty args
	err := validateHostMaps(nil, nil)
	assert.NoError(t, err)
	err = validateHostMaps(nil)
	assert.NoError(t, err)

	// test positive case
	err = validateHostMaps(twoHosts, twoMap, threeMap)
	assert.NoError(t, err)

	// test one entry missing
	err = validateHostMaps(twoHosts, oneMap, twoMap)
	assert.Error(t, err)

	// test two entries + one entry missing
	err = validateHostMaps(threeHosts, oneMap, twoMap)
	assert.Error(t, err)
}

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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vertica/vcluster/vclusterops/vstruct"
)

func TestReIPOptions(t *testing.T) {
	opt := VReIPFactory()
	err := opt.ValidateAnalyzeOptions()
	assert.Error(t, err)

	*opt.Name = "test_db"
	opt.RawHosts = []string{"192.168.1.101", "192.168.1.102"}
	err = opt.ValidateAnalyzeOptions()
	assert.ErrorContains(t, err, "must specify an absolute catalog path")

	*opt.CatalogPrefix = "/data"
	err = opt.ValidateAnalyzeOptions()
	assert.ErrorContains(t, err, "the re-ip list is not provided")

	var info ReIPInfo
	info.NodeAddress = "192.168.1.102"
	info.TargetAddress = "192.168.1.103"
	opt.ReIPList = append(opt.ReIPList, info)
	err = opt.ValidateAnalyzeOptions()
	assert.NoError(t, err)
}

func TestReadReIPFile(t *testing.T) {
	opt := VReIPFactory()
	currentDir, _ := os.Getwd()

	// ipv4 positive
	opt.Ipv6 = vstruct.False
	err := opt.ReadReIPFile(currentDir + "/test_data/re_ip_v4.json")
	assert.NoError(t, err)

	// ipv4 negative
	err = opt.ReadReIPFile(currentDir + "/test_data/re_ip_v4_wrong.json")
	assert.ErrorContains(t, err, "192.168.1.10a in the re-ip file is not a valid IPv4 address")

	// ipv6
	opt.Ipv6 = vstruct.True
	err = opt.ReadReIPFile(currentDir + "/test_data/re_ip_v6.json")
	assert.NoError(t, err)

	// ipv6 negative
	err = opt.ReadReIPFile(currentDir + "/test_data/re_ip_v6_wrong.json")
	assert.ErrorContains(t, err, "0:0:0:0:0:ffff:c0a8:016-6 in the re-ip file is not a valid IPv6 address")
}

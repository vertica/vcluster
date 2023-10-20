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
	"github.com/vertica/vcluster/vclusterops/vlog"
)

func TestTimeoutCase(t *testing.T) {
	var instructions []ClusterOp
	// use a non-existing IP to test the timeout error
	// 192.0.2.1 is one that is reserved for test purpose (by RFC 5737)
	hosts := []string{"192.0.2.1"}
	username := "testUser"
	password := "testPwd"
	httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOp(vlog.Printer{}, hosts, true, username, &password)
	assert.Nil(t, err)
	instructions = append(instructions, &httpsPollNodeStateOp)

	certs := HTTPSCerts{}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	err = clusterOpEngine.Run(vlog.Printer{})
	assert.ErrorContains(t, err, "[HTTPSPollNodeStateOp] cannot connect to host 192.0.2.1, please check if the host is still alive")
}

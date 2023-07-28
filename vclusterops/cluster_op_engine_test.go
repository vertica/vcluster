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

type mockOp struct {
	OpBase
	calledPrepare  bool
	calledExecute  bool
	calledFinalize bool
}

func makeMockOp(skipExecute bool) mockOp {
	return mockOp{
		OpBase: OpBase{
			name:        fmt.Sprintf("skip-enabled-%v", skipExecute),
			skipExecute: skipExecute,
		},
	}
}

func (m *mockOp) Prepare(execContext *OpEngineExecContext) error {
	m.calledPrepare = true
	if !m.skipExecute {
		m.setupClusterHTTPRequest([]string{"host1"})
	}
	return nil
}

func (m *mockOp) Execute(execContext *OpEngineExecContext) error {
	m.calledExecute = true
	return nil
}

func (m *mockOp) Finalize(execContext *OpEngineExecContext) error {
	m.calledFinalize = true
	return nil
}

func (m *mockOp) processResult(execContext *OpEngineExecContext) error {
	return nil
}

func (m *mockOp) setupClusterHTTPRequest(hosts []string) {
	m.clusterHTTPRequest.RequestCollection = map[string]HostHTTPRequest{}
	for i := range hosts {
		m.clusterHTTPRequest.RequestCollection[hosts[i]] = HostHTTPRequest{}
	}
}

func TestSkipExecuteOp(t *testing.T) {
	opWithSkipEnabled := makeMockOp(true)
	opWithSkipDisabled := makeMockOp(false)
	instructions := []ClusterOp{&opWithSkipDisabled, &opWithSkipEnabled}
	certs := HTTPSCerts{key: "key", cert: "cert", caCert: "ca-cert"}
	opEngn := MakeClusterOpEngine(instructions, &certs)
	err := opEngn.Run()
	assert.Equal(t, nil, err)
	assert.True(t, opWithSkipDisabled.calledPrepare)
	assert.True(t, opWithSkipDisabled.calledExecute)
	assert.True(t, opWithSkipDisabled.calledFinalize)
	assert.True(t, opWithSkipEnabled.calledPrepare)
	assert.False(t, opWithSkipEnabled.calledExecute)
	assert.True(t, opWithSkipEnabled.calledFinalize)
}

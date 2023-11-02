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

	"github.com/vertica/vcluster/vclusterops/vlog"
)

// nodes being down is not unusual for the purpose of this op.
// don't block 6 minutes because of one down node.
const healthRequestTimeoutSeconds = 20

type NMAGetHealthyNodesOp struct {
	OpBase
	vdb *VCoordinationDatabase
}

func makeNMAGetHealthyNodesOp(log vlog.Printer, hosts []string,
	vdb *VCoordinationDatabase) NMAGetHealthyNodesOp {
	nmaGetHealthyNodesOp := NMAGetHealthyNodesOp{}
	nmaGetHealthyNodesOp.name = "NMAGetHealthyNodesOp"
	nmaGetHealthyNodesOp.log = log.WithName(nmaGetHealthyNodesOp.name)
	nmaGetHealthyNodesOp.hosts = hosts
	nmaGetHealthyNodesOp.vdb = vdb
	return nmaGetHealthyNodesOp
}

func (op *NMAGetHealthyNodesOp) setupClusterHTTPRequest(hosts []string) error {
	op.vdb.HostList = []string{}
	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.Timeout = healthRequestTimeoutSeconds
		httpRequest.buildNMAEndpoint("health")
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *NMAGetHealthyNodesOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *NMAGetHealthyNodesOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMAGetHealthyNodesOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

func (op *NMAGetHealthyNodesOp) processResult(_ *OpEngineExecContext) error {
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			_, err := op.parseAndCheckMapResponse(host, result.content)
			if err == nil {
				op.vdb.HostList = append(op.vdb.HostList, host)
			} else {
				op.log.Error(err, "NMA health check response malformed from host", "Host", host)
				op.log.PrintWarning("Skipping unhealthy host %s", host)
			}
		} else {
			op.log.Error(result.err, "Host is not reachable", "Host", host)
			op.log.PrintWarning("Skipping unreachable host %s", host)
		}
	}
	if len(op.vdb.HostList) == 0 {
		return fmt.Errorf("NMA is down or unresponsive on all hosts")
	}

	return nil
}

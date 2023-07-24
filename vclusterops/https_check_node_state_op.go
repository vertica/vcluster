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
	"errors"
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type HTTPCheckNodeStateOp struct {
	OpBase
	OpHTTPBase
}

func MakeHTTPCheckNodeStateOp(opName string,
	hosts []string,
	useHTTPPassword bool,
	userName string,
	httpsPassword *string,
) HTTPCheckNodeStateOp {
	nodeStateChecker := HTTPCheckNodeStateOp{}
	nodeStateChecker.name = opName
	// The hosts are the ones we are going to talk to.
	// They can be a subset of the actual host information that we return,
	// as if any of the hosts is responsive, spread can give us the info of all nodes
	nodeStateChecker.hosts = hosts
	nodeStateChecker.useHTTPPassword = useHTTPPassword

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)
	nodeStateChecker.userName = userName
	nodeStateChecker.httpsPassword = httpsPassword
	return nodeStateChecker
}

func (op *HTTPCheckNodeStateOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildHTTPSEndpoint("nodes")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPCheckNodeStateOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *HTTPCheckNodeStateOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPCheckNodeStateOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error
	respondingNodeCount := 0

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			vlog.LogPrintError("[%s] unauthorized request: %s", op.name, result.content)
			// return here because we assume that
			// we will get the same error across other nodes
			allErrs = errors.Join(allErrs, result.err)
			return allErrs
		}

		if !result.isPassing() {
			// for any error, we continue to the next node
			if result.IsInternalError() {
				vlog.LogPrintError("[%s] internal error of the /nodes endpoint: %s", op.name, result.content)
				// At internal error originated from the server, so its a
				// response, just not a successful one.
				respondingNodeCount++
			}
			allErrs = errors.Join(allErrs, result.err)
			continue
		}

		// parse the /nodes endpoint response
		respondingNodeCount++
		nodesInfo := NodesInfo{}
		err := op.parseAndCheckResponse(host, result.content, &nodesInfo)
		if err != nil {
			err = fmt.Errorf("[%s] fail to parse result on host %s: %w",
				op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}
		// successful case, write the result into exec context
		execContext.nodeStates = nodesInfo.NodeList
		return nil
	}

	// If none of the requests succeed on any node, we
	// can assume that all nodes are down.
	if respondingNodeCount == 0 {
		// this list is built for Go client
		var nodeStates []NodeInfo
		for _, host := range op.hosts {
			nodeInfo := NodeInfo{}
			nodeInfo.Address = host
			nodeInfo.State = "DOWN"
			nodeStates = append(nodeStates, nodeInfo)
		}
		execContext.nodeStates = nodeStates
	}
	return allErrs
}

func (op *HTTPCheckNodeStateOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

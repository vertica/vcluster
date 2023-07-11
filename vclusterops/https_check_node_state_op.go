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

func (op *HTTPCheckNodeStateOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *HTTPCheckNodeStateOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *HTTPCheckNodeStateOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := false
	respondingNodeCount := 0

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			// parse the /nodes endpoint response
			nodesInfo := NodesInfo{}
			err := op.parseAndCheckResponse(host, result.content, &nodesInfo)
			if err != nil {
				vlog.LogPrintError("[%s] fail to parse result on host %s, details: %w",
					op.name, host, err)
			} else {
				// successful case, write the result into exec context
				execContext.nodeStates = nodesInfo.NodeList
				success = true
			}
			respondingNodeCount++
			break
		}

		if result.IsUnauthorizedRequest() {
			vlog.LogPrintError("[%s] unauthorized request: %s", op.name, result.content)
			respondingNodeCount++
			// break here because we assume that
			// we will get the same error across other nodes
			break
		}

		if result.IsInternalError() {
			vlog.LogPrintError("[%s] internal error of the /nodes endpoint: %s", op.name, result.content)
			respondingNodeCount++
			// for internal error, we use "continue" to try the next node
			continue
		}
	}

	// if the request did not pass on any node
	// we assume that all nodes are down
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

		// here we return a failed result for K8s operator to consume
		return MakeClusterOpResultFail()
	}

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}

func (op *HTTPCheckNodeStateOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

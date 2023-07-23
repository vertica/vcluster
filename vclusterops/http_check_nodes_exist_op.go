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

// HTTPCheckNodesExistOp defines an operation to get the
// node states and check if some hosts are already part
// of the database.
type HTTPCheckNodesExistOp struct {
	OpBase
	OpHTTPBase
	// The IP addresses of the hosts whose existence we want to check
	targetHosts []string
}

// MakeHTTPCheckNodesExistOp will make a https op that check if new nodes exists in current database
func MakeHTTPCheckNodesExistOp(
	hosts []string,
	targetHosts []string,
	useHTTPPassword bool,
	userName string,
	httpsPassword *string) HTTPCheckNodesExistOp {
	nodeStateChecker := HTTPCheckNodesExistOp{}
	nodeStateChecker.name = "HTTPCheckNodesExistOp"
	// The hosts are the ones we are going to talk to.
	// as if any of the hosts is responsive, spread can give us the info of all nodes.
	nodeStateChecker.hosts = hosts
	nodeStateChecker.targetHosts = targetHosts
	nodeStateChecker.useHTTPPassword = useHTTPPassword

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)
	nodeStateChecker.userName = userName
	nodeStateChecker.httpsPassword = httpsPassword
	return nodeStateChecker
}

func (op *HTTPCheckNodesExistOp) setupClusterHTTPRequest(hosts []string) {
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

func (op *HTTPCheckNodesExistOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *HTTPCheckNodesExistOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *HTTPCheckNodesExistOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := false
	respondingNodeCount := 0

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			// parse the /nodes endpoint response
			nodesInfo := NodesInfo{}
			err := op.parseAndCheckResponse(host, result.content, &nodesInfo)
			if err != nil {
				vlog.LogPrintError("[%s] fail to parse result on host %s, details: %s",
					op.name, host, err)
			} else {
				// We check if any of the new nodes already exist in the database
				if op.checkNodesExist(nodesInfo.NodeList) {
					return MakeClusterOpResultFail()
				}
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
		return MakeClusterOpResultFail()
	}

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}

func (op *HTTPCheckNodesExistOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

// checkNodesExist return true if at least one of the new hosts
// already exists in the database.
func (op *HTTPCheckNodesExistOp) checkNodesExist(nodes []NodeInfo) bool {
	// verify the new nodes do not exist in current database
	hostSet := make(map[string]struct{})
	for _, host := range op.targetHosts {
		hostSet[host] = struct{}{}
	}
	dupHosts := []string{}
	for _, host := range nodes {
		if _, exist := hostSet[host.Address]; exist {
			dupHosts = append(dupHosts, host.Address)
		}
	}
	if len(dupHosts) == 0 {
		return false
	}
	vlog.LogPrintError("[%s] new nodes %v already exist in the database", op.name, dupHosts)
	return true
}

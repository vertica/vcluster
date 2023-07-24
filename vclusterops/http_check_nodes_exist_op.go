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

func (op *HTTPCheckNodesExistOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *HTTPCheckNodesExistOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPCheckNodesExistOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			// return here because we assume that
			// we will get the same error across other nodes
			return fmt.Errorf("[%s] unauthorized request: %w", op.name, result.err)
		}

		if result.err != nil {
			err := fmt.Errorf("[%s] error of the /nodes endpoint: %w", op.name, result.err)
			allErrs = errors.Join(allErrs, err)
			// for any error, we use "continue" to try the next node
			continue
		}

		// parse the /nodes endpoint response
		nodesInfo := NodesInfo{}
		err := op.parseAndCheckResponse(host, result.content, &nodesInfo)
		if err != nil {
			err = fmt.Errorf("[%s] fail to parse result on host %s, details: %w",
				op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}
		// We check if any of the new nodes already exist in the database
		if op.checkNodesExist(nodesInfo.NodeList) {
			return errors.New("new node already exists in the database")
		}
		return nil
	}
	return allErrs
}

func (op *HTTPCheckNodesExistOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
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

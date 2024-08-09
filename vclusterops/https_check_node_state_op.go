/*
 (c) Copyright [2023-2024] Open Text.
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
)

type httpsCheckNodeStateOp struct {
	opBase
	opHTTPSBase
}

func makeHTTPSCheckNodeStateOp(hosts []string,
	useHTTPPassword bool,
	userName string,
	httpsPassword *string,
) (httpsCheckNodeStateOp, error) {
	op := httpsCheckNodeStateOp{}
	op.name = "HTTPCheckNodeStateOp"
	op.description = "Check node state from running database"
	// The hosts are the ones we are going to talk to.
	// They can be a subset of the actual host information that we return,
	// as if any of the hosts is responsive, spread can give us the info of all nodes
	op.hosts = hosts
	op.useHTTPPassword = useHTTPPassword

	err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
	if err != nil {
		return op, err
	}

	op.userName = userName
	op.httpsPassword = httpsPassword
	return op, nil
}

func (op *httpsCheckNodeStateOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.buildHTTPSEndpoint("nodes")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsCheckNodeStateOp) prepare(execContext *opEngineExecContext) error {
	execContext.dispatcher.setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsCheckNodeStateOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsCheckNodeStateOp) processResult(execContext *opEngineExecContext) error {
	var allErrs error
	respondingNodeCount := 0

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isUnauthorizedRequest() {
			op.logger.PrintError("[%s] unauthorized request: %s", op.name, result.content)
			execContext.hostsWithWrongAuth = append(execContext.hostsWithWrongAuth, host)
			// return here because we assume that
			// we will get the same error across other nodes
			allErrs = errors.Join(allErrs, result.err)
			return allErrs
		}

		if !result.isPassing() {
			// for any error, we continue to the next node
			if result.isInternalError() {
				op.logger.PrintError("[%s] internal error of the /nodes endpoint: %s", op.name, result.content)
				// At internal error originated from the server, so its a
				// response, just not a successful one.
				respondingNodeCount++
			}
			allErrs = errors.Join(allErrs, result.err)
			continue
		}

		// parse the /nodes endpoint response
		respondingNodeCount++
		nodesStates := nodesStateInfo{}
		err := op.parseAndCheckResponse(host, result.content, &nodesStates)
		if err != nil {
			err = fmt.Errorf("[%s] fail to parse result on host %s: %w",
				op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		nodesInfo := nodesInfo{}
		for _, node := range nodesStates.NodeList {
			n := node.asNodeInfoWithoutVer()
			nodesInfo.NodeList = append(nodesInfo.NodeList, n)
		}
		// successful case, write the result into exec context
		execContext.nodesInfo = nodesInfo.NodeList
		op.logger.PrintInfo("reporting results as obtained from the host [%s] ", host)
		return nil
	}

	return allErrs
}

func (op *httpsCheckNodeStateOp) finalize(_ *opEngineExecContext) error {
	return nil
}

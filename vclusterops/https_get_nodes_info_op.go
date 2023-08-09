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

type httpsGetNodesInfoOp struct {
	OpBase
	OpHTTPBase
}

func makeHTTPSGetNodesInfoOp(hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
) (httpsGetNodesInfoOp, error) {
	op := httpsGetNodesInfoOp{}
	op.name = "HTTPSGetNodeInfoOp"
	op.hosts = hosts
	op.useHTTPPassword = useHTTPPassword

	if useHTTPPassword {
		err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
		if err != nil {
			return op, err
		}
		op.userName = userName
		op.httpsPassword = httpsPassword
	}

	return op, nil
}

func (op *httpsGetNodesInfoOp) setupClusterHTTPRequest(hosts []string) error {
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

	return nil
}

func (op *httpsGetNodesInfoOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsGetNodesInfoOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsGetNodesInfoOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			return fmt.Errorf("[%s] wrong password/certificate for https service on host %s",
				op.name, host)
		}

		if result.isPassing() {
			// parse the /nodes endpoint response
			nodesStateInfo := NodesStateInfo{}
			err := op.parseAndCheckResponse(host, result.content, &nodesStateInfo)
			if err != nil {
				vlog.LogPrintError("[%s] fail to parse result on host %s, details: %s",
					op.name, host, err)
				return err
			}
			// save node list information to execContext
			execContext.nodeStates = nodesStateInfo.NodeList
			return nil
		}
		allErrs = errors.Join(allErrs, result.err)
	}
	return errors.Join(allErrs, fmt.Errorf("could not find a host with a passing result"))
}

func (op *httpsGetNodesInfoOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

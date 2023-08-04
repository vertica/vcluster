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
)

type httpsGetClusterInfoOp struct {
	OpBase
	OpHTTPBase
	clusterFileContent *string
}

func makeHTTPSGetClusterInfoOp(hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string, clusterFileContent *string) httpsGetClusterInfoOp {
	httpGetClusterInfoOp := httpsGetClusterInfoOp{}
	httpGetClusterInfoOp.name = "HTTPSGetClusterInfoOp"
	httpGetClusterInfoOp.hosts = hosts
	httpGetClusterInfoOp.clusterFileContent = clusterFileContent
	httpGetClusterInfoOp.useHTTPPassword = useHTTPPassword

	if useHTTPPassword {
		util.ValidateUsernameAndPassword(useHTTPPassword, userName)
		httpGetClusterInfoOp.userName = userName
		httpGetClusterInfoOp.httpsPassword = httpsPassword
	}
	return httpGetClusterInfoOp
}

func (op *httpsGetClusterInfoOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildHTTPSEndpoint("cluster")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *httpsGetClusterInfoOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *httpsGetClusterInfoOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsGetClusterInfoOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			return fmt.Errorf("[%s] wrong password/certificate for https service on host %s",
				op.name, host)
		}

		if result.isPassing() {
			// The content of cluster file will be stored as content of the response
			*op.clusterFileContent = result.content
			if *op.clusterFileContent == "" {
				return fmt.Errorf("[%s] cluster file content should not be empty", op.name)
			}
			return nil
		}
		allErrs = errors.Join(allErrs, result.err)
	}
	return errors.Join(allErrs, fmt.Errorf("could not find a host with a passing result"))
}

func (op *httpsGetClusterInfoOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

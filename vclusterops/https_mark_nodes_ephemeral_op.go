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

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type HTTPSMarkEphemeralNodeOp struct {
	OpBase
	OpHTTPSBase
	targetNodeName string
}

func makeHTTPSMarkEphemeralNodeOp(logger vlog.Printer, nodeName string,
	initiatorHost []string,
	useHTTPPassword bool,
	userName string,
	httpsPassword *string) (HTTPSMarkEphemeralNodeOp, error) {
	op := HTTPSMarkEphemeralNodeOp{}
	op.name = "HTTPSMarkEphemeralNodeOp"
	op.logger = logger.WithName(op.name)
	op.hosts = initiatorHost
	op.targetNodeName = nodeName
	op.useHTTPPassword = useHTTPPassword
	err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
	if err != nil {
		return op, err
	}
	op.userName = userName
	op.httpsPassword = httpsPassword
	return op, nil
}

func (op *HTTPSMarkEphemeralNodeOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.buildHTTPSEndpoint("nodes/" + op.targetNodeName + "/ephemeral")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
	return nil
}

func (op *HTTPSMarkEphemeralNodeOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *HTTPSMarkEphemeralNodeOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPSMarkEphemeralNodeOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isSuccess() {
			allErrs = errors.Join(allErrs, result.err)
			continue
		}
	}
	return allErrs
}

func (op *HTTPSMarkEphemeralNodeOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

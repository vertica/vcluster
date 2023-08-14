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
)

type HTTPSDropNodeOp struct {
	OpBase
	OpHTTPSBase
	targetHost    string
	RequestParams map[string]string
}

func makeHTTPSDropNodeOp(vnode string,
	initiatorHost []string,
	useHTTPPassword bool,
	userName string,
	httpsPassword *string,
	isEon bool) (HTTPSDropNodeOp, error) {
	dropNodeOp := HTTPSDropNodeOp{}
	dropNodeOp.name = "HTTPSDropNodeOp"
	dropNodeOp.hosts = initiatorHost
	dropNodeOp.targetHost = vnode
	dropNodeOp.useHTTPPassword = useHTTPPassword
	err := util.ValidateUsernameAndPassword(dropNodeOp.name, useHTTPPassword, userName)
	if err != nil {
		return dropNodeOp, err
	}
	dropNodeOp.userName = userName
	dropNodeOp.httpsPassword = httpsPassword
	dropNodeOp.RequestParams = make(map[string]string)
	if isEon {
		dropNodeOp.RequestParams["cascade"] = "true"
		return dropNodeOp, nil
	}
	dropNodeOp.RequestParams["cascade"] = "false"
	return dropNodeOp, nil
}

func (op *HTTPSDropNodeOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("nodes/" + op.targetHost + "/drop")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		httpRequest.QueryParams = op.RequestParams
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
	return nil
}

func (op *HTTPSDropNodeOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *HTTPSDropNodeOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPSDropNodeOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.IsSuccess() {
			allErrs = errors.Join(allErrs, result.err)
			continue
		}
	}
	return allErrs
}

func (op *HTTPSDropNodeOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

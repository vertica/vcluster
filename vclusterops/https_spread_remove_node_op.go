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

type HTTPSSpreadRemoveNodeOp struct {
	OpBase
	OpHTTPSBase
	RequestParams map[string]string
}

func makeHTTPSSpreadRemoveNodeOp(hostsToRemove []string, initiatorHost []string, useHTTPPassword bool,
	userName string, httpsPassword *string, hostNodeMap map[string]VCoordinationNode) (HTTPSSpreadRemoveNodeOp, error) {
	op := HTTPSSpreadRemoveNodeOp{}
	op.name = "HTTPSSpreadRemoveNodeOp"
	op.hosts = initiatorHost
	op.useHTTPPassword = useHTTPPassword

	err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
	if err != nil {
		return op, err
	}
	op.userName = userName
	op.httpsPassword = httpsPassword
	op.RequestParams = make(map[string]string)
	var nodeNames []string
	for _, host := range hostsToRemove {
		nodeNames = append(nodeNames, hostNodeMap[host].Name)
	}
	op.RequestParams["remove-target"] = util.ArrayToString(nodeNames, ",")
	return op, nil
}

func (op *HTTPSSpreadRemoveNodeOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("config/spread/remove")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		httpRequest.QueryParams = op.RequestParams
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
	return nil
}

func (op *HTTPSSpreadRemoveNodeOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *HTTPSSpreadRemoveNodeOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPSSpreadRemoveNodeOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
			continue
		}

		// decode the json-format response
		// The successful response object will be a dictionary as below:
		// {"detail": "Wrote a new spread.conf and reloaded Spread"}
		removeSpreadRsp, err := op.parseAndCheckMapResponse(host, result.content)
		if err != nil {
			err = fmt.Errorf("[%s] fail to parse result on host %s, details: %w", op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		// verify if the response's content is correct
		const removeSpreadOpSuccMsg = "Wrote a new spread.conf and reloaded Spread"
		if removeSpreadRsp["detail"] != removeSpreadOpSuccMsg {
			err = fmt.Errorf(`[%s] response detail should be '%s' but got '%s'`, op.name, removeSpreadOpSuccMsg, removeSpreadRsp["detail"])
			allErrs = errors.Join(allErrs, err)
		}
	}

	return allErrs
}

func (op *HTTPSSpreadRemoveNodeOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

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

type HTTPSReloadSpreadOp struct {
	OpBase
	OpHTTPSBase
}

func makeHTTPSReloadSpreadOp(hosts []string, useHTTPPassword bool,
	userName string, httpsPassword *string) (HTTPSReloadSpreadOp, error) {
	httpsReloadSpreadOp := HTTPSReloadSpreadOp{}
	httpsReloadSpreadOp.name = "HTTPSReloadSpreadOp"
	httpsReloadSpreadOp.hosts = hosts
	httpsReloadSpreadOp.useHTTPPassword = useHTTPPassword

	err := util.ValidateUsernameAndPassword(httpsReloadSpreadOp.name, useHTTPPassword, userName)
	if err != nil {
		return httpsReloadSpreadOp, err
	}
	httpsReloadSpreadOp.userName = userName
	httpsReloadSpreadOp.httpsPassword = httpsPassword
	return httpsReloadSpreadOp, nil
}

func (op *HTTPSReloadSpreadOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("config/spread/reload")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *HTTPSReloadSpreadOp) prepare(execContext *OpEngineExecContext) error {
	// If the host input is an empty string, we find up hosts to update the host input
	if len(op.hosts) == 0 {
		op.hosts = execContext.upHosts
	}
	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *HTTPSReloadSpreadOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPSReloadSpreadOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
			continue
		}

		// decode the json-format response
		// The successful response object will be a dictionary as below:
		// {"detail": "Reloaded"}
		reloadSpreadRsp, err := op.parseAndCheckMapResponse(host, result.content)
		if err != nil {
			err = fmt.Errorf("[%s] fail to parse result on host %s, details: %w", op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		// verify if the response's content is correct
		if reloadSpreadRsp["detail"] != "Reloaded" {
			err = fmt.Errorf(`[%s] response detail should be 'Reloaded' but got '%s'`, op.name, reloadSpreadRsp["detail"])
			allErrs = errors.Join(allErrs, err)
		}
	}

	return allErrs
}

func (op *HTTPSReloadSpreadOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

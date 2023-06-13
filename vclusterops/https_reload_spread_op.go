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

type HTTPSReloadSpreadOp struct {
	OpBase
	OpHTTPBase
}

func MakeHTTPSReloadSpreadOp(name string, hosts []string, useHTTPPassword bool,
	userName string, httpsPassword *string) HTTPSReloadSpreadOp {
	httpsReloadSpreadOp := HTTPSReloadSpreadOp{}
	httpsReloadSpreadOp.name = name
	httpsReloadSpreadOp.hosts = hosts
	httpsReloadSpreadOp.useHTTPPassword = useHTTPPassword

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)
	httpsReloadSpreadOp.userName = userName
	httpsReloadSpreadOp.httpsPassword = httpsPassword
	return httpsReloadSpreadOp
}

func (op *HTTPSReloadSpreadOp) setupClusterHTTPRequest(hosts []string) {
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
}

func (op *HTTPSReloadSpreadOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *HTTPSReloadSpreadOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *HTTPSReloadSpreadOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isPassing() {
			success = false
			continue
		}

		// decode the json-format response
		// The successful response object will be a dictionary as below:
		// {"detail": "Reloaded"}
		reloadSpreadRsp, err := op.parseAndCheckMapResponse(host, result.content)
		if err != nil {
			vlog.LogPrintError("[%s] fail to parse result on host %s, details: %w", op.name, host, err)
			success = false
			continue
		}

		// verify if the response's content is correct
		if reloadSpreadRsp["detail"] != "Reloaded" {
			vlog.LogError(`[%s] response detail should be 'Reloaded' but got '%s'`, op.name, reloadSpreadRsp["detail"])
			success = false
		}
	}

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}

func (op *HTTPSReloadSpreadOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

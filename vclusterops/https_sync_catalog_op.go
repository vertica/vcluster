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
	"strconv"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type HTTPSSyncCatalogOp struct {
	OpBase
	OpHTTPBase
}

func MakeHTTPSSyncCatalogOp(name string, hosts []string, useHTTPPassword bool,
	userName string, httpsPassword *string) HTTPSSyncCatalogOp {
	httpsSyncCatalogOp := HTTPSSyncCatalogOp{}
	httpsSyncCatalogOp.name = name
	httpsSyncCatalogOp.hosts = hosts
	httpsSyncCatalogOp.useHTTPPassword = useHTTPPassword

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)
	httpsSyncCatalogOp.userName = userName
	httpsSyncCatalogOp.httpsPassword = httpsPassword
	return httpsSyncCatalogOp
}

func (op *HTTPSSyncCatalogOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("cluster/catalog/sync")
		httpRequest.QueryParams = make(map[string]string)
		httpRequest.QueryParams["retry-count"] = strconv.Itoa(util.DefaultRetryCount)
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPSSyncCatalogOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *HTTPSSyncCatalogOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *HTTPSSyncCatalogOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// decode the json-format response
			// The response object will be a dictionary, an example:
			// {"new_truncation_version": "18"}
			syncCatalogRsp, err := op.parseAndCheckMapResponse(host, result.content)
			if err != nil {
				success = false
				continue
			}
			version, ok := syncCatalogRsp["new_truncation_version"]
			if !ok {
				vlog.LogError(`[%s] response does not contain field "new_truncation_version"`, op.name)
				success = false
			}
			vlog.LogPrintInfo(`[%s] the_latest_truncation_catalog_version: %s"`, op.name, version)
		} else {
			success = false
		}
	}

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}

func (op *HTTPSSyncCatalogOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

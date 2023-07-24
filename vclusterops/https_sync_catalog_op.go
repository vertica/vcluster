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
	"strconv"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type HTTPSSyncCatalogOp struct {
	OpBase
	OpHTTPBase
}

func MakeHTTPSSyncCatalogOp(hosts []string, useHTTPPassword bool,
	userName string, httpsPassword *string) HTTPSSyncCatalogOp {
	httpsSyncCatalogOp := HTTPSSyncCatalogOp{}
	httpsSyncCatalogOp.name = "HTTPSyncCatalogOp"
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

func (op *HTTPSSyncCatalogOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *HTTPSSyncCatalogOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPSSyncCatalogOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// decode the json-format response
			// The response object will be a dictionary, an example:
			// {"new_truncation_version": "18"}
			syncCatalogRsp, err := op.parseAndCheckMapResponse(host, result.content)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				continue
			}
			version, ok := syncCatalogRsp["new_truncation_version"]
			if !ok {
				err = fmt.Errorf(`[%s] response does not contain field "new_truncation_version"`, op.name)
				allErrs = errors.Join(allErrs, err)
			}
			vlog.LogPrintInfo(`[%s] the_latest_truncation_catalog_version: %s"`, op.name, version)
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}
	return allErrs
}

func (op *HTTPSSyncCatalogOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

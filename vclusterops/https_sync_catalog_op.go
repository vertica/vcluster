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
	clusterFileContent *string
}

func MakeHTTPSSyncCatalogOp(hosts []string, useHTTPPassword bool,
	userName string, httpsPassword *string, clusterFileContent *string) HTTPSSyncCatalogOp {
	httpsSyncCatalogOp := HTTPSSyncCatalogOp{}
	httpsSyncCatalogOp.name = "HTTPSyncCatalogOp"
	httpsSyncCatalogOp.hosts = hosts
	httpsSyncCatalogOp.useHTTPPassword = useHTTPPassword
	httpsSyncCatalogOp.clusterFileContent = clusterFileContent

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

type ClusterStateInfo struct {
	IsEon bool `json:"is_eon"`
}

func (op *HTTPSSyncCatalogOp) Prepare(execContext *OpEngineExecContext) error {
	// If the content of the cluster file is nil, we continue processing the sync catalog operation.
	// We use the "eonmode" flag provided by the user to proceed with this operation.
	// This case is used for certain operations (e.g., start_db, create_db) when the database is down.
	// Otherwise, we can call /cluster to check whether this is an EON database or not.
	if op.clusterFileContent != nil {
		// unmarshal the response content
		clusterStateInfo := ClusterStateInfo{}
		err := util.GetJSONLogErrors(*op.clusterFileContent, &clusterStateInfo, op.name)
		if err != nil {
			vlog.LogError("[%s] fail to parse response, detail: %s", op.name, err)
			return err
		}
		// if this is not eon database, skip the operation
		// Sync Catalog  operation only support eon mode
		if !clusterStateInfo.IsEon {
			vlog.LogInfo("[%s] Sync Catalog operation will not be processed, as it only supports EON mode", op.name)
			op.skipExecute = true
			return nil
		}
	}
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

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
	OpHTTPSBase
	dbName string
	vdb    *VCoordinationDatabase
}

func makeHTTPSGetClusterInfoOp(dbName string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string, vdb *VCoordinationDatabase,
) (httpsGetClusterInfoOp, error) {
	op := httpsGetClusterInfoOp{}
	op.name = "HTTPSGetClusterInfoOp"
	op.dbName = dbName
	op.hosts = hosts
	op.vdb = vdb
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

func (op *httpsGetClusterInfoOp) setupClusterHTTPRequest(hosts []string) error {
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

	return nil
}

func (op *httpsGetClusterInfoOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsGetClusterInfoOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

type ClusterStateInfo struct {
	IsEon                    bool     `json:"is_eon"`
	DBName                   string   `json:"db_name"`
	CommunalStorageLocations []string `json:"commnual_storage_locations"`
}

func (op *httpsGetClusterInfoOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			return fmt.Errorf("[%s] wrong password/certificate for https service on host %s",
				op.name, host)
		}

		if result.isPassing() {
			// unmarshal the response content
			clusterStateInfo := ClusterStateInfo{}
			err := op.parseAndCheckResponse(host, result.content, &clusterStateInfo)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				return appendHTTPSFailureError(allErrs)
			}

			// save cluster info to vdb
			op.vdb.IsEon = clusterStateInfo.IsEon
			op.vdb.UseDepot = clusterStateInfo.IsEon
			op.vdb.Name = clusterStateInfo.DBName
			if op.vdb.Name != op.dbName {
				err = fmt.Errorf(`[%s] database %s is running on host %s, rather than database %s`, op.name, op.vdb.Name, host, op.dbName)
				allErrs = errors.Join(allErrs, err)
				break
			}
			if len(clusterStateInfo.CommunalStorageLocations) > 0 {
				op.vdb.CommunalStorageLocation = clusterStateInfo.CommunalStorageLocations[0]
			}
			return nil
		}
		allErrs = errors.Join(allErrs, result.err)
	}
	return appendHTTPSFailureError(allErrs)
}

func (op *httpsGetClusterInfoOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

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

type httpsStartUpCommandOp struct {
	OpBase
	OpHTTPSBase
	vdb *VCoordinationDatabase
}

func makeHTTPSRestartUpCommandOp(useHTTPPassword bool, userName string, httpsPassword *string,
	vdb *VCoordinationDatabase) (httpsStartUpCommandOp, error) {
	op := httpsStartUpCommandOp{}
	op.name = "HTTPSStartUpCommandOp"
	op.useHTTPPassword = useHTTPPassword
	op.vdb = vdb

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

func (op *httpsStartUpCommandOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod

		httpRequest.BuildHTTPSEndpoint("startup/commands")

		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsStartUpCommandOp) prepare(execContext *OpEngineExecContext) error {
	// Use the /v1/startup/command endpoint for a primary Up host to view every start command of existing nodes
	var primaryUpHosts []string
	for host := range op.vdb.HostNodeMap {
		if op.vdb.HostNodeMap[host].IsPrimary && op.vdb.HostNodeMap[host].State == util.NodeUpState {
			primaryUpHosts = append(primaryUpHosts, host)
			break
		}
	}
	op.hosts = primaryUpHosts
	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsStartUpCommandOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsStartUpCommandOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			return fmt.Errorf("[%s] wrong password/certificate for https service on host %s",
				op.name, host)
		}

		if result.isPassing() {
			type HTTPStartUpCommandResponse map[string][]string
			/* "v_practice_db_node0001": [
				  "\/opt\/vertica\/bin\/vertica",
				  "-D",
				  "\/data\/practice_db\/v_practice_db_node0001_catalog",
				  "-C",
			 	  "practice_db",
				  "-n",
				  "v_practice_db_node0001",
				  "-h",
				  "192.168.1.101",
				  "-p",
				  "5433",
				  "-P",
				  "4803",
				  "-Y",
				  "ipv4"
				],
				"v_practice_db_node0002": [
				  "\/opt\/vertica\/bin\/vertica",
				  "-D",
				  "\/data\/practice_db\/v_practice_db_node0002_catalog",
				  "-C",
				  "practice_db",
				  "-n",
				  "v_practice_db_node0002",
				  "-h",
				  "192.168.1.102",
				  "-p",
				  "5433",
				  "-P",
				  "4803",
				  "-Y",
				  "ipv4"
			    ],
			*/
			var responseObj HTTPStartUpCommandResponse
			err := op.parseAndCheckResponse(host, result.content, &responseObj)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				continue
			}
			execContext.startupCommandMap = responseObj
			return nil
		}
		allErrs = errors.Join(allErrs, result.err)
	}
	return nil
}

func (op *httpsStartUpCommandOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

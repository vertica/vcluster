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
	OpHTTPBase
	startUpCommandFileContent *string
}

func makeHTTPSRestartUpCommandOp(
	useHTTPPassword bool, userName string, httpsPassword *string, startUpCommandFileContent *string) httpsStartUpCommandOp {
	httpStartUpCommandOp := httpsStartUpCommandOp{}
	httpStartUpCommandOp.name = "HTTPSStartUpCommandOp"
	httpStartUpCommandOp.useHTTPPassword = useHTTPPassword
	httpStartUpCommandOp.startUpCommandFileContent = startUpCommandFileContent

	if useHTTPPassword {
		util.ValidateUsernameAndPassword(useHTTPPassword, userName)
		httpStartUpCommandOp.userName = userName
		httpStartUpCommandOp.httpsPassword = httpsPassword
	}
	return httpStartUpCommandOp
}

func (op *httpsStartUpCommandOp) setupClusterHTTPRequest(hosts []string) {
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
}

func (op *httpsStartUpCommandOp) Prepare(execContext *OpEngineExecContext) error {
	// Use the /v1/startup/command endpoint for a primary Up host to view every start command of existing nodes
	var primaryUpHosts []string
	nodesList := execContext.nodesInfo
	for _, node := range nodesList {
		if node.IsPrimary && node.State == util.NodeUpState {
			primaryUpHosts = append(primaryUpHosts, node.Address)
			break
		}
	}
	op.hosts = primaryUpHosts
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *httpsStartUpCommandOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
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

			// The content of starup command file will be stored as content of the response
			*op.startUpCommandFileContent = result.content
			if *op.startUpCommandFileContent == "" {
				return fmt.Errorf("[%s] file content should not be empty", op.name)
			}
			return nil
		}
		allErrs = errors.Join(allErrs, result.err)
	}
	return nil
}

func (op *httpsStartUpCommandOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

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
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type nmaStartNodeOp struct {
	OpBase
	hostRequestBodyMap        map[string]string
	startUpCommandFileContent *string
}

func makeNMAStartNodeOp(hosts []string, startUpCommandContent *string) nmaStartNodeOp {
	startNodeOp := nmaStartNodeOp{}
	startNodeOp.name = "NMAStartNodeOp"
	startNodeOp.hosts = hosts
	startNodeOp.startUpCommandFileContent = startUpCommandContent
	return startNodeOp
}

func (op *nmaStartNodeOp) updateRequestBody(execContext *OpEngineExecContext) error {
	op.hostRequestBodyMap = make(map[string]string)
	// If the content of the startup command file is nil, we will use startup command information from NMA Read Catalog Editor.
	// This case is used for certain operations (e.g., start_db, create_db) when the database is down,
	// and we need to use the NMA catalog/database endpoint.
	// Otherwise, we can use the startup command file from the HTTPS startup/commands endpoint when the database is up.
	if op.startUpCommandFileContent != nil {
		// unmarshal the response content
		type HTTPStartUpCommandResponse map[string][]string
		var responseObj HTTPStartUpCommandResponse
		err := util.GetJSONLogErrors(*op.startUpCommandFileContent, &responseObj, op.name)
		if err != nil {
			vlog.LogError("[%s] fail to parse response, detail: %s", op.name, err)
			return err
		}
		// map {host: startCommand} e.g.,
		// {ip1:[/opt/vertica/bin/vertica -D /data/practice_db/v_practice_db_node0001_catalog -C
		// practice_db -n v_practice_db_node0001 -h 192.168.1.101 -p 5433 -P 4803 -Y ipv4]}
		hostStartCommandMap := make(map[string][]string)
		nodesList := execContext.nodesInfo
		for _, node := range nodesList {
			hoststartCommand, ok := responseObj[node.Name]
			if ok {
				hostStartCommandMap[node.Address] = hoststartCommand
			}
		}
		for _, host := range op.hosts {
			err = updateHostRequestBodyMapFromNodeStartCommand(host, hostStartCommandMap[host], op.hostRequestBodyMap, op.name)
			if err != nil {
				return err
			}
		}
	} else {
		// use startup command information from NMA catalog/database endpoint when the database is down
		for _, host := range op.hosts {
			node, ok := execContext.nmaVDatabase.HostNodeMap[host]
			if !ok {
				return fmt.Errorf("[%s] the bootstrap node (%s) is not found from the catalog editor information: %+v",
					op.name, host, execContext.nmaVDatabase)
			}
			err := updateHostRequestBodyMapFromNodeStartCommand(host, node.StartCommand, op.hostRequestBodyMap, op.name)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *nmaStartNodeOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildNMAEndpoint("nodes/start")
		httpRequest.RequestData = op.hostRequestBodyMap[host]
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *nmaStartNodeOp) Prepare(execContext *OpEngineExecContext) error {
	err := op.updateRequestBody(execContext)
	if err != nil {
		return err
	}

	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *nmaStartNodeOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *nmaStartNodeOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

type startNodeResponse struct {
	DBLogPath  string `json:"dbLogPath"`
	ReturnCode int    `json:"return_code"`
}

func (op *nmaStartNodeOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// the response object will be a dictionary including the dbLog path and a return code, e.g.,:
			// {'dbLogPath':  '/data/platform_test_db/dbLog',
			// 'return_code', 0 }

			responseObj := startNodeResponse{}
			err := op.parseAndCheckResponse(host, result.content, &responseObj)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				continue
			}

			if responseObj.ReturnCode != 0 {
				err = fmt.Errorf(`[%s] return_code should be 0 but got %d`, op.name, responseObj.ReturnCode)
				allErrs = errors.Join(allErrs, err)
			}
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	return allErrs
}
